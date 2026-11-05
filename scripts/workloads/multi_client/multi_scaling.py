#!/usr/bin/env python3
"""scaling_data with the benchmark client spread over several client nodes (experiment 12, 3 Nov 2026).

One xl170 client node (20 hardware threads) could not drive more than about 50k point reads/s or 32k inserts/s with
the benchmark runner even with microbench/fast_client. This driver runs each scaling_data point (operation, N busy
branches) on K client nodes at once: node i takes n_i of the N branches (as even as possible; N < K uses N nodes)
with min(max_procs, n_i) client processes, each node runs scripts/workloads/multi_client/node_run.sh (the
environment and runner arguments of scaling_data.sh, per-client op counts of the total N), and every node's timed
window starts at the same moment: a coordinator in this process answers GO once every client process of every
node has passed its local start barrier (microbench/fast_client/patches.py, cross-node start). Then the per-node
results are merged into one runner-style summary.

    # on the coordinator (a0), after neon.py TOPO start / seed / env-init / deploy-client:
    multi_scaling.py deploy --topology T --clients b0,b1,b2,b3
    # on the first client node (the topology's client), as a detached unit:
    multi_scaling.py run --topology T --clients b0,b1,b2,b3 --outdir DIR [--ns 1,4,...] [--ops READ,RANGE_READ]

Output (same layout as scaling_data.sh, so the plotting reads it unchanged):
    DIR/neon/scaling_data/progress.log                   START / DONE (merged numbers + per-node client CPU) / FAILED
    DIR/neon/scaling_data/data_ops/OP/<run_id>_OP_threadsN_summary.json   merged summary (+ "multi_client" block)
    DIR/neon/scaling_data/data_ops/OP/<node>/...         each node's runner summary and parquet
    DIR/neon/scaling_data/logs/data_OP_nN.<node>.log     each node's runner log
Merging: window = GO to the end of the longest node window (each node's elapsed_time starts at GO); throughput = all
nodes' successful operations / window; latency quantiles over all nodes' parquet rows (timed rows only, as in the
runner); remote CPU from the first node's sampler (its window spans GO to its last op); client CPU summed over
nodes (each node's runner processes, cores over its own window x its window / merged window).
Pass checks: every node exited 0, failed_ops = 0, each node ran its intended processes, remote node CPU present for
every server-set agent. A point that does not pass is retried once at the end (as scaling_data.sh).

--backend pgcopy (experiment 13: clients co-located with their branch instances on every host node): the same, with
<state_dir>/pgcopy.env (each node's own view, written by pgcopy.py deploy-client), the purge through pgcopy_ctl.py,
and ssh to the other nodes on their LAN addresses with the cluster-internal key (~/.ssh/config on every node). Two
more modes:

    multi_scaling.py run-branch  --backend pgcopy --topology T --clients n1,...,n15 --outdir DIR [--ns 1,4,...]
        scaling_branch over the nodes: node 1 runs scaling_branch.sh's creator runner (CREATES creates, every
        CONNECT_EVERY-th connected) with its n_1 busy branches (background MIXED); every other node i runs the same
        runner on its n_i busy branches with one create that only waits (fast_client hold patch) until node 1 is
        done: N busy branches in all, one creator. Merged summary = node 1's summary (create latencies) plus a
        "multi_client" block with every node's background ops in the window, at
        DIR/pgcopy/scaling_branch/branch_create/mixed/<run_id>_BRANCH_CREATE_threads1_summary.json.
    multi_scaling.py run-ceiling --backend pgcopy --topology T --clients ... --outdir DIR [--ns 128,...] [--kinds ...]
        ceiling_check.py on every node with its share of each N, every measurement started together (coordinator
        GO per kind and N); per-node CSVs summed per (kind, N) into DIR/pgcopy/scaling_data/ceiling_check.csv.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import socket
import socketserver
import subprocess
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts" / "setup"))
from topo import Topo  # noqa: E402

REMOTE_REPO = "~/db-fork"
FC_DIR = "microbench/fast_client"
NODE_RUN = "scripts/workloads/multi_client/node_run.sh"
DEFAULT_PORT = 47800


def log_line(progress: Path, msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {msg}"
    print(line, flush=True)
    with progress.open("a") as f:
        f.write(line + "\n")


def md5s() -> str:
    files = [f"{FC_DIR}/patches.py", f"{FC_DIR}/sitecustomize.py", NODE_RUN, "scripts/workloads/multi_client/multi_scaling.py",
             "microbench/runner2.py", "dblib/db_api.py", "scripts/run_throughput_bench.sh", "dblib/neon_local.py",
             "dblib/pgcopy.py", "scripts/workloads/common.sh", "scripts/workloads/scaling_data.sh",
             "scripts/workloads/scaling_branch.sh", "scripts/workloads/ceiling_check.py"]
    return " ".join(f"{f}:{hashlib.md5((REPO / f).read_bytes()).hexdigest()[:8]}" for f in files if (REPO / f).exists())


# ---------------------------------------------------------------- coordinator
class Coordinator:
    """GO for a key once `expected` connections sent it; ABORT on request."""

    def __init__(self, port: int):
        self.keys: dict[str, dict] = {}
        self.lock = threading.Lock()
        coord = self

        class Handler(socketserver.StreamRequestHandler):
            def handle(self):
                key = self.rfile.readline().decode().strip()
                ev = threading.Event()
                with coord.lock:
                    k = coord.keys.get(key)
                    if k is None or k["state"] != "wait":
                        self.wfile.write(b"ABORT unknown or closed key\n")
                        return
                    k["conns"].append(ev)
                    if len(k["conns"]) == k["expected"]:
                        k["go"] = time.time()
                        k["state"] = "go"
                        for e in k["conns"]:
                            e.set()
                ev.wait()
                with coord.lock:
                    msg = "GO" if k["state"] == "go" else f"ABORT {k.get('reason', '')}"
                try:
                    self.wfile.write(f"{msg}\n".encode())
                except OSError:
                    pass

        class Server(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

        self.srv = Server(("0.0.0.0", port), Handler)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def expect(self, key: str, n: int) -> None:
        with self.lock:
            self.keys[key] = {"expected": n, "conns": [], "state": "wait", "go": None}

    def release(self, key: str) -> None:
        """GO for a key now, whoever has arrived (the hold of background-only nodes)."""
        with self.lock:
            k = self.keys.get(key)
            if k and k["state"] == "wait":
                k["state"], k["go"] = "go", time.time()
                for e in k["conns"]:
                    e.set()

    def abort(self, key: str, reason: str) -> None:
        with self.lock:
            k = self.keys.get(key)
            if k and k["state"] == "wait":
                k["state"], k["reason"] = "abort", reason
                for e in k["conns"]:
                    e.set()

    def status(self, key: str) -> tuple[str, float | None, int]:
        with self.lock:
            k = self.keys[key]
            return k["state"], k["go"], len(k["conns"])


# ---------------------------------------------------------------- helpers
def shares(n: int, k: int) -> list[int]:
    k = min(k, n)
    return [n // k + (1 if i < n % k else 0) for i in range(k)]


def ssh_base(topo: Topo, backend: str = "neon") -> list[str]:
    if backend == "pgcopy":   # LAN addresses, the cluster-internal key from ~/.ssh/config (pgcopy.py install)
        return ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]
    key = os.path.expanduser(topo.ssh_key) if getattr(topo, "ssh_key", None) else os.path.expanduser("~/.ssh/dassl_rsa")
    return ["ssh", "-i", key, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new"]


class Ctx:
    """What every mode needs from the topology for one backend."""

    def __init__(self, a):
        self.backend = getattr(a, "backend", "neon")
        self.topo = Topo(a.topology)
        self.sec = self.topo.section(self.backend)
        if not self.sec:
            raise SystemExit(f"topology has no [{self.backend}] section")
        self.clients = a.clients.split(",") if a.clients else list(self.sec.get("hosts") or [])
        self.w = self.topo.wl(self.backend)
        self.max_procs = self.w["max_procs"] or 20
        self.state = self.topo.state_dir
        self.env_file = f"{self.state}/{self.backend}.env"
        self.runner_backend = {"neon": "neon_local", "pgcopy": "pgcopy"}[self.backend]
        self.ssh = ssh_base(self.topo, self.backend)

    def target(self, name: str) -> str:
        nd = self.topo.node(name)
        return nd.lan if self.backend == "pgcopy" else f"{self.topo.ssh_user}@{nd.host}"

    def rid(self, threads: int, branches: int) -> str:
        return f"{self.runner_backend}_chw{self.w['W']}_tp_t{threads}_b{branches}"

    def remote_spec(self) -> str:
        for line in Path(self.env_file).read_text().splitlines():
            if line.startswith("DBFORK_REMOTE_CPU="):
                return line.split("=", 1)[1].strip("'\"")
        return ""

    def purge(self, logfile: Path) -> None:
        ctl = ["scripts/setup/neon_ctl.py", "--env", self.env_file, "purge"] if self.backend == "neon" else \
            ["scripts/setup/pgcopy_ctl.py", "--env", self.env_file, "purge"]
        with logfile.open("a") as f:
            subprocess.run(["uv", "run", "python", *ctl], cwd=REPO, stdout=f, stderr=subprocess.STDOUT, check=False)

    def launch(self, node: str, cmd: str, logf):
        argv = ["bash", "-c", cmd] if node == self.clients[0] else self.ssh + [self.target(node), cmd]
        return subprocess.Popen(argv, stdout=logf, stderr=subprocess.STDOUT)

    def fetch(self, node: str, src_dir: Path) -> None:
        src_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(["rsync", "-a", "-e", " ".join(self.ssh), f"{self.target(node)}:{src_dir}/", f"{src_dir}/"],
                       check=False)


def merge(op: str, n: int, nodes: list[dict], go: float, outroot: Path) -> dict:
    import numpy as np
    import pyarrow.parquet as pq
    from dblib import result_collector as rc
    from dblib import result_pb2 as rslt

    sums = [json.loads(Path(x["summary"]).read_text()) for x in nodes]
    window = max(s["elapsed_time"] for s in sums)
    verb, stmt = [], []
    for x in nodes:
        t = pq.read_table(x["parquet"], columns=["op_type", "status", "latency"]).to_pandas()
        ok = t["status"]   # the runner's parquet stores the status name ("OK"); accept the enum value too
        t = t[(ok == "OK") | (ok == rslt.OpStatus.OK) | (ok == str(rslt.OpStatus.OK))]
        verb.append(t[t["op_type"].isin(list(rc.VERB_OP_TYPES))]["latency"].to_numpy())
        stmt.append(t[t["op_type"].isin(list(rc.STATEMENT_OP_TYPES))]["latency"].to_numpy())

    def q(a):
        a = np.concatenate(a) if a else np.array([])
        if not a.size:
            return {"n": 0}
        return {"n": int(a.size), "mean": float(a.mean()), "p50": float(np.percentile(a, 50)),
                "p95": float(np.percentile(a, 95)), "p99": float(np.percentile(a, 99)), "max": float(a.max())}

    m = dict(sums[0])
    total_ops = sum(s["total_ops"] for s in sums)
    client_cpu_s = sum((s.get("client_cpu_cores") or 0) * s["elapsed_time"] for s in sums)
    m.update({
        "run_id": f"{m['run_id'].rsplit('_tp_', 1)[0]}_tp_t{n}_b{n}", "num_threads": n, "num_branches": n,
        "intended_ops": sum(s["intended_ops"] for s in sums), "total_ops": total_ops,
        "failed_ops": sum(s["failed_ops"] or 0 for s in sums), "elapsed_time": window,
        "throughput": total_ops / window if window > 0 else 0.0,
        "data_ops_count": sum(s.get("data_ops_count", 0) for s in sums),
        "loaded_branches": sum(s.get("loaded_branches") or 0 for s in sums),
        "num_processes": sum(s["num_processes"] for s in sums),
        "client_cpu_cores": client_cpu_s / window if window > 0 else None,
        "latency": {"ops": q(verb), "statements": q(stmt)},
        "multi_client": {
            "go_time": go, "window_from": "GO", "remote_cpu_from": nodes[0]["node"],
            "nodes": [{"node": x["node"], "branches": x["n"], "procs": x["procs"], "num_processes": s["num_processes"],
                       "total_ops": s["total_ops"], "failed_ops": s["failed_ops"], "elapsed_time": s["elapsed_time"],
                       "throughput": s["throughput"], "client_cpu_cores": s.get("client_cpu_cores"),
                       "summary": os.path.relpath(x["summary"], outroot)} for x, s in zip(nodes, sums)],
        },
    })
    return m


def check(m: dict, nodes: list[dict], remote_spec: str) -> list[str]:
    problems = []
    if m.get("failed_ops"):
        problems.append(f"failed_ops={m['failed_ops']}")
    for x in m["multi_client"]["nodes"]:
        if x["num_processes"] != x["procs"]:
            problems.append(f"{x['node']}: num_processes {x['num_processes']} != {x['procs']}")
    labels = dict(y.split("=", 1) for y in filter(None, (z.strip() for z in remote_spec.split(";"))))
    rc = m.get("remote_cpu") or {}
    for label, url in labels.items():
        if "set=server" in url and (rc.get(label) or {}).get("node_cores") is None:
            problems.append(f"no remote_cpu.{label}.node_cores")
    return problems


def done_line(m: dict) -> str:
    st = m["latency"]["statements"]
    r2 = lambda x: None if x is None else round(x, 2)  # noqa: E731
    cpu = " ".join(f"{k}={r2(v.get('proc_cores'))}/{r2(v.get('node_cores'))}" for k, v in (m.get("remote_cpu") or {}).items()
                   if v.get("proc_cores") is not None or v.get("node_cores") is not None)
    per = " ".join(f"{x['node']}:{x['branches']}b/{x['num_processes']}p/{r2(x['client_cpu_cores'])}c"
                   for x in m["multi_client"]["nodes"])
    return (f"tput={m['throughput']:.0f}/s window={m['elapsed_time']:.1f}s procs={m['num_processes']} "
            f"p50={st.get('p50', 0) * 1e3:.3f}ms mean={st.get('mean', 0) * 1e3:.3f}ms "
            f"client_cpu={r2(m['client_cpu_cores'])} clients=[{per}] {cpu}")


# ---------------------------------------------------------------- run
def first_client(cx: Ctx):
    topo, clients = cx.topo, cx.clients
    if clients[0] != cx.sec["client"]:
        raise SystemExit(f"the first client must be the topology's client ({cx.sec['client']}): this driver runs there")
    me = topo.node(clients[0])
    here = {socket.gethostname(), socket.getfqdn()}
    if not me.is_here() and me.host not in here and not any(h.startswith(me.name + ".") for h in here):
        print(f"warning: this host ({socket.gethostname()}) does not look like {me.name} ({me.host})", file=sys.stderr)
    return me


def run(a) -> int:
    cx = Ctx(a)
    topo, clients, max_procs, env_file = cx.topo, cx.clients, cx.max_procs, cx.env_file
    me = first_client(cx)
    sync_addr = f"{me.lan}:{a.port}"
    remote_spec = cx.remote_spec()
    outroot = Path(a.outdir).resolve() / cx.backend / "scaling_data"
    (outroot / "logs").mkdir(parents=True, exist_ok=True)
    progress = outroot / "progress.log"
    ns = [int(x) for x in (a.ns or ",".join(map(str, cx.w["ns_data"]))).split(",")]
    ops = a.ops.split(",")
    coord = Coordinator(a.port)
    log_line(progress, f"=== multi-client scaling_data ({cx.backend}, topology {topo.name}): clients={clients} ns={ns} "
                       f"ops={ops} max_procs/node={max_procs} sync={sync_addr} host={socket.gethostname()}")
    log_line(progress, f"code md5: {md5s()}")

    def purge(tag: str) -> None:
        cx.purge(outroot / "logs" / f"purge_{tag}.log")

    def done(name: str) -> bool:
        return progress.exists() and any(f" DONE  {name} " in l for l in progress.read_text().splitlines())

    def one(op: str, total: int, attempt: int) -> None:
        name = f"data_{op}_n{total}"
        sh = shares(total, len(clients))
        nodes = [{"node": c, "n": k, "procs": min(max_procs, k)} for c, k in zip(clients, sh)]
        key = f"{name}.{attempt}.{int(time.time())}"
        coord.expect(key, sum(x["procs"] for x in nodes))
        purge("before")
        log_line(progress, f"START {name} t={time.time():.3f} shares={[(x['node'], x['n'], x['procs']) for x in nodes]}")
        t0 = time.time()
        procs = []
        for x in nodes:
            cmd = (f"cd {REMOTE_REPO if x['node'] != clients[0] else REPO} && "
                   f"env BENCH_FAST_CLIENT=1 PYTHONPATH=$PWD/{FC_DIR} BENCH_SYNC_ADDR={sync_addr} "
                   f"BENCH_SYNC_KEY={key} {NODE_RUN} --topology {shlex.quote(a.topology_remote or a.topology)} "
                   f"--backend {cx.backend} --outdir {shlex.quote(str(Path(a.outdir).resolve()))} --op {op} "
                   f"--n-total {total} --n-node {x['n']} --procs {x['procs']} --node {x['node']} "
                   f"--setup-parallelism {max(1, a.setup_parallelism // len(nodes))}")
            logf = (outroot / "logs" / f"{name}.{x['node']}.log").open("w")
            procs.append((x, cx.launch(x["node"], cmd, logf), logf))
        rcs = {}
        while len(rcs) < len(procs):
            for x, p, _ in procs:
                if x["node"] in rcs:
                    continue
                r = p.poll()
                if r is not None:
                    rcs[x["node"]] = r
                    if r != 0:
                        coord.abort(key, f"{x['node']} exited {r} before the start")
            time.sleep(1)
        for _, _, f in procs:
            f.close()
        state_, go, arrived = coord.status(key)
        # results of the other nodes back to this node (same paths)
        for x in nodes[1:]:
            cx.fetch(x["node"], outroot / "data_ops" / op / x["node"])
        bad = {k: v for k, v in rcs.items() if v != 0}
        secs = int(time.time() - t0)
        if bad or state_ != "go":
            log_line(progress, f"FAILED {name} ({secs}s, exits {rcs}, start {state_} {arrived} arrived; "
                               f"see logs/{name}.<node>.log) t={time.time():.3f}")
            purge("after_failure")
            return
        rid = lambda k: cx.rid(k, k)  # noqa: E731
        for x in nodes:
            d = outroot / "data_ops" / op / x["node"]
            x["summary"] = str(d / f"{rid(x['n'])}_{op}_threads{x['n']}_summary.json")
            x["parquet"] = str(d / f"{rid(x['n'])}.parquet")
        try:
            m = merge(op, total, nodes, go, outroot)
        except Exception as e:  # noqa: BLE001
            log_line(progress, f"CHECK-FAIL {name} ({secs}s) merge failed: {e!r} t={time.time():.3f}")
            return
        out = outroot / "data_ops" / op / f"{rid(total)}_{op}_threads{total}_summary.json"
        out.write_text(json.dumps(m, indent=2))
        problems = check(m, nodes, remote_spec)
        word = "DONE " if not problems else "CHECK-FAIL"
        log_line(progress, f"{word} {name} ({secs}s) {done_line(m)} "
                           f"{'PROBLEMS: ' + ', '.join(problems) if problems else 'ok'} t={time.time():.3f}")

    for total in ns:
        for op in ops:
            if done(f"data_{op}_n{total}"):
                log_line(progress, f"SKIP  data_{op}_n{total} (done before)")
                continue
            one(op, total, 1)
    for total in ns:
        for op in ops:
            if not done(f"data_{op}_n{total}"):
                log_line(progress, f"RETRY data_{op}_n{total}")
                one(op, total, 2)
    purge("end")
    log_line(progress, "=== multi-client scaling_data done")
    return 0


# ---------------------------------------------------------------- run-branch
def run_branch(a) -> int:
    """scaling_branch with the N busy branches spread over the nodes and one creator (node 1)."""
    cx = Ctx(a)
    topo, clients, max_procs = cx.topo, cx.clients, cx.max_procs
    me = first_client(cx)
    sync_addr = f"{me.lan}:{a.port}"
    remote_spec = cx.remote_spec()
    bg = (a.background or cx.w["background"]).upper()
    outroot = Path(a.outdir).resolve() / cx.backend / "scaling_branch"
    d = outroot / "branch_create" / bg.lower()
    (outroot / "logs").mkdir(parents=True, exist_ok=True)
    d.mkdir(parents=True, exist_ok=True)
    progress = outroot / "progress.log"
    ns = [int(x) for x in (a.ns or ",".join(map(str, cx.w["ns_branch"]))).split(",")]
    creates = a.creates or cx.w["creates"]
    coord = Coordinator(a.port)
    log_line(progress, f"=== multi-client scaling_branch ({cx.backend}, topology {topo.name}): clients={clients} ns={ns} "
                       f"background={bg} creates={creates} max_procs/node={max_procs} sync={sync_addr} "
                       f"host={socket.gethostname()}")
    log_line(progress, f"code md5: {md5s()}")

    def done(name: str) -> bool:
        return progress.exists() and any(f" DONE  {name} " in l for l in progress.read_text().splitlines())

    def one(total: int, attempt: int) -> None:
        name = f"create_{bg.lower()}_n{total}"
        sh = shares(total, len(clients))
        nodes = [{"node": c, "n": k, "procs": min(max_procs, k + 1)} for c, k in zip(clients, sh)]
        key = f"{name}.{attempt}.{int(time.time())}"
        hold = key + ".hold"
        # one GO connection per node: only the process holding the timed creator thread passes the start barrier
        # (background clients never do)
        coord.expect(key, len(nodes))
        coord.expect(hold, 10 ** 9)            # released when node 1's creator is done
        cx.purge(outroot / "logs" / "purge_before.log")
        log_line(progress, f"START {name} t={time.time():.3f} shares={[(x['node'], x['n'], x['procs']) for x in nodes]}")
        t0 = time.time()
        procs = []
        for i, x in enumerate(nodes):
            env = f"BENCH_FAST_CLIENT=1 PYTHONPATH=$PWD/{FC_DIR} BENCH_SYNC_ADDR={sync_addr} BENCH_SYNC_KEY={key}"
            if i > 0:
                env += f" BENCH_HOLD_KEY={hold}"
            cmd = (f"cd {REMOTE_REPO if i else REPO} && env {env} {NODE_RUN} --mode branch "
                   f"--topology {shlex.quote(a.topology_remote or a.topology)} --backend {cx.backend} "
                   f"--outdir {shlex.quote(str(Path(a.outdir).resolve()))} --op {bg} --n-total {total} "
                   f"--n-node {x['n']} --procs {x['procs']} --node {x['node']} --creates {creates} "
                   f"--setup-parallelism {max(1, a.setup_parallelism // len(nodes))}")
            logf = (outroot / "logs" / f"{name}.{x['node']}.log").open("w")
            procs.append((x, cx.launch(x["node"], cmd, logf), logf))
        rcs: dict = {}
        while len(rcs) < len(procs):
            for x, p, _ in procs:
                if x["node"] in rcs:
                    continue
                r = p.poll()
                if r is not None:
                    rcs[x["node"]] = r
                    if r != 0:
                        coord.abort(key, f"{x['node']} exited {r} before the start")
                    if x["node"] == clients[0]:
                        coord.release(hold)     # the creator is done: the background-only nodes stop
            time.sleep(1)
        for _, _, f in procs:
            f.close()
        state_, go, arrived = coord.status(key)
        for x in nodes[1:]:
            cx.fetch(x["node"], d / x["node"])
        bad = {k: v for k, v in rcs.items() if v != 0}
        secs = int(time.time() - t0)
        if bad or state_ != "go":
            log_line(progress, f"FAILED {name} ({secs}s, exits {rcs}, start {state_} {arrived} arrived; "
                               f"see logs/{name}.<node>.log) t={time.time():.3f}")
            cx.purge(outroot / "logs" / "purge_after_failure.log")
            return
        try:
            sums = []
            for x in nodes:
                f = d / x["node"] / f"{cx.rid(1, x['n'])}_BRANCH_CREATE_threads1_summary.json"
                sums.append(json.loads(f.read_text()))
            m = dict(sums[0])
            m["run_id"] = cx.rid(1, total)
            m["num_branches"] = total
            m["num_processes"] = sum(s_["num_processes"] for s_ in sums)
            k = int(os.environ.get("BENCH_BG_SAMPLE") or 20)   # fast_client samples background rows 1/k
            bgs = [dict(s_.get("background") or {}) for s_ in sums]
            for b_ in bgs:
                for key in ("ops_in_window", "throughput"):
                    if b_.get(key) is not None:
                        b_[f"{key}_sampled"] = b_[key]
                        b_[key] = b_[key] * k
                b_["row_sample"] = k
            m["multi_client"] = {
                "go_time": go, "creator": clients[0], "remote_cpu_from": clients[0],
                "nodes": [{"node": x["node"], "branches": x["n"], "procs": x["procs"],
                           "num_processes": s_["num_processes"], "elapsed_time": s_.get("elapsed_time"),
                           "background": b_, "client_cpu_cores": s_.get("client_cpu_cores"),
                           "summary": os.path.relpath(d / x["node"], outroot)} for x, s_, b_ in zip(nodes, sums, bgs)],
                "background_ops_in_window": sum((b_.get("ops_in_window") or 0) for b_ in bgs),
                "background_failed": sum((b_.get("failed") or 0) for b_ in bgs),
            }
        except Exception as e:  # noqa: BLE001
            log_line(progress, f"CHECK-FAIL {name} ({secs}s) merge failed: {e!r} t={time.time():.3f}")
            return
        (d / f"{cx.rid(1, total)}_BRANCH_CREATE_threads1_summary.json").write_text(json.dumps(m, indent=2))
        problems = []
        if sums[0].get("failed_ops"):
            problems.append(f"failed_ops={sums[0]['failed_ops']}")
        if m["multi_client"]["background_failed"]:
            problems.append(f"background failed={m['multi_client']['background_failed']}")
        for x in m["multi_client"]["nodes"]:
            if x["num_processes"] != x["procs"]:
                problems.append(f"{x['node']}: num_processes {x['num_processes']} != {x['procs']}")
        labels = dict(y.split("=", 1) for y in filter(None, (z.strip() for z in remote_spec.split(";"))))
        for label, url in labels.items():
            if "set=server" in url and ((m.get("remote_cpu") or {}).get(label) or {}).get("node_cores") is None:
                problems.append(f"no remote_cpu.{label}.node_cores")
        lat = (m.get("latency") or {}).get("ops") or {}
        r2 = lambda x: None if x is None else round(x, 2)  # noqa: E731
        per = " ".join(f"{x['node']}:{x['branches']}b/{(x['background'] or {}).get('ops_in_window')}bg"
                       for x in m["multi_client"]["nodes"])
        word = "DONE " if not problems else "CHECK-FAIL"
        log_line(progress, f"{word} {name} ({secs}s) p50={lat.get('p50', 0) * 1e3:.1f}ms mean={lat.get('mean', 0) * 1e3:.1f}ms "
                           f"window={m.get('elapsed_time', 0):.1f}s procs={m['num_processes']} "
                           f"bg_ops={m['multi_client']['background_ops_in_window']} client_cpu={r2(m.get('client_cpu_cores'))} "
                           f"clients=[{per}] {'PROBLEMS: ' + ', '.join(problems) if problems else 'ok'} t={time.time():.3f}")

    for total in ns:
        if done(f"create_{bg.lower()}_n{total}"):
            log_line(progress, f"SKIP  create_{bg.lower()}_n{total} (done before)")
            continue
        one(total, 1)
    for total in ns:
        if not done(f"create_{bg.lower()}_n{total}"):
            log_line(progress, f"RETRY create_{bg.lower()}_n{total}")
            one(total, 2)
    cx.purge(outroot / "logs" / "purge_end.log")
    log_line(progress, "=== multi-client scaling_branch done")
    return 0


# ---------------------------------------------------------------- run-ceiling
def run_ceiling(a) -> int:
    """ceiling_check.py on every node with its share of each N, each measurement started together."""
    import csv
    cx = Ctx(a)
    topo, clients = cx.topo, cx.clients
    me = first_client(cx)
    sync_addr = f"{me.lan}:{a.port}"
    outroot = Path(a.outdir).resolve() / cx.backend / "scaling_data"
    cdir = outroot / "ceiling"
    (outroot / "logs").mkdir(parents=True, exist_ok=True)
    cdir.mkdir(parents=True, exist_ok=True)
    progress = outroot / "progress.log"
    ns = [int(x) for x in a.ns.split(",")]
    kinds = a.kinds.split(",")
    if progress.exists() and any(" DONE  ceiling " in l for l in progress.read_text().splitlines()):
        log_line(progress, "SKIP  ceiling (done before)")
        return 0
    coord = Coordinator(a.port)
    prefix = f"ceiling.{int(time.time())}"
    used = [c for i, c in enumerate(clients) if shares(max(ns), len(clients))[i:i + 1]]
    per_node = {c: [shares(n, len(clients))[i] if i < min(n, len(clients)) else 0 for n in ns]
                for i, c in enumerate(used)}
    for kind in kinds:
        for j, n in enumerate(ns):
            coord.expect(f"{prefix}.{kind}.{n}", sum(1 for c in used if per_node[c][j] > 0))
    cx.purge(outroot / "logs" / "purge_ceiling_before.log")
    log_line(progress, f"START ceiling t={time.time():.3f} ns={ns} kinds={kinds} nodes={len(used)} "
                       f"shares_of_max={[per_node[c][-1] for c in used]}")
    t0 = time.time()
    procs = []
    for i, c in enumerate(used):
        out = cdir / f"{c}.csv"
        cmd = (f"cd {REMOTE_REPO if i else REPO} && export PATH=$HOME/.local/bin:$PATH && "
               f"uv run python scripts/workloads/ceiling_check.py --topology {shlex.quote(a.topology_remote or a.topology)} "
               f"--backend {cx.backend} --ns {a.ns} --ns-local {','.join(map(str, per_node[c]))} --kinds {a.kinds} "
               f"--seconds {a.seconds} --procs {cx.max_procs} --out {out} --sync-addr {sync_addr} "
               f"--sync-prefix {prefix} --no-purge")
        logf = (outroot / "logs" / f"ceiling.{c}.log").open("w")
        procs.append((c, cx.launch(c, cmd, logf), logf))
    rcs = {}
    while len(rcs) < len(procs):
        for c, p, _ in procs:
            if c not in rcs and p.poll() is not None:
                rcs[c] = p.returncode
                if p.returncode != 0:
                    for kind in kinds:
                        for n in ns:
                            coord.abort(f"{prefix}.{kind}.{n}", f"{c} exited {p.returncode}")
        time.sleep(1)
    for _, _, f in procs:
        f.close()
    for c in used[1:]:
        cx.fetch(c, cdir)
    cx.purge(outroot / "logs" / "purge_ceiling_after.log")
    secs = int(time.time() - t0)
    bad = {k: v for k, v in rcs.items() if v != 0}
    if bad:
        log_line(progress, f"FAILED ceiling ({secs}s, exits {rcs}; see logs/ceiling.<node>.log) t={time.time():.3f}")
        return 1
    rows = {}
    for c in used:
        f = cdir / f"{c}.csv"
        if not f.exists():
            continue
        for r in csv.DictReader(f.open()):
            rows.setdefault((r["kind"], int(r["busy_branches_total"])), []).append((c, r))
    merged = []
    for (kind, n), lst in sorted(rows.items(), key=lambda x: (kinds.index(x[0][0]), x[0][1])):
        ops = sum(float(r["ops_per_s"]) for _, r in lst)
        wsum = lambda key: (sum(float(r[key]) * float(r["ops_per_s"]) for _, r in lst if r.get(key))  # noqa: E731
                            / ops if ops else None)
        row = {"backend": cx.backend, "kind": kind, "busy_branches": n, "nodes": len(lst),
               "client_processes": sum(int(r["client_processes"]) for _, r in lst), "ops_per_s": round(ops),
               "errors": sum(int(r["errors"]) for _, r in lst),
               "p50_ms_opsweighted": round(wsum("p50_ms"), 2) if ops else None,
               "p95_ms_opsweighted": round(wsum("p95_ms"), 2) if ops else None,
               "client_cpu_cores": round(sum(float(r["client_cpu_cores"]) for _, r in lst), 1)}
        for key in ("server_node_cores", "server_proc_cores", "computes_proc_cores", "copy_proc_cores"):
            vals = [float(r[key]) for _, r in lst if r.get(key) not in (None, "")]
            row[f"sum_{key}"] = round(sum(vals), 1) if vals else None
        seed = [float(r["seed_node_cores"]) for _, r in lst if r.get("seed_node_cores") not in (None, "")]
        row["seed_node_cores"] = round(max(seed), 1) if seed else None
        row["per_node_ops_per_s"] = " ".join(f"{c}:{r['ops_per_s']}" for c, r in lst)
        merged.append(row)
    out = outroot / "ceiling_check.csv"
    if merged:
        keys = list(dict.fromkeys(k for r in merged for k in r))
        with out.open("w", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=keys)
            wr.writeheader()
            wr.writerows(merged)
    summary = " ".join(f"{r['kind']}@{r['busy_branches']}={r['ops_per_s']}/s" for r in merged)
    word = "DONE " if merged and len(merged) == len(kinds) * len(ns) else "CHECK-FAIL"
    log_line(progress, f"{word} ceiling ({secs}s) {summary} rows={len(merged)} t={time.time():.3f}")
    return 0


# ---------------------------------------------------------------- remerge
def remerge(a) -> int:
    """Rebuild the merged summaries of finished points from the per-node results (e.g. after a merge fix); the GO
    time is taken from each point's progress line when present."""
    cx = Ctx(a)
    clients, max_procs = cx.clients, cx.max_procs
    outroot = Path(a.outdir).resolve() / cx.backend / "scaling_data"
    for total in [int(x) for x in a.ns.split(",")]:
        for op in a.ops.split(","):
            nodes = [{"node": c, "n": k, "procs": min(max_procs, k)} for c, k in zip(clients, shares(total, len(clients)))]
            for x in nodes:
                d = outroot / "data_ops" / op / x["node"]
                x["summary"] = str(d / f"{cx.rid(x['n'], x['n'])}_{op}_threads{x['n']}_summary.json")
                x["parquet"] = str(d / f"{cx.rid(x['n'], x['n'])}.parquet")
            if not all(Path(x["summary"]).exists() for x in nodes):
                print(f"skip {op} n={total}: per-node summaries missing")
                continue
            out = outroot / "data_ops" / op / f"{cx.rid(total, total)}_{op}_threads{total}_summary.json"
            go = (json.loads(out.read_text()).get("multi_client") or {}).get("go_time") if out.exists() else None
            m = merge(op, total, nodes, go, outroot)
            out.write_text(json.dumps(m, indent=2))
            print(f"{op} n={total}: {done_line(m)}")
    return 0


# ---------------------------------------------------------------- deploy
def deploy(a) -> int:
    if getattr(a, "backend", "neon") == "pgcopy":
        raise SystemExit("pgcopy: scripts/setup/pgcopy.py TOPO deploy-client deploys every host")
    from neon import Neon  # scripts/setup/neon.py
    topo = Topo(a.topology)
    nn = Neon(topo, False)
    for c in a.clients.split(","):
        if c == topo.neon["client"]:
            continue  # neon.py TOPO deploy-client
        node = topo.node(c)
        print(f"== {c} ({node.host}): repo, venv, state_dir, neon.env, neon_seed.json", flush=True)
        nn.deploy(node)
        nn.ensure_state_dir(node)
        for f in ("neon.env", "neon_seed.json"):
            p = Path(f"{topo.state_dir}/{f}")
            if not p.exists():
                raise SystemExit(f"no {p}: run neon.py {a.topology} seed / env-init / deploy-client first")
            nn.r.put(node, str(p), p.read_text(), mode=0o600)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("deploy", "run", "remerge", "run-branch", "run-ceiling"):
        p = sub.add_parser(name)
        p.add_argument("--topology", required=True)
        p.add_argument("--backend", choices=["neon", "pgcopy"], default="neon")
        p.add_argument("--clients", default="",
                       help="client nodes, the topology's client first: b0,b1,... (pgcopy default: every host)")
        if name != "deploy":
            p.add_argument("--outdir", required=True)
            p.add_argument("--ns", default="")
        if name in ("run", "remerge"):
            p.add_argument("--ops", default="READ,RANGE_READ,INSERT,UPDATE")
        if name == "run-branch":
            p.add_argument("--background", default="")
            p.add_argument("--creates", type=int, default=0)
        if name == "run-ceiling":
            p.add_argument("--kinds", default="point,range,insert,update")
            p.add_argument("--seconds", type=float, default=30)
        if name in ("run", "run-branch", "run-ceiling"):
            p.add_argument("--port", type=int, default=DEFAULT_PORT)
            p.add_argument("--setup-parallelism", type=int, default=64,
                           help="total branch-setup parallelism, divided over the nodes of a point")
            p.add_argument("--topology-remote", default="", help="topology path on the other nodes (default: same)")
    a = ap.parse_args()
    if a.backend == "neon" and not a.clients:
        ap.error("--clients is required for neon")
    if a.cmd == "run-ceiling" and not a.ns:
        a.ns = "128,256,512,1024"
    os.chdir(REPO)
    return {"deploy": deploy, "run": run, "remerge": remerge, "run-branch": run_branch,
            "run-ceiling": run_ceiling}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
