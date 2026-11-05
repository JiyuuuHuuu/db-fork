#!/usr/bin/env python3
"""Ceiling check: is a high-N plateau of scaling_data the benchmark client or the server? (experiments 3, 5, 7)

Experiment 5's minimal asyncio client, for Neon and Doltgres: the same load shape as the benchmark (N busy
branches, one connection per branch, one request in flight on each) without the runner's per-operation
bookkeeping. If it reaches the benchmark's ceiling with the server's cores full and the client's not, the ceiling
is the server.

Kinds (experiment 5's): point and range (100 rows) reads with keys over all W warehouses, inserts (warehouse id
above W, unique per measurement; item ids unique per connection), point updates.

Neon: a run root and max(N) branches of it, each with its compute (compute opts as in scaling_data: LFC_MB,
statement log off, data directory on disk), every compute's cache filled first with the benchmark's prewarm
statements (heap prefix, then stock_pkey); purged at the end.
Doltgres: a fresh database copied from the seed (the runner's helpers), branches rc_0..rc_{max(N)-1} of its main,
one connection per branch to "<db>/rc_i" (experiment 7's client: psycopg AsyncClientCursor); the database is
dropped at the end.
pgcopy (experiment 13): a run root copied from the seed instance, then max(N) instances copied from it onto this
node's agent (PGCOPY_LOCAL_AGENT; 16 at a time), each prewarmed like Neon's computes, connections on 127.0.0.1.
Several nodes at once (scripts/workloads/multi_client/multi_scaling.py run-ceiling): --ns-local gives this node's
share of each N in --ns, --sync-addr/--sync-prefix start every measurement on every node together (GO per kind and
N from the coordinator), --no-purge leaves the other nodes' instances alone; rows carry busy_branches_total.

    uv run python scripts/workloads/ceiling_check.py --topology T --backend neon|doltgres \\
        [--ns 128,256,512,1024] [--kinds point,range,insert,update] [--seconds 30] [--procs MAX_PROCS] \\
        [--out DIR/<backend>/scaling_data/ceiling_check.csv | --outdir DIR] [--dry-run]

Run on the client node under the client placement (scaling_data.sh --ceiling does both, with the per-run CPU
agent). Server CPU: DBFORK_REMOTE_CPU (as in the benchmark) and, with DBFORK_SERVER_PID, the server process's
utime + stime. Writes one CSV row per (kind, N).
"""
import argparse
import asyncio
import csv
import json
import multiprocessing as mp
import os
import random
import resource
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from cold_start import dolt_fresh_db, dolt_uri, neon_compute_opts, out_root, workload_env  # noqa: E402

DIST = "'" + "x" * 24 + "'"
TICK = os.sysconf("SC_CLK_TCK")
DB_DOLT = "ceiling_check"


def make_sql(W):
    return {
        "point": lambda r, st: f"SELECT * FROM stock WHERE s_w_id = {r.randint(1, W)} AND s_i_id = {r.randint(1, 100000)}",
        "range": lambda r, st: (lambda k: f"SELECT * FROM stock WHERE s_w_id = {r.randint(1, W)} AND s_i_id >= {k} "
                                          f"AND s_i_id <= {k + 99}")(r.randint(1, 99900)),
        "insert": lambda r, st: (f"INSERT INTO stock VALUES ({st.next()}, {st.w}, {r.randint(10, 100)}, "
                                 + ", ".join([DIST] * 10) + f", 0, 0, 0, 'ceiling check', 1)"),
        "update": lambda r, st: (f"UPDATE stock SET s_quantity = {r.randint(10, 100)}, s_ytd = s_ytd + 1, "
                                 f"s_order_cnt = s_order_cnt + 1, s_data = 'ceiling check {r.randint(0, 9999)}' "
                                 f"WHERE s_w_id = {r.randint(1, W)} AND s_i_id = {r.randint(1, 100000)}"),
    }


class _Keys:
    """Fresh primary keys for inserts: warehouse id unique per measurement, item ids unique per connection."""

    def __init__(self, w, base):
        self.w = w
        self.k = base

    def next(self):
        self.k += 1
        return self.k


async def _connect(backend, target, env):
    import psycopg
    if backend == "pgcopy":
        host, port = target
        return await psycopg.AsyncConnection.connect(
            host=host, port=port, dbname=env.get("PGCOPY_DB", "bench"), user=env.get("PGCOPY_PG_USER", "bench"),
            password=env.get("PGCOPY_PG_PASSWORD", ""), autocommit=True)
    if backend == "neon":
        host, port = target
        return await psycopg.AsyncConnection.connect(
            host=host, port=port, dbname=env.get("NEON_LOCAL_DB", "bench"),
            user=env.get("NEON_LOCAL_PG_USER", "bench"), password=env.get("NEON_LOCAL_PG_PASSWORD", ""),
            autocommit=True)
    return await psycopg.AsyncConnection.connect(
        host=env.get("DOLT_HOST", "127.0.0.1"), port=int(env.get("DOLT_PORT", "5433")),
        user=env.get("DOLT_USER", "postgres"), password=env.get("DOLT_PASSWORD", "password"),
        dbname=f"{DB_DOLT}/{target}", autocommit=True, cursor_factory=psycopg.AsyncClientCursor)


async def _run(backend, targets, kind, seconds, start_at, out, w, base, W, env):
    sql = make_sql(W)[kind]
    conns = [await _connect(backend, t, env) for t in targets]
    rnd = random.Random()
    lat, errors = [], 0
    while time.time() < start_at:
        await asyncio.sleep(0.005)
    stop = start_at + seconds

    async def loop(conn, keys):
        nonlocal errors
        async with conn.cursor() as cur:
            while time.time() < stop:
                q = sql(rnd, keys)
                t = time.perf_counter()
                try:
                    await cur.execute(q)
                    if cur.description is not None:
                        await cur.fetchall()
                    lat.append(time.perf_counter() - t)
                except Exception:
                    errors += 1

    ru0 = resource.getrusage(resource.RUSAGE_SELF)
    await asyncio.gather(*[loop(c, _Keys(w, base + 1_000_000 * i)) for i, c in enumerate(conns)])
    ru1 = resource.getrusage(resource.RUSAGE_SELF)
    for c in conns:
        await c.close()
    out.put((lat, errors, (ru1.ru_utime + ru1.ru_stime) - (ru0.ru_utime + ru0.ru_stime)))


def _proc(*a):
    asyncio.run(_run(*a))


def server_cpu():
    pid = os.environ.get("DBFORK_SERVER_PID", "")
    if not pid.isdigit():
        return None
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().rsplit(")", 1)[1].split()
        return (int(fields[11]) + int(fields[12])) / TICK
    except (OSError, ValueError, IndexError):
        return None


def sync_go(addr, key):
    """Wait for the coordinator's GO for key (multi-node ceiling: every node's measurement starts together)."""
    import socket
    host, port = addr.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=60) as s:
        s.sendall(f"{key}\n".encode())
        s.settimeout(3600)
        line = s.makefile("r").readline().strip()
    if line != "GO":
        raise RuntimeError(f"ceiling sync {key}: {line or 'coordinator closed the connection'}")


def measure(backend, targets, kind, seconds, procs, w, W, sync=None):
    from microbench.runner2 import RemoteCpuSampler
    n = len(targets)
    nproc = min(procs, n)
    groups = [targets[i::nproc] for i in range(nproc)]
    ctx = mp.get_context("spawn")
    out = ctx.Queue()
    if sync:
        sync_go(*sync)    # then a common start, a few seconds out (clocks NTP-synchronised)
        start_at = time.time() + 3 + 1024 / 50
    else:
        start_at = time.time() + 5 + n / 50
    env = dict(os.environ)
    ps = [ctx.Process(target=_proc, args=(backend, g, kind, seconds, start_at, out, w, 200_000 + 50_000_000 * j,
                                          W, env))
          for j, g in enumerate(groups)]
    sampler = RemoteCpuSampler().start()
    for p in ps:
        p.start()
    while time.time() < start_at:
        time.sleep(0.01)
    s0 = server_cpu()
    time.sleep(seconds)
    s1 = server_cpu()
    lat, errors, client_cpu = [], 0, 0.0
    for _ in ps:
        got, err, cpu = out.get()
        lat += got
        errors += err
        client_cpu += cpu
    for p in ps:
        p.join()
    time.sleep(1.5)
    sampler.stop()
    rc = sampler.usage(start_at, start_at + seconds) or {}
    lat.sort()
    row = {"backend": backend, "kind": kind, "busy_branches": n, "client_processes": nproc,
           "ops_per_s": round(len(lat) / seconds), "errors": errors,
           "p50_ms": round(lat[len(lat) // 2] * 1e3, 2) if lat else None,
           "p95_ms": round(lat[int(len(lat) * 0.95)] * 1e3, 2) if lat else None,
           "client_cpu_cores": round(client_cpu / seconds, 1),
           "server_cpu_cores": None if s0 is None or s1 is None else round((s1 - s0) / seconds, 2)}
    for label, v in rc.items():
        row[f"{label}_proc_cores"] = None if v["proc_cores"] is None else round(v["proc_cores"], 1)
        row[f"{label}_node_cores"] = None if v["node_cores"] is None else round(v["node_cores"], 1)
        if v.get("disk_writes_per_s") is not None:
            row[f"{label}_ssd_reads_per_s"] = round(v["disk_reads_per_s"])
            row[f"{label}_ssd_writes_per_s"] = round(v["disk_writes_per_s"])
    return row


def neon_prewarm(target, stmts, prefix="NEON_LOCAL"):
    import psycopg2
    host, port = target
    db = os.environ.get(f"{prefix}_DB", "bench")
    c = psycopg2.connect(host=host, port=port, dbname=db,
                         user=os.environ.get(f"{prefix}_PG_USER", "bench"),
                         password=os.environ.get(f"{prefix}_PG_PASSWORD", ""), connect_timeout=60)
    c.autocommit = True
    with c.cursor() as cur:
        for s in stmts:
            cur.execute(s)
    c.close()


def dolt_branches(n, create):
    import psycopg2
    c = psycopg2.connect(dolt_uri(DB_DOLT))
    c.autocommit = True
    cur = c.cursor()
    for i in range(n):
        try:
            cur.execute(f"SELECT dolt_branch('rc_{i}', 'main')" if create else f"SELECT dolt_branch('-D', 'rc_{i}')")
        except Exception:
            pass   # already there / already gone
    c.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topology", required=True)
    ap.add_argument("--backend", required=True, choices=["neon", "doltgres", "pgcopy"])
    ap.add_argument("--ns", default="128,256,512,1024")
    ap.add_argument("--ns-local", default="", help="pgcopy, several nodes: this node's share of each N in --ns")
    ap.add_argument("--sync-addr", default="", help="coordinator host:port (several nodes)")
    ap.add_argument("--sync-prefix", default="", help="key prefix: GO per <prefix>.<kind>.<N>")
    ap.add_argument("--no-purge", action="store_true", help="pgcopy: keep the other nodes' instances (several nodes)")
    ap.add_argument("--kinds", default="point,range,insert,update")
    ap.add_argument("--seconds", type=float, default=30)
    ap.add_argument("--procs", type=int, help="client processes at most (default: the topology's MAX_PROCS)")
    ap.add_argument("--out", help="CSV (default <outdir>/<backend>/scaling_data/ceiling_check.csv)")
    ap.add_argument("--outdir")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    v = workload_env(args.topology, args.backend, dry=args.dry_run)
    W = int(v["W"])
    procs = args.procs or int(v["MAX_PROCS"])
    if not args.out and not args.outdir:
        raise SystemExit("--out or --outdir is required")
    out = Path(args.out) if args.out else out_root(args.outdir, args.backend, "scaling_data", False) / "ceiling_check.csv"
    ns = [int(x) for x in args.ns.split(",")]
    ns_local = [int(x) for x in args.ns_local.split(",")] if args.ns_local else list(ns)
    if len(ns_local) != len(ns):
        raise SystemExit("--ns-local needs one share per --ns entry")
    sync = (args.sync_addr, args.sync_prefix) if args.sync_addr else None
    kinds = args.kinds.split(",")
    blocks = int(v["PREWARM_HEAP_BLOCKS"])
    stmts = ["SET maintenance_io_concurrency = 128", f"SELECT pg_prewarm('stock', 'buffer', 'main', 0, {blocks - 1})",
             "SELECT pg_prewarm('stock_pkey')"] if args.backend in ("neon", "pgcopy") else []
    opts = neon_compute_opts(int(v["LFC_MB"]), statement_log=False, pgdata_on_disk=True) if args.backend == "neon" else {}
    print(f"ceiling check ({args.backend}): W={W} ns={ns} kinds={kinds} seconds={args.seconds} procs<={procs} "
          f"compute_opts={opts} prewarm={len(stmts)} statements out={out}", flush=True)
    if args.dry_run:
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    if args.backend == "pgcopy":
        from dblib import pgcopy as pc
        os.environ["PGCOPY_INSTANCE_OPTS"] = json.dumps({"statement_log": False})
        if args.no_purge:
            root = pc.new_run("ceiling")
            pc.create_run_root(root)
        else:
            pc.setup_run("ceiling")
        try:
            k = max(ns_local)
            names = [f"cc_{i}" for i in range(k)]
            t0 = time.time()
            agent = pc.local_agent()
            with ThreadPoolExecutor(16) as ex:
                made = list(ex.map(lambda nm: pc.create_instance(pc.instance_id_for(nm), pc.instance_id_for("main"),
                                                                 agent=agent or None), names))
            targets = [(pc.connect_host(r["agent"], r["host"]), int(r["port"])) for r in made]
            print(f"{len(targets)} instances ready in {time.time() - t0:.0f}s", flush=True)
            t0 = time.time()
            with ThreadPoolExecutor(32) as ex:
                list(ex.map(lambda t: neon_prewarm(t, stmts, "PGCOPY"), targets))
            print(f"prewarmed {len(targets)} instances in {time.time() - t0:.0f}s", flush=True)
            rows = sweep("pgcopy", targets, kinds, ns, args.seconds, procs, W, ns_local=ns_local, sync=sync)
        finally:
            if not args.no_purge:
                pc.teardown_run("ceiling")
    elif args.backend == "neon":
        os.environ["NEON_LOCAL_COMPUTE_OPTS"] = json.dumps(opts)
        from dblib import neon_local as nl
        nl.setup_run("ceiling")
        try:
            names = [f"cc_{i}" for i in range(max(ns))]
            for name in names:
                nl.create_timeline(nl.timeline_id_for(name), nl.timeline_id_for(nl.MAIN_BRANCH))
            with ThreadPoolExecutor(64) as ex:
                computes = list(ex.map(lambda nm: nl.ensure_compute(nl.timeline_id_for(nm)), names))
            targets = [(r["host"], int(r["port"])) for r in computes]
            print(f"{len(targets)} branches with computes ready", flush=True)
            t0 = time.time()
            with ThreadPoolExecutor(64) as ex:
                list(ex.map(lambda t: neon_prewarm(t, stmts), targets))
            print(f"prewarmed {len(targets)} computes in {time.time() - t0:.0f}s", flush=True)
            rows = sweep("neon", targets, kinds, ns, args.seconds, procs, W)
        finally:
            nl.teardown_run("ceiling")
    else:
        mgr = dolt_fresh_db(DB_DOLT, v["SEED_DB"], W)
        try:
            dolt_branches(max(ns), create=True)
            rows = sweep("doltgres", [f"rc_{i}" for i in range(max(ns))], kinds, ns, args.seconds, procs, W)
        finally:
            dolt_branches(max(ns), create=False)
            mgr.cleanup()
    if rows:
        keys = list(dict.fromkeys(k for r in rows for k in r))
        with open(out, "w", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=keys)
            wr.writeheader()
            wr.writerows(rows)
        print("wrote", out)


def sweep(backend, targets, kinds, ns, seconds, procs, W, ns_local=None, sync=None):
    rows = []
    w = W
    for kind in kinds:
        for j, n in enumerate(ns):
            w += 1
            k = ns_local[j] if ns_local else n
            if k == 0:   # no share of this N on this node (several nodes): not part of the measurement
                continue
            key = (sync[0], f"{sync[1]}.{kind}.{n}") if sync else None
            r = measure(backend, targets[:k], kind, seconds, procs, w, W, sync=key)
            r["busy_branches_total"] = n
            print(r, flush=True)
            rows.append(r)
    return rows


if __name__ == "__main__":
    main()
