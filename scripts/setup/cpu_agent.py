"""CPU, disk and memory sampler for a server node (stdlib only, Python >= 3.10: it also runs on CloudLab nodes).

    python3 cpu_agent.py --set server=2,6,10-38:4 [--set clients=...] [--out data/.../cpu/<run>.csv] \\
        [--port 9010] [--bind 127.0.0.1] [--interval 0.5] [--disk nvme2c2n1] [--numa-node 2] \\
        [--run-dir NEON_RUN_DIR] [--group-pidfile doltgres=<root>/doltgres.pid] \\
        [--group-comm pageserver=pageserver --group-comm computes=postgres ...]

Merged from experiment 7's agent (Doltgres, one --pid) and experiment 11's (Neon groups from pid files). Sets:
--set name=<cpu list> (a,b-c,d-e:step), `all` (every online CPU) or `node:N` (the CPUs of NUMA node N).

Process groups (any mix of):
- --run-dir DIR: the single-host Neon deployment's pid files (experiment 11): broker, storcon-pg, hook, storcon,
  safekeeper, pageserver, agent; the pageserver's children are "walredo", the agent's children "compute_ctl" (comm
  compute_ctl) and "computes" (every compute Postgres process).
- --group-pidfile name=PATH: the process whose pid is the first line of PATH (a pid file or a postmaster.pid) and its
  descendants.
- --group-comm name=c1,c2: every process whose /proc/<pid>/comm is one of c1, c2 (truncated to 15 characters, as the
  kernel does: storage_controller -> storage_control), and its descendants. For services started by systemd-run.
A process belongs to the nearest root among itself and its ancestors, except that every descendant of a pageserver
is walredo (its Postgres children would otherwise match comm postgres). A process that exits keeps its last CPU
reading in its group's total. "Server" groups are all groups but agent and hook (the Neon compute agent and the
compute hook are benchmark scaffolding, not Neon).

Outputs:
- With --out: every --interval seconds, one row per CPU set to --out (t, set, ncpu, busy, usr, sys, softirq, iowait
  in cores, proc_cores = CPU of the server groups on the server row, per_core busy fractions); every --proc-interval seconds, rows to <out>.procs.csv: t, group, nproc,
  cores, rss_mb per group; "server" (all server groups); "other_server" (busy of set server minus every group:
  foreign load, kernel); disk_busy, disk_read_mbs, disk_write_mbs, disk_iops (with --disk); mem_MemFree,
  mem_FilePages, mem_AnonPages, mem_Shmem, mem_Available (MemFree + file-backed page cache, MB; NUMA node
  --numa-node, else the machine). Experiment 11 named the "server" and "other_server" rows "neon" and "other_node2".
- Without --out (service mode on remote nodes): HTTP only.
- GET /cpu?set=<name>: cumulative counters in the format microbench/runner2.py's RemoteCpuSampler reads
  ({t, ncpu, node_busy_s, node_iowait_s}; set=server adds proc_s = CPU of the server groups when any group is
  configured, and with --disk disk_busy_s, disk_reads, disk_writes, disk_read_bytes, disk_write_bytes).
  GET /cpu?group=<g> ({t, ncpu, proc_s} of one group; g=server or neon: all server groups). Counters are computed
  on request (one /proc scan serves requests within 0.2 s).

Stops on SIGTERM / SIGINT.
"""

import argparse
import json
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

TICK = os.sysconf("SC_CLK_TCK")
PAGE = os.sysconf("SC_PAGE_SIZE")
# experiment 11's pid files in the Neon run dir -> group
NEON_PIDFILES = {"broker": "broker", "storcon-pg": "storcon_pg", "hook": "hook", "storcon": "storcon",
                 "safekeeper": "safekeeper", "pageserver": "pageserver", "agent": "agent"}
NEON_GROUPS = ["pageserver", "walredo", "safekeeper", "storcon", "storcon_pg", "broker", "compute_ctl", "computes",
               "agent", "hook"]
NOT_SERVER = ("agent", "hook")


def parse_cpus(spec):
    """'2,6,10-14,20-38:4' | 'all' | 'node:N' -> CPU ids."""
    if spec == "all":
        with open("/sys/devices/system/cpu/online") as f:
            spec = f.read().strip()
    elif spec.startswith("node:"):
        with open(f"/sys/devices/system/node/node{int(spec[5:])}/cpulist") as f:
            spec = f.read().strip()
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if ":" in part:
            part, s = part.split(":")
            step = int(s)
        if "-" in part:
            a, b = (int(x) for x in part.split("-"))
            out.extend(range(a, b + 1, step))
        else:
            out.append(int(part))
    return out


def cpu_times() -> dict:
    """{cpu: (busy, usr, sys, softirq, iowait, total)} in seconds."""
    out = {}
    with open("/proc/stat") as f:
        for line in f:
            if not line.startswith("cpu") or line.startswith("cpu "):
                continue
            p = line.split()
            user, nice, system, idle, iowait, irq, softirq, steal = (int(x) for x in p[1:9])
            busy = user + nice + system + irq + softirq + steal
            out[int(p[0][3:])] = tuple(v / TICK for v in (busy, user + nice, system, softirq, iowait,
                                                          busy + idle + iowait))
    return out


def disk_counters(dev) -> dict:
    if not dev:
        return {}
    with open("/proc/diskstats") as f:
        for line in f:
            p = line.split()
            if p[2] == dev:
                return {"disk_busy_s": int(p[12]) / 1000.0, "disk_reads": int(p[3]), "disk_read_bytes": int(p[5]) * 512,
                        "disk_writes": int(p[7]), "disk_write_bytes": int(p[9]) * 512}
    return {}


def meminfo(node) -> dict:
    """MB by key, of NUMA node `node` or (None) the machine. /proc/meminfo has no FilePages: Cached + Buffers."""
    out = {}
    path = "/proc/meminfo" if node is None else f"/sys/devices/system/node/node{node}/meminfo"
    k = 0 if node is None else 2      # node files prefix each line with "Node N"
    try:
        with open(path) as f:
            for line in f:
                p = line.split()
                out[p[k].rstrip(":")] = int(p[k + 1]) / 1024
    except OSError:
        pass
    if node is None:
        out["FilePages"] = out.get("Cached", 0) + out.get("Buffers", 0)
    return out


class ProcScanner:
    """Cumulative CPU seconds per process group, with exited processes' last readings kept."""

    def __init__(self, run_dir, pidfiles, comms):
        self.run_dir = run_dir
        self.pidfiles = dict(pidfiles)                  # group -> path
        self.comms = {c[:15]: g for g, cs in comms.items() for c in cs}
        groups = list(NEON_GROUPS) if run_dir else []
        for g in list(self.pidfiles) + list(comms):
            if g not in groups:
                groups.append(g)
        # groups the classification derives from others
        if "pageserver" in groups and "walredo" not in groups:
            groups.append("walredo")
        if "agent" in groups:
            groups += [g for g in ("compute_ctl", "computes") if g not in groups]
        self.groups = groups
        self.server = [g for g in groups if g not in NOT_SERVER]
        self.last = {}                  # pid -> (group, cpu_s, starttime)
        self.dead = {g: 0.0 for g in groups}
        self.lock = threading.Lock()
        self.cache = None               # (t, totals)

    def roots(self) -> dict:
        files = {}
        if self.run_dir:
            files.update({os.path.join(self.run_dir, f"{n}.pid"): g for n, g in NEON_PIDFILES.items()})
        files.update({p: g for g, p in self.pidfiles.items()})
        out = {}
        for path, group in files.items():
            try:
                with open(path) as f:
                    out[int(f.readline().strip())] = group
            except (OSError, ValueError):
                pass
        return out

    def scan(self, want_rss=False):
        pid_roots = self.roots()
        procs = {}                      # pid -> (ppid, comm, cpu_s, starttime, rss)
        for d in os.listdir("/proc"):
            if not d.isdigit():
                continue
            try:
                with open(f"/proc/{d}/stat") as f:
                    s = f.read()
            except OSError:
                continue
            i, j = s.index("("), s.rindex(")")
            comm = s[i + 1:j]
            fl = s[j + 2:].split()
            procs[int(d)] = (int(fl[1]), comm, (int(fl[11]) + int(fl[12])) / TICK, int(fl[19]),
                             int(fl[21]) * PAGE if want_rss else 0)
        roots = {pid: self.comms[p[1]] for pid, p in procs.items() if p[1] in self.comms}
        roots.update(pid_roots)         # a pid file is more specific than a comm
        nearest = {}                    # pid -> group of the nearest root among itself and its ancestors

        def near(pid):
            chain, p = [], pid
            while p in procs and p not in nearest and p not in roots and len(chain) < 256:
                chain.append(p)
                p = procs[p][0]
            g = roots.get(p) if p in roots else nearest.get(p)
            for q in chain:
                nearest[q] = g
            if p in roots:
                nearest[p] = roots[p]
            return g

        totals = {g: 0.0 for g in self.groups}
        nproc = {g: 0 for g in self.groups}
        rss = {g: 0 for g in self.groups}
        seen = {}
        for pid, (ppid, comm, cpu, start, r) in procs.items():
            above = near(ppid) if ppid in procs else None
            if above == "pageserver" and pid not in pid_roots:
                g = "walredo"
            elif pid in roots:
                g = roots[pid]
            elif above == "agent":
                g = "compute_ctl" if comm == "compute_ctl" else "computes" if comm == "postgres" else "agent"
            else:
                g = above
            if g is None:
                continue
            totals[g] += cpu
            nproc[g] += 1
            rss[g] += r
            seen[pid] = (g, cpu, start)
        with self.lock:
            for pid, (g, cpu, start) in self.last.items():
                cur = seen.get(pid)
                if cur is None or cur[2] != start:      # exited (or pid reused)
                    self.dead[g] += cpu
            self.last = seen
            for g in self.groups:
                totals[g] += self.dead[g]
        return totals, nproc, rss

    def totals(self, max_age=0.2):
        with self.lock:
            c = self.cache
        if c is not None and time.time() - c[0] < max_age:
            return c[1]
        t = time.time()
        totals, _, _ = self.scan()
        with self.lock:
            self.cache = (t, totals)
        return totals


def pairs(items, what):
    out = {}
    for s in items or []:
        if "=" not in s:
            raise SystemExit(f"{what} wants name=value, got {s!r}")
        k, v = s.split("=", 1)
        out[k] = v
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", action="append", required=True, help="name=<cpu list>|all|node:N")
    ap.add_argument("--out", help="CSV path; none = HTTP only")
    ap.add_argument("--port", type=int, default=9010)
    ap.add_argument("--bind", default="127.0.0.1", help="0.0.0.0 when the runner is on another machine")
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--proc-interval", type=float, default=1.0)
    ap.add_argument("--disk", default="", help="block device in /proc/diskstats, e.g. sdc or nvme2c2n1")
    ap.add_argument("--numa-node", type=int, help="memory rows from this node's meminfo (else /proc/meminfo)")
    ap.add_argument("--run-dir", help="the single-host Neon deployment's run dir (pid files)")
    ap.add_argument("--group-pidfile", action="append", help="name=PATH")
    ap.add_argument("--group-comm", action="append", help="name=comm1,comm2")
    args = ap.parse_args()
    online = cpu_times()
    sets = {}
    for name, spec in pairs(args.set, "--set").items():
        cpus = [c for c in parse_cpus(spec) if c in online]
        if not cpus:
            raise SystemExit(f"set {name}: no online CPU in {spec!r}")
        sets[name] = cpus
    comms = {g: [c for c in v.split(",") if c] for g, v in pairs(args.group_comm, "--group-comm").items()}
    scanner = ProcScanner(args.run_dir, pairs(args.group_pidfile, "--group-pidfile"), comms)
    scan_lock = threading.Lock()
    groups, server_groups = scanner.groups, scanner.server

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            q = parse_qs(urlparse(self.path).query)
            name = (q.get("set") or [""])[0]
            group = (q.get("group") or [""])[0]
            t = time.time()
            if name in sets:
                cpus = sets[name]
                ct = cpu_times()
                obj = {"t": t, "ncpu": len(cpus), "node_busy_s": sum(ct[c][0] for c in cpus),
                       "node_iowait_s": sum(ct[c][4] for c in cpus)}
                if name == "server":
                    if server_groups:
                        with scan_lock:
                            totals = scanner.totals()
                        obj["proc_s"] = sum(totals[g] for g in server_groups)
                    obj.update(disk_counters(args.disk))
            elif group in groups or group in ("server", "neon"):
                with scan_lock:
                    totals = scanner.totals()
                obj = {"t": t, "ncpu": len(sets.get("server", [])),
                       "proc_s": totals[group] if group in groups else sum(totals[g] for g in server_groups)}
            else:
                self.send_error(404)
                return
            body = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    ThreadingHTTPServer.allow_reuse_address = True
    srv = ThreadingHTTPServer((args.bind, args.port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"cpu_agent on {args.bind}:{args.port}: sets {', '.join(f'{k}({len(v)})' for k, v in sets.items())}; "
          f"groups {', '.join(groups) or 'none'}", flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    def proc_writer():
        path = args.out[:-4] + ".procs.csv" if args.out.endswith(".csv") else args.out + ".procs.csv"
        with open(path, "w") as f:
            f.write("t,group,nproc,cores,rss_mb\n")
            with scan_lock:
                prev, _, _ = scanner.scan()
            t0, d0, s0 = time.time(), disk_counters(args.disk), cpu_times()
            server = sets.get("server", [])
            while not stop.wait(args.proc_interval):
                with scan_lock:
                    cur, nproc, rss = scanner.scan(want_rss=True)
                t1, d1, s1 = time.time(), disk_counters(args.disk), cpu_times()
                dt = t1 - t0
                gsum = 0.0
                for g in groups:
                    c = (cur[g] - prev[g]) / dt
                    gsum += c
                    f.write(f"{t1:.3f},{g},{nproc[g]},{c:.3f},{rss[g] / 2**20:.0f}\n")
                if groups:
                    sg = server_groups
                    f.write(f"{t1:.3f},server,{sum(nproc[g] for g in sg)},"
                            f"{sum(cur[g] - prev[g] for g in sg) / dt:.3f},{sum(rss[g] for g in sg) / 2**20:.0f}\n")
                if server:
                    node_busy = sum(s1[c][0] - s0[c][0] for c in server) / dt
                    f.write(f"{t1:.3f},other_server,,{node_busy - gsum:.3f},\n")
                if d0 and d1:
                    f.write(f"{t1:.3f},disk_busy,,{(d1['disk_busy_s'] - d0['disk_busy_s']) / dt:.3f},\n"
                            f"{t1:.3f},disk_read_mbs,,{(d1['disk_read_bytes'] - d0['disk_read_bytes']) / dt / 2**20:.2f},\n"
                            f"{t1:.3f},disk_write_mbs,,{(d1['disk_write_bytes'] - d0['disk_write_bytes']) / dt / 2**20:.2f},\n"
                            f"{t1:.3f},disk_iops,,{(d1['disk_reads'] + d1['disk_writes'] - d0['disk_reads'] - d0['disk_writes']) / dt:.0f},\n")
                mi = meminfo(args.numa_node)
                for k in ("MemFree", "FilePages", "AnonPages", "Shmem"):
                    f.write(f"{t1:.3f},mem_{k},,{mi.get(k, 0):.0f},\n")
                # available (estimate): free + file-backed page cache, which the kernel reclaims under pressure
                avail = mi.get("MemFree", 0) + mi.get("Active(file)", 0) + mi.get("Inactive(file)", 0)
                f.write(f"{t1:.3f},mem_Available,,{avail:.0f},\n")
                f.flush()
                prev, t0, d0, s0 = cur, t1, d1, s1

    def scan_loop():
        # Service mode: scan anyway, so a process living between two requests still lands in its group's total.
        while not stop.wait(args.proc_interval):
            with scan_lock:
                scanner.totals(max_age=0)

    if not args.out:
        if groups:
            threading.Thread(target=scan_loop, daemon=True).start()
        stop.wait()
        srv.shutdown()
        return

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    pw = threading.Thread(target=proc_writer, daemon=True)
    pw.start()
    with open(args.out, "w") as f:
        f.write("t,set,ncpu,busy,usr,sys,softirq,iowait,proc_cores,per_core\n")
        def server_proc():
            if not server_groups:
                return None
            with scan_lock:
                totals = scanner.totals(max_age=args.interval / 2)
            return sum(totals[g] for g in server_groups)

        t0, c0, p0 = time.time(), cpu_times(), server_proc()
        while not stop.wait(args.interval):
            t1, c1, p1 = time.time(), cpu_times(), server_proc()
            dt = t1 - t0
            proc = f"{(p1 - p0) / dt:.3f}" if p0 is not None and p1 is not None else ""
            for name, cpus in sets.items():
                d = {c: [b - a for a, b in zip(c0[c], c1[c])] for c in cpus}
                tot = [sum(d[c][i] for c in cpus) / dt for i in range(5)]
                per = "|".join(f"{c}:{d[c][0] / d[c][5]:.2f}" if d[c][5] > 0 else f"{c}:" for c in cpus)
                f.write(f"{t1:.3f},{name},{len(cpus)},{tot[0]:.3f},{tot[1]:.3f},{tot[2]:.3f},"
                        f"{tot[3]:.3f},{tot[4]:.3f},{proc if name == 'server' else ''},{per}\n")
            f.flush()
            t0, c0, p0 = t1, c1, p1
    srv.shutdown()
    pw.join(3)


if __name__ == "__main__":
    main()
