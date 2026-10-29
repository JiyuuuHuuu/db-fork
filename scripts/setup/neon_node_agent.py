#!/usr/bin/env python3
"""Neon node agent (stdlib only, Python >= 3.10): started by scripts/setup/neon.py on every Neon node of a
deployment. Experiment 5's agent (multi-node, TCP) and experiment 11's (one host, Unix sockets) in one file;
every path, port and address comes from the command line.

Every node:
  GET  /cpu?proc=<comm,...>&disk=<dev>   CPU seconds of the named processes, node busy/iowait seconds, disk counters
  GET  /time                             {"t": time.time()} (clock-offset check)
  GET  /stats                            computes, their RSS, per-compute PSS (sampled), memory, disk free
  POST /sampler {"max_computes": n}      computes queried per sampler tick (-1: all, 0: none)
  Sampler: every --sample-interval seconds one JSON line in <archive>/samples.jsonl: node memory, NIC and
  compute-root counters, a filtered copy of each --services /metrics endpoint, and (round-robin) the running
  computes' neon_perf_counters, LFC stats and backpressure time.

Compute nodes (--computes), one Neon compute (compute_ctl + Postgres 17) per timeline:
  POST   /computes {"timeline_id", "tenant_id", "shared_buffers"?, "lfc_mb"?,
                    "pageservers"? [host:port per shard, in shard order], "stripe_size"? (pages, sharded only),
                    "statement_log"? (bool), "pgdata_on_disk"? (bool: data directory on --disk-compute-root,
                    only the LFC file on --compute-root)}
         start the timeline's compute unless it runs; wait until it accepts queries;
         return {"name", "host", "port", "start_ms", "started"} (host: a socket directory under uds).
         compute_ctl's /metrics.json (startup phases) goes to <archive>/starts.jsonl; with statement logging
         on, the compute's log (Postgres' per-statement durations) is kept in <archive>/pg/ when it stops.
  GET    /computes               list
  DELETE /computes/<timeline_id> stop one;  DELETE /computes  stop all
  POST   /computes/<timeline_id>/sql {"sql", "db"?}  run SQL as cloud_admin (seeding, perf counters)

Transport (--transport):
  tcp  computes listen on --compute-listen (default all interfaces), port --pg-port-base + slot; this agent on
       --listen host:port; the pageserver and safekeepers are host:port.
  uds  (experiment 10/11, one host) every connection of a compute is a Unix socket: Postgres listens only on
       <sock-dir>/c<slot>/.s.PGSQL.<port> (listen_addresses=''), compute_ctl serves its HTTP APIs on
       <sock-dir>/c<slot>/{ext,int}.sock, this agent on --listen unix:<path>; --pageserver and --safekeepers
       name a socket directory and a port (<dir>:<port>, libpq naming).
The agent runs under the node's server placement (taskset/numactl); compute_ctl and Postgres inherit it.
"""
import argparse
import json
import os
import re
import shutil
import signal
import socket
import socketserver
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ARGS = None
TEMPLATE = None
LOCK = threading.Lock()
COMPUTES = {}          # timeline_id -> dict(slot, port, proc, dir, lfc_dir, sockdir, state, event, start_ms, ...)
START_SEM = None
ARCHIVE = None
ARCHIVE_LOCK = threading.Lock()
SAMPLE_MAX = [0]
SAMPLE_RR = [0]        # round-robin position over the running computes
HZ = os.sysconf("SC_CLK_TCK")
PAGE = os.sysconf("SC_PAGE_SIZE")


def archive_line(name, obj):
    with ARCHIVE_LOCK, open(os.path.join(ARCHIVE, name), "a") as f:
        f.write(json.dumps(obj) + "\n")


def uds():
    return ARGS.transport == "uds"


# ---------------------------------------------------------------- HTTP client (TCP or Unix socket)
class UnixHTTPConnection(HTTPConnection):
    def __init__(self, path, timeout=10):
        super().__init__("localhost", timeout=timeout)
        self.path_ = path

    def connect(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect(self.path_)
        self.sock = s


def fetch(url, timeout=5):
    """GET an http:// or http+unix://<hex path>/... URL; returns the body text."""
    if url.startswith("http+unix://"):
        host, _, path = url[len("http+unix://"):].partition("/")
        conn = UnixHTTPConnection(bytes.fromhex(host).decode(), timeout)
        try:
            conn.request("GET", "/" + path)
            return conn.getresponse().read().decode()
        finally:
            conn.close()
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.read().decode()


# ---------------------------------------------------------------- CPU / disk
def cpu_sample(procs, disk):
    want = set(p for p in procs if p)
    proc_ticks = 0
    if want:
        for pid in os.listdir("/proc"):
            if not pid.isdigit():
                continue
            try:
                with open(f"/proc/{pid}/stat") as f:
                    s = f.read()
            except OSError:
                continue
            comm = s[s.index("(") + 1:s.rindex(")")]
            if comm in want:
                fields = s[s.rindex(")") + 2:].split()
                proc_ticks += int(fields[11]) + int(fields[12])
    with open("/proc/stat") as f:
        cpu = [int(x) for x in f.readline().split()[1:]]
    total = sum(cpu[:8])
    idle, iowait = cpu[3], cpu[4]
    out = {"t": time.time(), "proc_s": proc_ticks / HZ, "node_busy_s": (total - idle - iowait) / HZ,
           "node_iowait_s": iowait / HZ, "ncpu": os.cpu_count()}
    if disk:
        with open("/proc/diskstats") as f:
            for line in f:
                p = line.split()
                if p[2] == disk:
                    out["disk_busy_s"] = int(p[12]) / 1000.0
                    out["disk_reads"] = int(p[3])
                    out["disk_read_bytes"] = int(p[5]) * 512
                    out["disk_writes"] = int(p[7])
                    out["disk_write_bytes"] = int(p[9]) * 512
    return out


# ---------------------------------------------------------------- computes
def ports(slot):
    """Postgres port, compute_ctl external and internal HTTP ports of a slot."""
    return ARGS.pg_port_base + slot, ARGS.http_port_base + 2 * slot, ARGS.http_port_base + 1 + 2 * slot


def free_slot():
    used = {c["slot"] for c in COMPUTES.values()}
    s = 0
    while s in used:
        s += 1
    return s


def pg_host_of(c):
    return c["sockdir"] if uds() else ARGS.host


def libpq_url(addr, user="no_user"):
    """postgres:// URL of host:port; a host that is a directory (a Unix-socket directory) is percent-encoded."""
    host, port = addr.rsplit(":", 1)
    if host.startswith("/"):
        host = urllib.parse.quote(host, safe="")
    return f"postgres://{user}@{host}:{port}"


def render_config(tenant_id, timeline_id, port, shared_buffers, lfc_mb, compute_dir, sockdir=None, pageservers=None,
                  stripe_size=None, statement_log=None, lfc_dir=None):
    cfg = json.loads(json.dumps(TEMPLATE))
    spec = cfg["spec"]
    spec["tenant_id"] = tenant_id
    spec["timeline_id"] = timeline_id
    spec["endpoint_id"] = timeline_id
    spec["cluster"]["name"] = timeline_id
    # One pageserver (unsharded tenant, shard index "0000") or one per shard, in shard order
    # (shard index = number and count in hex, e.g. "0102" is shard 1 of 2).
    pss = [libpq_url(a) for a in (pageservers or [ARGS.pageserver])]
    info = spec["pageserver_connection_info"]
    info["prefer_protocol"] = "libpq"
    k = len(pss)
    if k == 1:
        info.update(shard_count=0, stripe_size=None,
                    shards={"0000": {"pageservers": [{"id": None, "libpq_url": pss[0], "grpc_url": None}]}})
        spec["shard_stripe_size"] = None
    else:
        if not stripe_size:
            raise ValueError(f"{k} pageserver shards need a stripe_size")
        info.update(shard_count=k, stripe_size=int(stripe_size),
                    shards={f"{i:02x}{k:02x}": {"pageservers": [{"id": None, "libpq_url": u, "grpc_url": None}]}
                            for i, u in enumerate(pss)})
        spec["shard_stripe_size"] = int(stripe_size)
    spec["pageserver_connstring"] = ",".join(pss)
    spec["safekeepers_generation"] = None     # walproposer then creates the timeline on the safekeeper
    spec["safekeeper_connstrings"] = ARGS.safekeepers.split(",")
    spec["mode"] = "Primary"
    spec["endpoint_storage_addr"] = None
    spec["endpoint_storage_token"] = None
    conf = spec["cluster"]["postgresql_conf"]
    if uds():
        conf = re.sub(r"listen_addresses='[^']*'", "listen_addresses=''", conf)
        conf += f"unix_socket_directories='{sockdir}'\n"
    else:
        conf = re.sub(r"listen_addresses='[^']*'", f"listen_addresses='{ARGS.compute_listen}'", conf)
        conf += "unix_socket_directories=''\n"
    conf = re.sub(r"port=\d+", f"port={port}", conf)
    conf = re.sub(r"shared_buffers=\S+", f"shared_buffers={shared_buffers}", conf)
    conf = re.sub(r"neon\.safekeepers='[^']*'", f"neon.safekeepers='{ARGS.safekeepers}'", conf)
    conf = re.sub(r"effective_io_concurrency=\d+", f"effective_io_concurrency={ARGS.eic}", conf)
    conf = re.sub(r"neon\.max_file_cache_size=\S+", f"neon.max_file_cache_size={lfc_mb}MB", conf)
    conf = re.sub(r"neon\.file_cache_size_limit=\S+", f"neon.file_cache_size_limit={lfc_mb}MB", conf)
    conf = re.sub(r"neon\.file_cache_chunk_size=\d+", f"neon.file_cache_chunk_size={ARGS.lfc_chunk}", conf)
    conf = re.sub(r"neon\.file_cache_path='[^']*'", f"neon.file_cache_path='{lfc_dir or compute_dir}/file.cache'",
                  conf)
    log_stmts = (not ARGS.no_statement_log) if statement_log is None else bool(statement_log)
    if not log_stmts:   # the seeding compute (bulk INSERTs) and the scaling runs (1000s of computes)
        conf = re.sub(r"log_min_duration_statement=\S+", "log_min_duration_statement=-1", conf)
    spec["cluster"]["postgresql_conf"] = conf
    return cfg


def pg_env():
    return dict(os.environ, LD_LIBRARY_PATH=f"{ARGS.neon_bin}/pg_install/v17/lib")


def psql(c, sql, timeout=30, db="postgres"):
    host = c["sockdir"] if uds() else "127.0.0.1"
    return subprocess.run([f"{ARGS.neon_bin}/pg_install/v17/bin/psql", "-h", host, "-p", str(c["port"]),
                           "-U", "cloud_admin", "-d", db, "-XAtq", "-v", "ON_ERROR_STOP=1",
                           "-c", sql], capture_output=True, text=True, timeout=timeout, env=pg_env())


def start_compute(c, tenant_id, timeline_id, shared_buffers, lfc_mb, **spec_opts):
    port, ext, internal = c["port"], *ports(c["slot"])[1:]
    d = c["dir"]
    for x in filter(None, (d, c.get("lfc_dir"), c.get("sockdir"))):
        shutil.rmtree(x, ignore_errors=True)
        os.makedirs(x)
    if c.get("lfc_dir"):
        spec_opts["lfc_dir"] = c["lfc_dir"]
    with open(f"{d}/config.json", "w") as f:
        json.dump(render_config(tenant_id, timeline_id, port, shared_buffers, lfc_mb, d, c.get("sockdir"),
                                **spec_opts), f)
    # INSTANCE_ID makes compute_ctl serve /metrics.json without a JWT (dev setup).
    env = dict(pg_env(), TOKIO_WORKER_THREADS="2", RUST_LOG="warn", INSTANCE_ID=timeline_id)
    if uds():
        connstr = libpq_url(f"{c['sockdir']}:{port}", "cloud_admin") + "/postgres"
        http_args = ["--external-http-socket", f"{c['sockdir']}/ext.sock",
                     "--internal-http-socket", f"{c['sockdir']}/int.sock"]
    else:
        connstr = f"postgres://cloud_admin@127.0.0.1:{port}/postgres"
        http_args = []
    c["ext_port"] = ext
    log = open(f"{d}/compute.log", "w")
    t0 = time.time()
    c["proc"] = subprocess.Popen(
        [f"{ARGS.neon_bin}/compute_ctl", "--external-http-port", str(ext), "--internal-http-port", str(internal),
         *http_args, "--pgdata", f"{d}/pgdata", "--connstr", connstr,
         "--config", f"{d}/config.json", "--pgbin", f"{ARGS.neon_bin}/pg_install/v17/bin/postgres",
         "--compute-id", timeline_id, "--dev"],
        stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)
    deadline = t0 + ARGS.start_timeout
    while time.time() < deadline:
        if c["proc"].poll() is not None:
            raise RuntimeError(f"compute_ctl exited with {c['proc'].returncode}; see {d}/compute.log")
        try:
            r = psql(c, "select 1", timeout=10)
            if r.returncode == 0:
                c["t0"], c["t_ready"] = t0, time.time()
                return (c["t_ready"] - t0) * 1000
        except subprocess.TimeoutExpired:
            pass
        time.sleep(0.02)
    raise TimeoutError(f"compute for {timeline_id} not ready after {ARGS.start_timeout}s")


def stop_compute(c):
    p = c.get("proc")
    if p is not None and p.poll() is None:
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(p.pid, signal.SIGKILL)   # postgres children share the session
        except ProcessLookupError:
            pass
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    kill_by_cwd(c["dir"])
    keep_log(c)
    for x in filter(None, (c["dir"], c.get("lfc_dir"), c.get("sockdir"))):
        shutil.rmtree(x, ignore_errors=True)


def kill_by_cwd(root):
    """SIGKILL this user's processes working under root. Postgres makes every child its own process-group leader
    (InitPostmasterChild calls setsid), so killpg of compute_ctl's group misses a child that hangs in shutdown:
    a checkpointer outlived its postmaster this way (scripts/setup smoke run, 29 Oct 2026)."""
    root = os.path.realpath(root) + "/"
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            if os.stat(f"/proc/{pid}").st_uid != os.getuid():
                continue
            cwd = os.readlink(f"/proc/{pid}/cwd")
        except OSError:
            continue
        if (cwd.removesuffix(" (deleted)") + "/").startswith(root):
            try:
                os.kill(int(pid), signal.SIGKILL)
            except ProcessLookupError:
                pass


def keep_log(c, sub="pg"):
    """Copy the compute's log (compute_ctl output with Postgres' stderr) to the archive: always for a failed
    start, for a stopped compute only with statement logging on (otherwise it holds nothing measured, and
    thousands of computes would fill the archive)."""
    src = f"{c['dir']}/compute.log"
    if not os.path.exists(src) or (sub == "pg" and not c.get("statement_log")):
        return
    os.makedirs(os.path.join(ARCHIVE, sub), exist_ok=True)
    tl = os.path.basename(c["dir"])
    try:
        shutil.copy(src, os.path.join(ARCHIVE, sub, f"{tl}_{time.time():.3f}.log"))
    except OSError:
        pass


def compute_metrics_url(c):
    if uds():
        return f"http+unix://{(c['sockdir'] + '/ext.sock').encode().hex()}/metrics.json"
    return f"http://127.0.0.1:{c['ext_port']}/metrics.json"


def record_start(tl, c):
    """compute_ctl's startup phases (GET /metrics.json) next to the agent's own start time; runs after the
    reply so it never adds to a timed start."""
    m = None
    for _ in range(20):
        try:
            m = json.loads(fetch(compute_metrics_url(c)))
            if m.get("total_startup_ms"):
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.2)
    archive_line("starts.jsonl", {"timeline": tl, "t0": c.get("t0"), "t_ready": c.get("t_ready"),
                                  "start_ms": c.get("start_ms"), "slot": c["slot"], "metrics": m})


def disk_free_gb(path):
    try:
        st = os.statvfs(path)
    except OSError:
        return None
    return st.f_bavail * st.f_frsize / 2**30


def ensure_compute(body):
    tl = body["timeline_id"]
    with LOCK:
        c = COMPUTES.get(tl)
        if c is None:
            free = disk_free_gb(ARGS.compute_root)
            if ARGS.disk_guard_gb and free is not None and free < ARGS.disk_guard_gb:
                raise RuntimeError(f"less than {ARGS.disk_guard_gb} GB free on {ARGS.compute_root}")
            slot = free_slot()
            # "pgdata_on_disk": the compute's data directory (and its 16 MB WAL segment) on --disk-compute-root,
            # only the LFC file on --compute-root (the tmpfs): fits more computes per node
            on_disk = bool(body.get("pgdata_on_disk")) and bool(ARGS.disk_compute_root)
            c = {"slot": slot, "port": ports(slot)[0],
                 "dir": f"{ARGS.disk_compute_root if on_disk else ARGS.compute_root}/{tl}",
                 "lfc_dir": f"{ARGS.compute_root}/{tl}" if on_disk else None,
                 "sockdir": f"{ARGS.sock_dir}/c{slot}" if uds() else None, "state": "starting",
                 "event": threading.Event(), "start_ms": None, "error": None, "proc": None,
                 "lfc_mb": int(body.get("lfc_mb", ARGS.lfc_mb)),
                 "statement_log": body.get("statement_log", not ARGS.no_statement_log)}
            COMPUTES[tl] = c
            owner = True
        else:
            owner = False
    if not owner:
        c["event"].wait(ARGS.start_timeout + 30)
        if c["state"] != "running":
            raise RuntimeError(c["error"] or "compute not running")
        return {"name": tl, "host": pg_host_of(c), "port": c["port"], "start_ms": c["start_ms"], "started": False}
    try:
        with START_SEM:
            c["start_ms"] = start_compute(c, body["tenant_id"], tl, body.get("shared_buffers") or ARGS.shared_buffers,
                                          c["lfc_mb"], pageservers=body.get("pageservers"),
                                          stripe_size=body.get("stripe_size"),
                                          statement_log=body.get("statement_log"))
        c["state"] = "running"
        threading.Thread(target=record_start, args=(tl, c), daemon=True).start()
    except Exception as e:  # noqa: BLE001
        c["state"] = "failed"
        c["error"] = str(e)
        keep_log(c, "failed_starts")
        stop_compute(c)
        with LOCK:
            COMPUTES.pop(tl, None)
        raise
    finally:
        c["event"].set()
    return {"name": tl, "host": pg_host_of(c), "port": c["port"], "start_ms": c["start_ms"], "started": True}


def delete_compute(tl):
    with LOCK:
        c = COMPUTES.pop(tl, None)
    if c is None:
        return False
    stop_compute(c)
    return True


def clean_roots():
    for root in filter(None, (ARGS.compute_root, ARGS.disk_compute_root)):
        shutil.rmtree(root, ignore_errors=True)
        os.makedirs(root, exist_ok=True)
    if uds() and os.path.isdir(ARGS.sock_dir):
        for name in os.listdir(ARGS.sock_dir):
            if re.fullmatch(r"c\d+", name):
                shutil.rmtree(os.path.join(ARGS.sock_dir, name), ignore_errors=True)


def delete_all():
    with LOCK:
        names = list(COMPUTES)
    with ThreadPoolExecutor(64) as ex:
        list(ex.map(delete_compute, names))
    clean_roots()
    remove_orphan_shm()
    return len(names)


def remove_orphan_shm():
    """Postgres keeps a small SysV shared-memory segment per postmaster; a compute that is killed leaves it
    behind, and after a few thousand the kernel's segment ids (kernel.shmmni) run out and no compute can start.
    Remove this user's segments that no process has attached."""
    try:
        out = subprocess.run(["ipcs", "-m"], capture_output=True, text=True).stdout
    except OSError:
        return
    me = os.environ.get("USER") or str(os.getuid())
    for line in out.splitlines():
        p = line.split()
        if len(p) >= 6 and p[1].isdigit() and p[2] in (me, str(os.getuid())) and p[5] == "0":
            subprocess.run(["ipcrm", "-m", p[1]], capture_output=True)


# ---------------------------------------------------------------- memory
def meminfo():
    with open("/proc/meminfo") as f:
        return {l.split(":")[0]: int(l.split()[1]) for l in f}     # kB


def node_meminfo(node):
    out = {}
    try:
        with open(f"/sys/devices/system/node/node{node}/meminfo") as f:
            for line in f:
                p = line.split()
                out[p[2].rstrip(":")] = int(p[3])     # kB
    except OSError:
        pass
    return out


def proc_children():
    """{ppid: [pid, ...]} of every process."""
    kids = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/stat") as f:
                s = f.read()
        except OSError:
            continue
        kids.setdefault(int(s[s.rindex(")") + 2:].split()[1]), []).append(int(pid))
    return kids


def descendants(kids, root):
    """root and every process below it (Postgres leaves compute_ctl's session, so walk the tree)."""
    out, todo = [], [root]
    while todo:
        p = todo.pop()
        out.append(p)
        todo.extend(kids.get(p, []))
    return out


def pss_kb(pid):
    try:
        with open(f"/proc/{pid}/smaps_rollup") as f:
            for line in f:
                if line.startswith("Pss:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


def numa_mem(prefix):
    if ARGS.numa_node < 0:
        return {}
    mi = node_meminfo(ARGS.numa_node)
    return {f"{prefix}memfree_mb": mi.get("MemFree", 0) / 1024, f"{prefix}filepages_mb": mi.get("FilePages", 0) / 1024,
            f"{prefix}anon_mb": mi.get("AnonPages", 0) / 1024, f"{prefix}shmem_mb": mi.get("Shmem", 0) / 1024}


def stats():
    with LOCK:
        running = [(tl, c["proc"].pid) for tl, c in COMPUTES.items() if c["state"] == "running" and c.get("proc")]
    kids = proc_children()
    trees = [descendants(kids, pid) for _, pid in running]
    rss = 0
    nproc = 0
    for pids in trees:
        for p in pids:
            try:
                with open(f"/proc/{p}/statm") as f:
                    rss += int(f.read().split()[1]) * PAGE
                nproc += 1
            except OSError:
                pass
    # PSS of a sample of computes (compute_ctl and every process below it)
    pss = [sum(pss_kb(p) for p in tree) / 1024 for tree in trees[:16]]
    mem = meminfo()
    return {"computes": len(COMPUTES), "running": len(running), "processes": nproc, "rss_mb": rss / 2**20,
            "pss_mb_per_compute": (sum(pss) / len(pss)) if pss else None, "pss_sampled": len(pss),
            "mem_available_mb": mem["MemAvailable"] / 1024, "mem_shmem_mb": mem.get("Shmem", 0) / 1024,
            **numa_mem(f"node{ARGS.numa_node}_"), "disk_free_gb": disk_free_gb(ARGS.compute_root)}


# ---------------------------------------------------------------- sampler
COMPUTE_SQL = ("SELECT 'pc', metric || coalesce('@' || bucket_le::text, ''), value FROM neon_perf_counters "
               "UNION ALL SELECT 'bp', 'backpressure_throttling_us', backpressure_throttling_time()")
LFC_SQL = " UNION ALL SELECT 'lfc', stat_name, count FROM neon_get_lfc_stats() AS t(stat_name text, count bigint)"
SERVICE_KEEP = re.compile(
    r"^(pageserver_smgr_query_seconds_global_(sum|count)\{[^}]*get_page_at_lsn"
    r"|pageserver_wal_redo_seconds_(sum|count)"
    r"|pageserver_layers_per_read_global_(sum|count)"
    r"|pageserver_wait_lsn_seconds_(sum|count)"
    r"|pageserver_page_service_pagestream_response_flush_in_progress_micros"
    r"|safekeeper_(flush|write)_wal_seconds_(sum|count)"
    r"|safekeeper_pg_io_bytes_total"
    r"|storage_controller_http_request_latency_(sum|count)"
    r"|storage_controller_pageserver_request_latency_(sum|count)"
    r"|storage_controller_http_request_status_total)")


def node_counters():
    """Memory, the compute root and every NIC's byte counters (cumulative)."""
    mem = meminfo()
    out = {"mem_available_mb": mem["MemAvailable"] / 1024, "mem_shmem_mb": mem.get("Shmem", 0) / 1024,
           **numa_mem(""), "computes_running": len(COMPUTES), "disk_free_gb": disk_free_gb(ARGS.compute_root)}
    try:
        st = os.statvfs(ARGS.compute_root)
        out["compute_root_used_mb"] = (st.f_blocks - st.f_bfree) * st.f_frsize / 2**20
    except OSError:
        pass
    with open("/proc/net/dev") as f:
        for line in list(f)[2:]:
            name, rest = line.split(":", 1)
            name, v = name.strip(), rest.split()
            if name != "lo":
                out[f"nic:{name}:rx_bytes"] = int(v[0])
                out[f"nic:{name}:tx_bytes"] = int(v[8])
    return out


def sample_once(services):
    t_a = time.time()
    out = {"computes": {}, "services": {}, "node": node_counters()}
    n = SAMPLE_MAX[0]
    if ARGS.computes and n != 0:
        with LOCK:
            running = sorted((tl, c) for tl, c in COMPUTES.items() if c["state"] == "running")
        if 0 < n < len(running):
            # Many computes: sample a rotating subset per tick (one psql each costs CPU on the node)
            i = SAMPLE_RR[0] % len(running)
            running = (running[i:] + running[:i])[:n]
            SAMPLE_RR[0] = i + n
        for tl, c in running:
            try:
                r = psql(c, COMPUTE_SQL + (LFC_SQL if c.get("lfc_mb") else ""), timeout=5)
            except subprocess.TimeoutExpired:
                continue
            if r.returncode != 0:
                out["computes"][tl] = {"error": r.stderr.strip()[:200]}
                continue
            vals = {}
            for line in r.stdout.splitlines():
                parts = line.split("|")
                if len(parts) == 3:
                    try:
                        vals[f"{parts[0]}:{parts[1]}"] = float(parts[2])
                    except ValueError:
                        pass
            out["computes"][tl] = vals
    for label, url in services:
        try:
            text = fetch(url)
        except Exception as e:  # noqa: BLE001
            out["services"][label] = {"error": str(e)[:200]}
            continue
        vals = {}
        for line in text.splitlines():
            if SERVICE_KEEP.match(line):
                name, _, value = line.rpartition(" ")
                try:
                    vals[name] = float(value)
                except ValueError:
                    pass
        out["services"][label] = vals
    out["t"] = (t_a + time.time()) / 2
    out["t_a"] = t_a
    return out


def sampler(services):
    nxt = time.time()
    while True:
        nxt += ARGS.sample_interval
        try:
            archive_line("samples.jsonl", sample_once(services))
        except Exception as e:  # noqa: BLE001
            archive_line("sampler_errors.jsonl", {"t": time.time(), "error": str(e)})
        time.sleep(max(0.0, nxt - time.time()))


# ---------------------------------------------------------------- HTTP server
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    # Headers and body go out as separate writes; with Nagle on, the body waits for the client's delayed ACK of
    # the headers (40 ms) on kept-alive connections (experiment 4). TCP only: see UnixHandler.
    disable_nagle_algorithm = True

    def log_message(self, fmt, *args):
        pass

    def address_string(self):
        return "unix" if not self.client_address else str(self.client_address[0])

    def reply(self, code, obj):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def handle_any(self, method):
        u = urlparse(self.path)
        parts = [p for p in u.path.split("/") if p]
        q = parse_qs(u.query)
        try:
            if method == "GET" and parts == ["time"]:
                return self.reply(200, {"t": time.time()})
            if method == "GET" and parts == ["cpu"]:
                return self.reply(200, cpu_sample((q.get("proc", [""])[0]).split(","), q.get("disk", [""])[0]))
            if method == "GET" and parts == ["stats"]:
                return self.reply(200, stats())
            if method == "POST" and parts == ["sampler"]:
                SAMPLE_MAX[0] = int(self.body().get("max_computes", 0))
                return self.reply(200, {"max_computes": SAMPLE_MAX[0]})
            if not ARGS.computes:
                return self.reply(404, {"error": "not a compute node"})
            if method == "GET" and parts == ["computes"]:
                with LOCK:
                    return self.reply(200, [{"name": k, "host": pg_host_of(c), "port": c["port"],
                                             "state": c["state"], "start_ms": c["start_ms"],
                                             "pid": c["proc"].pid if c.get("proc") else None}
                                            for k, c in COMPUTES.items()])
            if method == "POST" and parts == ["computes"]:
                return self.reply(200, ensure_compute(self.body()))
            if method == "DELETE" and parts == ["computes"]:
                return self.reply(200, {"stopped": delete_all()})
            if method == "DELETE" and len(parts) == 2 and parts[0] == "computes":
                return self.reply(200 if delete_compute(parts[1]) else 404, {})
            if method == "POST" and len(parts) == 3 and parts[0] == "computes" and parts[2] == "sql":
                c = COMPUTES.get(parts[1])
                if c is None or c["state"] != "running":
                    return self.reply(404, {"error": "no running compute"})
                b = self.body()
                r = psql(c, b["sql"], timeout=7200, db=b.get("db", "postgres"))
                return self.reply(200 if r.returncode == 0 else 500, {"stdout": r.stdout, "stderr": r.stderr})
            return self.reply(404, {"error": f"no route {method} {u.path}"})
        except Exception as e:  # noqa: BLE001
            return self.reply(500, {"error": str(e)})

    def do_GET(self):
        self.handle_any("GET")

    def do_POST(self):
        self.handle_any("POST")

    def do_DELETE(self):
        self.handle_any("DELETE")


class UnixHandler(Handler):
    disable_nagle_algorithm = False     # TCP_NODELAY does not exist on a Unix socket


class UnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    request_queue_size = 1024


class TcpServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1024


def main():
    global ARGS, TEMPLATE, START_SEM, ARCHIVE
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listen", default="0.0.0.0:9000", help="host:port or unix:/path of this agent")
    ap.add_argument("--transport", choices=["tcp", "uds"], default="tcp")
    ap.add_argument("--archive-dir", required=True, help="samples.jsonl, starts.jsonl, pg/ (compute logs)")
    ap.add_argument("--sample-interval", type=float, default=1.0)
    ap.add_argument("--sample-max-computes", type=int, default=8,
                    help="computes queried per sampler tick, round-robin (-1: all, 0: none)")
    ap.add_argument("--services", default="", help="label=url,... of /metrics endpoints to sample")
    ap.add_argument("--numa-node", type=int, default=-1, help="report this NUMA node's memory too (-1: none)")
    g = ap.add_argument_group("computes")
    g.add_argument("--computes", action="store_true", help="serve /computes on this node")
    g.add_argument("--host", default="127.0.0.1", help="tcp: address clients use for this node's computes")
    g.add_argument("--compute-listen", default="0.0.0.0", help="tcp: listen_addresses of the computes")
    g.add_argument("--sock-dir", default="", help="uds: directory of the computes' socket directories c<slot>")
    g.add_argument("--neon-bin", default="", help="directory with compute_ctl and pg_install/v17")
    g.add_argument("--template", default="", help="compute_config_template.json")
    g.add_argument("--compute-root", default="", help="compute data directories (and LFC files)")
    g.add_argument("--disk-compute-root", default="", help="data directories of computes started with pgdata_on_disk")
    g.add_argument("--pageserver", default="", help="host:port (uds: socket dir:port) of the unsharded pageserver")
    g.add_argument("--safekeepers", default="", help="host:port,... (uds: socket dir:port)")
    g.add_argument("--shared-buffers", default="1MB")
    g.add_argument("--lfc-mb", type=int, default=0, help="local file cache size per compute (0: off)")
    g.add_argument("--lfc-chunk", type=int, default=128, help="LFC chunk size in pages (Neon's default 128)")
    g.add_argument("--eic", type=int, default=2, help="effective_io_concurrency")
    g.add_argument("--no-statement-log", action="store_true", help="do not log every statement's duration")
    g.add_argument("--max-concurrent-starts", type=int, default=32)
    g.add_argument("--start-timeout", type=float, default=300)
    g.add_argument("--disk-guard-gb", type=float, default=0, help="refuse a start below this free space (0: off)")
    g.add_argument("--pg-port-base", type=int, default=20000, help="Postgres port of slot k: base + k")
    g.add_argument("--http-port-base", type=int, default=22000,
                   help="compute_ctl ports of slot k (tcp): base + 2k (external), base + 2k + 1 (internal)")
    ARGS = ap.parse_args()
    if ARGS.computes:
        missing = [o for o in ("neon_bin", "template", "compute_root", "pageserver", "safekeepers")
                   if not getattr(ARGS, o)]
        if uds() and not ARGS.sock_dir:
            missing.append("sock_dir")
        if missing:
            ap.error("--computes needs " + ", ".join("--" + m.replace("_", "-") for m in missing))
    START_SEM = threading.Semaphore(ARGS.max_concurrent_starts)
    SAMPLE_MAX[0] = ARGS.sample_max_computes
    ARCHIVE = ARGS.archive_dir
    os.makedirs(ARCHIVE, exist_ok=True)
    if ARGS.computes:
        with open(ARGS.template) as f:
            TEMPLATE = json.load(f)
        clean_roots()
        remove_orphan_shm()
    services = [tuple(x.split("=", 1)) for x in ARGS.services.split(",") if x]
    threading.Thread(target=sampler, args=(services,), daemon=True).start()
    if ARGS.listen.startswith("unix:"):
        path = ARGS.listen[len("unix:"):]
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        UnixServer(path, UnixHandler).serve_forever()
    else:
        host, port = ARGS.listen.rsplit(":", 1)
        TcpServer((host, int(port)), Handler).serve_forever()


if __name__ == "__main__":
    main()
