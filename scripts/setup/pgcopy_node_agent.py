#!/usr/bin/env python3
"""pgcopy node agent (stdlib only, Python >= 3.10): one per node of a pgcopy deployment (experiment 13), started by
scripts/setup/pgcopy.py as the systemd unit pgcopy-agent. It runs PostgreSQL instances whose data directories are
copied from another instance with pg_basebackup (a branch), or initialised (the seed).

  POST   /instances {"name", "parent"?: {"host", "port", "name"?}, "init"?: bool, "start"?: bool (default true),
                     "shared_buffers"?, "statement_log"?: bool, "parent_role"?: bool}
         create the instance unless it exists: initdb (init) or pg_basebackup from parent (timed: copy_ms; bytes =
         size of the copied directory), write postgresql.auto.conf and pg_hba.conf, then (start) postgres and wait
         until it answers `select 1` (timed: start_ms). Reply {name, host, port, copy_ms, start_ms, bytes, started,
         running}. A second request for the same name waits for the first and returns its timings, started=false.
  POST   /instances/<name>/start          start a stopped/copied instance (idempotent)
  GET    /instances                       list;  GET /instances/<name>  one (404 if unknown)
  DELETE /instances/<name>                stop (immediate shutdown) and remove
  DELETE /instances                       every instance but the protected ones (--protect), then stale shm
  POST   /instances/<name>/sql {"sql", "db"?}   psql as the bench user
  GET    /stats                           instances, running, data_bytes, wal_bytes, memory, disk free
  GET    /time                            {"t": time.time()}

Archive (--archive-dir): forks.jsonl (one line per create: parent, t0, t_copied, t_ready, copy_ms, start_ms, bytes),
failed/ (the log of a failed copy or start), pg/ (an instance's log at delete when statement_log was on).

Addresses: the agent and every instance listen on --host (the node's LAN address) and 127.0.0.1 only, never on all
interfaces (the nodes' public interfaces face the internet). Copies connect to the parent's LAN address.
"""
import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

ARGS = None
LOCK = threading.Lock()
INSTANCES = {}         # name -> dict(slot, port, dir, log, state, event, proc, copy_ms, start_ms, bytes, ...)
COPY_SEM = None
START_SEM = None
ARCHIVE_LOCK = threading.Lock()
STATS_CACHE = {"t": 0.0, "v": None}
PW = "bench"
USER = "bench"


def archive_line(name, obj):
    with ARCHIVE_LOCK, open(os.path.join(ARGS.archive_dir, name), "a") as f:
        f.write(json.dumps(obj) + "\n")


def pg_env():
    return dict(os.environ, PGPASSWORD=PW, PGCONNECT_TIMEOUT="10")


def binp(name):
    return os.path.join(ARGS.pg_bin, name)


# ---------------------------------------------------------------- instances
def free_slot():
    used = {c["slot"] for c in INSTANCES.values()}
    s = 0
    while s in used:
        s += 1
    return s


def dir_bytes(path):
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.lstat(os.path.join(root, f)).st_size
            except OSError:
                pass
    return total


def write_conf(c, shared_buffers, statement_log, parent_role):
    """postgresql.auto.conf (the copy brings the parent's: replaced entirely) and pg_hba.conf."""
    d = c["dir"]
    lines = [
        f"port = {c['port']}",
        f"listen_addresses = '{ARGS.host},127.0.0.1'",
        "unix_socket_directories = ''",
        f"shared_buffers = '{shared_buffers}'",
        "huge_pages = off",
        "dynamic_shared_memory_type = posix",
        "max_parallel_workers_per_gather = 0",
        "logging_collector = off",
        f"log_min_duration_statement = {0 if statement_log else -1}",
        "log_line_prefix = '%m [%p] '",
    ]
    if parent_role:   # the seed and run roots: every branch copy is a replication connection to them, holding a
        # temporary replication slot (WAL is kept until each copy has streamed it: without slots, 16 concurrent
        # copies of one root forced enough segment switches that WAL a slow copy still needed was removed)
        lines += ["max_wal_senders = 96", "max_replication_slots = 96", "max_connections = 160",
                  "wal_keep_size = '512MB'"]
    else:
        lines += [f"max_wal_senders = {ARGS.host_wal_senders}", f"max_connections = {ARGS.host_max_connections}"]
    with open(f"{d}/postgresql.auto.conf", "w") as f:
        f.write("# written by pgcopy_node_agent.py\n" + "\n".join(lines) + "\n")
    with open(f"{d}/pg_hba.conf", "w") as f:
        f.write("local all all trust\n"
                f"host all {USER} 127.0.0.1/32 scram-sha-256\n"
                f"host all {USER} {ARGS.lan_cidr} scram-sha-256\n"
                f"host replication {USER} 127.0.0.1/32 scram-sha-256\n"
                f"host replication {USER} {ARGS.lan_cidr} scram-sha-256\n")


def initdb(c):
    pw = f"{c['dir']}.pw"
    with open(pw, "w") as f:
        f.write(PW + "\n")
    try:
        r = subprocess.run([binp("initdb"), "-D", c["dir"], "-U", USER, f"--pwfile={pw}", "--auth-local=trust",
                            "--auth-host=scram-sha-256", "-E", "UTF8", "--locale=C.UTF-8", "--no-sync"],
                           capture_output=True, text=True, timeout=300)
    finally:
        os.unlink(pw)
    if r.returncode != 0:
        raise RuntimeError(f"initdb failed: {r.stderr.strip()[-500:]}")


def copy_from(c, parent):
    os.makedirs(os.path.dirname(c["dir"]), exist_ok=True)
    r = subprocess.run([binp("pg_basebackup"), "-h", parent["host"], "-p", str(parent["port"]), "-U", USER,
                        "-D", c["dir"], "-Fp", "-X", "stream", "--checkpoint=fast", "--no-sync",
                        "--no-manifest", "-w"],
                       capture_output=True, text=True, timeout=ARGS.copy_timeout, env=pg_env())
    if r.returncode != 0:
        raise RuntimeError(f"pg_basebackup from {parent['host']}:{parent['port']} failed: "
                           f"{r.stderr.strip()[-500:]}")


def psql(c, sql, timeout=30, db="postgres"):
    return subprocess.run([binp("psql"), "-h", "127.0.0.1", "-p", str(c["port"]), "-U", USER, "-d", db,
                           "-XAtq", "-v", "ON_ERROR_STOP=1", "-c", sql],
                          capture_output=True, text=True, timeout=timeout, env=pg_env())


def start_pg(c):
    """Start postgres on the instance's directory; wait until `select 1` answers (20 ms polls). Returns ms."""
    logf = open(c["log"], "a")
    t0 = time.time()
    c["proc"] = subprocess.Popen([binp("postgres"), "-D", c["dir"]], stdout=logf, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, start_new_session=True, cwd=c["dir"])
    logf.close()
    deadline = t0 + ARGS.start_timeout
    while time.time() < deadline:
        if c["proc"].poll() is not None:
            raise RuntimeError(f"postgres exited with {c['proc'].returncode}; see {c['log']}")
        try:
            if psql(c, "select 1", timeout=10).returncode == 0:
                c["t_ready"] = time.time()
                c["running"] = True
                return (c["t_ready"] - t0) * 1000
        except subprocess.TimeoutExpired:
            pass
        time.sleep(0.02)
    raise TimeoutError(f"instance {c['name']} not ready after {ARGS.start_timeout}s")


def kill_by_cwd(root):
    """SIGKILL this user's processes working under root (Postgres children are their own session leaders)."""
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


def stop_pg(c, remove=True):
    p = c.get("proc")
    if p is not None and p.poll() is None:
        try:
            p.send_signal(signal.SIGQUIT)    # immediate shutdown: the directory is removed anyway
        except ProcessLookupError:
            pass
        try:
            p.wait(timeout=20)
        except subprocess.TimeoutExpired:
            pass
    if os.path.isdir(c["dir"]):
        kill_by_cwd(c["dir"])
    if p is not None:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass
    c["running"] = False
    if remove:
        if c.get("statement_log") and os.path.exists(c["log"]):
            keep_log(c, "pg")
        shutil.rmtree(c["dir"], ignore_errors=True)
        try:
            os.unlink(c["log"])
        except OSError:
            pass


def keep_log(c, sub):
    if not os.path.exists(c["log"]):
        return
    os.makedirs(os.path.join(ARGS.archive_dir, sub), exist_ok=True)
    try:
        shutil.copy(c["log"], os.path.join(ARGS.archive_dir, sub, f"{c['name']}_{time.time():.3f}.log"))
    except OSError:
        pass


def reply_of(c, started):
    return {"name": c["name"], "host": ARGS.host, "port": c["port"], "copy_ms": c.get("copy_ms"),
            "start_ms": c.get("start_ms"), "bytes": c.get("bytes"), "started": started,
            "running": bool(c.get("running")), "state": c["state"], "parent": c.get("parent")}


def new_entry(name, body):
    slot = free_slot()
    return {"name": name, "slot": slot, "port": ARGS.pg_port_base + slot, "dir": f"{ARGS.instance_root}/{name}",
            "log": f"{ARGS.instance_root}/{name}.log", "state": "creating", "event": threading.Event(),
            "proc": None, "running": False, "copy_ms": None, "start_ms": None, "bytes": None, "error": None,
            "parent": (body.get("parent") or {}).get("name"), "statement_log": bool(body.get("statement_log")),
            "shared_buffers": body.get("shared_buffers") or ARGS.shared_buffers,
            "parent_role": bool(body.get("parent_role"))}


def ensure_instance(body):
    name = body["name"]
    if not re.fullmatch(r"[A-Za-z0-9_]{1,63}", name):
        raise ValueError(f"bad instance name {name!r}")
    with LOCK:
        c = INSTANCES.get(name)
        owner = c is None
        if owner:
            free = disk_free_gb(ARGS.instance_root)
            if ARGS.disk_guard_gb and free is not None and free < ARGS.disk_guard_gb:
                raise RuntimeError(f"less than {ARGS.disk_guard_gb} GB free on {ARGS.instance_root}")
            c = new_entry(name, body)
            INSTANCES[name] = c
    if not owner:
        c["event"].wait(ARGS.copy_timeout + ARGS.start_timeout + 60)
        if c["state"] == "failed":
            raise RuntimeError(c["error"] or "instance failed")
        if body.get("start", True) and not c.get("running"):
            return start_instance(name)
        return reply_of(c, False)
    want_start = body.get("start", True)
    try:
        t0 = time.time()
        if body.get("init"):
            initdb(c)
        else:
            parent = body.get("parent")
            if not parent:
                raise ValueError("a copy needs 'parent' {host, port}")
            with COPY_SEM:
                t0 = time.time()
                copy_from(c, parent)
        t_copied = time.time()
        c["copy_ms"] = (t_copied - t0) * 1000
        c["bytes"] = dir_bytes(c["dir"])
        write_conf(c, c["shared_buffers"], c["statement_log"], c["parent_role"])
        c["state"] = "copied"
        if want_start:
            with START_SEM:
                c["start_ms"] = start_pg(c)
            c["state"] = "running"
        archive_line("forks.jsonl", {"name": name, "parent": c["parent"], "init": bool(body.get("init")),
                                     "t0": t0, "t_copied": t_copied, "t_ready": c.get("t_ready"),
                                     "copy_ms": c["copy_ms"], "start_ms": c["start_ms"], "bytes": c["bytes"],
                                     "slot": c["slot"], "parent_role": c["parent_role"]})
    except Exception as e:  # noqa: BLE001
        c["state"] = "failed"
        c["error"] = str(e)
        archive_line("failures.jsonl", {"name": name, "t": time.time(), "error": str(e)[:2000]})
        keep_log(c, "failed")
        stop_pg(c)
        with LOCK:
            INSTANCES.pop(name, None)
        raise
    finally:
        c["event"].set()
    return reply_of(c, want_start)


def start_instance(name):
    with LOCK:
        c = INSTANCES.get(name)
    if c is None:
        raise KeyError(name)
    with c.setdefault("start_lock", threading.Lock()):
        if c.get("running") and c["proc"] is not None and c["proc"].poll() is None:
            return reply_of(c, False)
        with START_SEM:
            c["start_ms"] = start_pg(c)
        c["state"] = "running"
        archive_line("starts.jsonl", {"name": name, "t_ready": c.get("t_ready"), "start_ms": c["start_ms"]})
    return reply_of(c, True)


def delete_instance(name):
    with LOCK:
        c = INSTANCES.pop(name, None)
    if c is None:
        return False
    stop_pg(c)
    return True


def delete_all():
    with LOCK:
        names = [n for n in INSTANCES if n not in ARGS.protect]
    with ThreadPoolExecutor(64) as ex:
        list(ex.map(delete_instance, names))
    clean_root(keep=set(INSTANCES))
    remove_orphan_shm()
    if ARGS.fstrim:
        trim()
    return len(names)


def trim():
    """sudo fstrim of the instance root's filesystem, so that every point starts with the SSD's freed blocks known
    to it. Without it the drive (no `discard` mount option) runs out of free blocks after hundreds of GB of copies
    and deletes, and its writes and fsyncs slow down (experiment 13: a fork's start went from 1.1 s to 3.3 s on the
    node that took every timed create)."""
    t0 = time.time()
    r = subprocess.run(["sudo", "-n", "fstrim", "-v", ARGS.instance_root], capture_output=True, text=True)
    archive_line("trims.jsonl", {"t0": t0, "seconds": round(time.time() - t0, 2), "rc": r.returncode,
                                 "out": (r.stdout or r.stderr).strip()[:300]})


def clean_root(keep=()):
    os.makedirs(ARGS.instance_root, exist_ok=True)
    for f in os.listdir(ARGS.instance_root):
        base = f[:-4] if f.endswith(".log") else f
        if base in keep:
            continue
        p = os.path.join(ARGS.instance_root, f)
        if os.path.isdir(p):
            kill_by_cwd(p)
            shutil.rmtree(p, ignore_errors=True)
        else:
            try:
                os.unlink(p)
            except OSError:
                pass


def remove_orphan_shm():
    """SysV segments of dead postmasters (no process attached) and POSIX DSM segments (/dev/shm/PostgreSQL.*) that no
    process of this user maps: killed instances leave both behind."""
    try:
        out = subprocess.run(["ipcs", "-m"], capture_output=True, text=True).stdout
        me = os.environ.get("USER") or str(os.getuid())
        for line in out.splitlines():
            p = line.split()
            if len(p) >= 6 and p[1].isdigit() and p[2] in (me, str(os.getuid())) and p[5] == "0":
                subprocess.run(["ipcrm", "-m", p[1]], capture_output=True)
    except OSError:
        pass
    try:
        segs = [f for f in os.listdir("/dev/shm") if f.startswith("PostgreSQL.")
                and os.stat(f"/dev/shm/{f}").st_uid == os.getuid()]
    except OSError:
        return
    if not segs:
        return
    used = set()
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            if os.stat(f"/proc/{pid}").st_uid != os.getuid():
                continue
            with open(f"/proc/{pid}/maps") as f:
                for line in f:
                    i = line.find("/dev/shm/PostgreSQL.")
                    if i >= 0:
                        used.add(line[i + len("/dev/shm/"):].split()[0])
            for fd in os.listdir(f"/proc/{pid}/fd"):
                try:
                    t = os.readlink(f"/proc/{pid}/fd/{fd}")
                except OSError:
                    continue
                if t.startswith("/dev/shm/PostgreSQL."):
                    used.add(t[len("/dev/shm/"):])
        except OSError:
            continue
    for f in segs:
        if f not in used:
            try:
                os.unlink(f"/dev/shm/{f}")
            except OSError:
                pass


def adopt_protected():
    """At agent start: a protected instance (the seed) whose directory exists is started again on its old slot 0..;
    everything else under the instance root is removed."""
    for name in ARGS.protect:
        d = f"{ARGS.instance_root}/{name}"
        if not os.path.isfile(f"{d}/PG_VERSION"):
            continue
        port = None
        try:
            with open(f"{d}/postgresql.auto.conf") as f:
                m = re.search(r"^port = (\d+)", f.read(), re.M)
                port = int(m.group(1)) if m else None
        except OSError:
            pass
        slot = (port - ARGS.pg_port_base) if port else free_slot()
        sb = ARGS.shared_buffers
        try:
            with open(f"{d}/postgresql.auto.conf") as f:
                m = re.search(r"^shared_buffers = '([^']+)'", f.read(), re.M)
                sb = m.group(1) if m else sb
        except OSError:
            pass
        c = new_entry(name, {"parent_role": True, "shared_buffers": sb})
        c.update(slot=slot, port=ARGS.pg_port_base + slot, state="copied")
        write_conf(c, sb, False, True)    # the current parent settings (e.g. replication slots, 6 Nov)
        c["event"].set()
        kill_by_cwd(d)
        try:
            os.unlink(f"{d}/postmaster.pid")
        except OSError:
            pass
        INSTANCES[name] = c
        try:
            c["start_ms"] = start_pg(c)
            c["state"] = "running"
            print(f"adopted {name} on port {c['port']} ({c['start_ms']:.0f} ms)", flush=True)
        except Exception as e:  # noqa: BLE001
            print(f"could not restart {name}: {e}", flush=True)
            c["state"] = "failed"
    clean_root(keep=set(INSTANCES))
    remove_orphan_shm()


# ---------------------------------------------------------------- stats
def disk_free_gb(path):
    try:
        st = os.statvfs(path)
    except OSError:
        return None
    return st.f_bavail * st.f_frsize / 2**30


def meminfo():
    with open("/proc/meminfo") as f:
        return {l.split(":")[0]: int(l.split()[1]) for l in f}


def stats():
    now = time.time()
    if STATS_CACHE["v"] is None or now - STATS_CACHE["t"] > 1.0:
        data = wal = 0
        with LOCK:
            dirs = [c["dir"] for c in INSTANCES.values()]
        for d in dirs:
            b = dir_bytes(d)
            w = dir_bytes(f"{d}/pg_wal")
            data += b - w
            wal += w
        STATS_CACHE.update(t=now, v=(data, wal))
    data, wal = STATS_CACHE["v"]
    mem = meminfo()
    with LOCK:
        n = len(INSTANCES)
        running = sum(1 for c in INSTANCES.values() if c.get("running"))
    return {"instances": n, "running": running, "data_bytes": data, "wal_bytes": wal,
            "mem_available_mb": mem["MemAvailable"] / 1024, "mem_shmem_mb": mem.get("Shmem", 0) / 1024,
            "disk_free_gb": disk_free_gb(ARGS.instance_root)}


# ---------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True    # experiment 4: headers and body as separate writes wait for a delayed ACK

    def log_message(self, fmt, *args):
        pass

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
        parts = [p for p in urlparse(self.path).path.split("/") if p]
        try:
            if method == "GET" and parts == ["time"]:
                return self.reply(200, {"t": time.time()})
            if method == "GET" and parts == ["stats"]:
                return self.reply(200, stats())
            if parts[:1] != ["instances"]:
                return self.reply(404, {"error": f"no route {method} {self.path}"})
            if method == "GET" and len(parts) == 1:
                with LOCK:
                    return self.reply(200, [reply_of(c, False) for c in INSTANCES.values()])
            if method == "POST" and len(parts) == 1:
                return self.reply(200, ensure_instance(self.body()))
            if method == "DELETE" and len(parts) == 1:
                return self.reply(200, {"deleted": delete_all()})
            name = parts[1]
            if method == "GET" and len(parts) == 2:
                with LOCK:
                    c = INSTANCES.get(name)
                if c is None:
                    return self.reply(404, {"error": f"no instance {name}"})
                if c["state"] == "creating":
                    c["event"].wait(ARGS.copy_timeout + ARGS.start_timeout + 60)
                return self.reply(200, reply_of(c, False))
            if method == "DELETE" and len(parts) == 2:
                if name in ARGS.protect:
                    return self.reply(403, {"error": f"{name} is protected"})
                return self.reply(200 if delete_instance(name) else 404, {})
            if method == "POST" and len(parts) == 3 and parts[2] == "start":
                try:
                    return self.reply(200, start_instance(name))
                except KeyError:
                    return self.reply(404, {"error": f"no instance {name}"})
            if method == "POST" and len(parts) == 3 and parts[2] == "sql":
                with LOCK:
                    c = INSTANCES.get(name)
                if c is None or not c.get("running"):
                    return self.reply(404, {"error": "no running instance"})
                b = self.body()
                r = psql(c, b["sql"], timeout=7200, db=b.get("db", "postgres"))
                return self.reply(200 if r.returncode == 0 else 500, {"stdout": r.stdout, "stderr": r.stderr})
            return self.reply(404, {"error": f"no route {method} {self.path}"})
        except Exception as e:  # noqa: BLE001
            return self.reply(500, {"error": str(e)})

    def do_GET(self):
        self.handle_any("GET")

    def do_POST(self):
        self.handle_any("POST")

    def do_DELETE(self):
        self.handle_any("DELETE")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1024


def main():
    global ARGS, COPY_SEM, START_SEM
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--listen", required=True, help="<LAN ip>:<port> (never 0.0.0.0)")
    ap.add_argument("--host", required=True, help="this node's LAN address (instances listen on it and 127.0.0.1)")
    ap.add_argument("--lan-cidr", default="10.0.0.0/8", help="pg_hba: addresses allowed to connect and copy")
    ap.add_argument("--pg-bin", default="/usr/lib/postgresql/17/bin")
    ap.add_argument("--instance-root", required=True)
    ap.add_argument("--archive-dir", required=True)
    ap.add_argument("--protect", default="seed", help="instance names DELETE /instances keeps (comma list)")
    ap.add_argument("--shared-buffers", default="40MB")
    ap.add_argument("--host-max-connections", type=int, default=20)
    ap.add_argument("--host-wal-senders", type=int, default=8)
    ap.add_argument("--max-concurrent-copies", type=int, default=16)
    ap.add_argument("--max-concurrent-starts", type=int, default=32)
    ap.add_argument("--copy-timeout", type=float, default=600)
    ap.add_argument("--start-timeout", type=float, default=300)
    ap.add_argument("--disk-guard-gb", type=float, default=20)
    ap.add_argument("--pg-port-base", type=int, default=24000)
    ap.add_argument("--fstrim", action="store_true", help="sudo fstrim the instance root after every purge")
    ARGS = ap.parse_args()
    ARGS.protect = set(filter(None, ARGS.protect.split(",")))
    host, port = ARGS.listen.rsplit(":", 1)
    if host in ("0.0.0.0", "", "::"):
        ap.error("--listen must name the LAN address, not all interfaces")
    COPY_SEM = threading.Semaphore(ARGS.max_concurrent_copies)
    START_SEM = threading.Semaphore(ARGS.max_concurrent_starts)
    os.makedirs(ARGS.archive_dir, exist_ok=True)
    os.makedirs(ARGS.instance_root, exist_ok=True)
    adopt_protected()
    print(f"pgcopy agent on {ARGS.listen}: {len(INSTANCES)} instances adopted", flush=True)
    Server((host, int(port)), Handler).serve_forever()


if __name__ == "__main__":
    main()
