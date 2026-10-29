#!/usr/bin/env python3
"""Deploy Doltgres (1.4.0 by default) as a topology describes it (scripts/setup/topologies/*.toml).

  uv run python scripts/setup/doltgres.py TOPO install        binary, config.yaml, committer identity (remote: also
                                                             apt packages, the SSD, cpu_agent.py)
  uv run python scripts/setup/doltgres.py TOPO start          start the server on the node's server placement
  uv run python scripts/setup/doltgres.py TOPO stop | restart | status
  uv run python scripts/setup/doltgres.py TOPO seed           build ch_w<W>_seed once, from the client
  uv run python scripts/setup/doltgres.py TOPO env-init [--view NODE]
                                                             <state_dir>/doltgres.env (DOLT_* + DBFORK_REMOTE_CPU)
  uv run python scripts/setup/doltgres.py TOPO deploy-client  the repo + venv + doltgres.env on the client node
  ... --dry-run                                              print every command, file and unit; run nothing

Local server node (host "local", dassl-serv-01; experiment 11's doltgres_server.sh): everything under
<state_dir>/doltgres; the server runs with setsid under `taskset -c <server_cpus> numactl --membind=<server_mem>`,
GOMAXPROCS = number of server CPUs, pid in <root>/doltgres.pid, log in <root>/logs/doltgres.log. Neon shares the
server CPUs, so start refuses while a Neon service of this user runs. The CPU sampler is per run (the workload
script starts it with --out into its data directory); start prints its command line.

Remote server node (CloudLab; experiment 1's setup_cloudlab_dolt.sh): the root is <data_root>/doltgres on the SSD
(ssd_dev, formatted XFS only if it has no filesystem, mounted at data_root); the server is the systemd unit
`doltgres` (numactl --cpunodebind/--membind=server_numa, or taskset for server_cpus), listening on 0.0.0.0 when the
client is another machine. A second unit, `dbfork-cpu-agent` (shared with Neon, nodes.py), serves
scripts/setup/cpu_agent.py on ports.cpu_agent for the runner's DBFORK_REMOTE_CPU (service mode, no CSV).
"""
from __future__ import annotations

import argparse
import json
import shlex
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from nodes import Runner, die, log  # noqa: E402
from topo import Node, Topo, TopoError, env_text, write_env  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
REMOTE_REPO = "~/db-fork"            # Runner.deploy_repo's default destination
UNIT = "doltgres"
AGENT_UNIT = Runner.CPU_UNIT          # shared with Neon (nodes.py cpu_agent_service)
NEON_PROCS = "pageserver|safekeeper|storage_control|storage_broker|compute_ctl"
COMMITTER = '{"user.email":"benchmark@example.com","user.name":"benchmark"}'

# Runs on the client with the backend's env file sourced (dblib.dolt reads DOLT_* at import). The runner's own
# seeding path: ensure_seed_database generates the seed database unless the driver can already open it.
SEED_PY = r"""
import json, sys, time
import psycopg2
from dblib.dolt import DoltToolSuite
from macrobench.schema import CH_TABLES
from microbench import runner2, task2_pb2 as tp

seed_db, W, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
t0 = time.time()
created = runner2.ensure_seed_database(tp.Backend.DOLT, tp.GeneratedData(scale_factor=W, seed_db=seed_db),
                                       scale_factor=W)
sec = round(time.time() - t0, 1)
print(f"{seed_db}: {'generated in %.1f s' % sec if created else 'exists, not regenerated'}", flush=True)
conn = psycopg2.connect(DoltToolSuite.get_initial_connection_uri(seed_db))
rows = {}
with conn.cursor() as cur:
    for t in CH_TABLES:
        cur.execute(f"SELECT count(*) FROM {t}")
        rows[t] = cur.fetchone()[0]
conn.close()
print(f"rows: {sum(rows.values()):,} in {len(rows)} tables", flush=True)
with open(out, "w") as f:
    json.dump({"seed_db": seed_db, "W": W, "rows": rows, "seconds": sec if created else None,
               "created_now": created}, f)
"""


class Doltgres:
    def __init__(self, topo: Topo, runner: Runner):
        if not topo.doltgres:
            raise TopoError("topology has no [doltgres] section")
        self.t, self.r = topo, runner
        self.srv = topo.role_node("doltgres", "server")
        self.cli = topo.client("doltgres")
        self.same_host = self.srv.host == self.cli.host
        self.port = topo.ports["doltgres"]
        self.version = topo.doltgres["version"]
        self.root = self.abspath(self.srv, self.srv.doltgres_root)
        self.bin = f"{self.root}/dl/{self.version}/doltgresql-linux-amd64/bin/doltgres"
        self.config = f"{self.root}/config.yaml"
        self.home = f"{self.root}/home"            # DOLT_ROOT_PATH: holds .dolt/config_global.json
        self.logs = f"{self.root}/logs"

    def abspath(self, node: Node, p: str) -> str:
        """config.yaml and systemd need absolute paths: expand ~ on the node (CloudLab homes are /users/<user>)."""
        if not p.startswith("~"):
            return p
        if node.local:
            return str(Path(p).expanduser())
        home = self.r.out(node, "echo $HOME") or f"/users/{self.t.ssh_user or 'USER'}"
        return home + p[1:]

    # ---- helpers ----
    def port_open(self) -> bool:
        r = self.r.run(self.srv, f"ss -ltn | grep -q ':{self.port} '", check=False)
        return bool(r) and r.returncode == 0 and not self.r.dry

    def pid(self) -> str:
        """The running server's pid ('' if none)."""
        if self.srv.local:
            p = Runner.local_pid(self.root, "doltgres")
            try:
                return str(p) if p and Path(f"/proc/{p}/comm").read_text().strip() == "doltgres" else ""
            except OSError:
                return ""
        p = self.r.unit_pid(self.srv, UNIT)
        return "" if p in ("", "0") else p

    def server_spec(self) -> str:
        """The server CPUs as cpu_agent.py takes them."""
        n = self.srv
        return n.cpus_csv("server") or (f"node:{n.server_numa}" if n.server_numa else "all")

    def agent_argv(self, script: str, out: str | None = None) -> list[str]:
        """cpu_agent.py for this deployment: sets server (+ clients on the same host), group doltgres, the disk and
        the server's memory node; it serves 0.0.0.0 when the runner is on another machine."""
        n, c = self.srv, self.cli
        argv = ["python3", script, "--port", str(self.t.ports["cpu_agent"]),
                "--bind", "127.0.0.1" if self.same_host else "0.0.0.0", "--set", f"server={self.server_spec()}"]
        if self.same_host:
            cl = c.cpus_csv("client") or (f"node:{c.client_numa}" if c.client_numa else "")
            if cl:
                argv += ["--set", f"clients={cl}"]
        if n.local:
            argv += ["--group-pidfile", f"doltgres={self.root}/doltgres.pid"]
        else:
            argv += ["--group-comm", "doltgres=doltgres"]
        if n.disk_dev:
            argv += ["--disk", n.disk_dev]
        mem = n.server_numa or (n.server_mem if n.server_mem.isdigit() else "")
        if mem:
            argv += ["--numa-node", mem]
        if out:
            argv += ["--out", out]
        return argv

    def client_state_dir(self) -> str:
        """The env file's directory on the client: the topology's state_dir, at the same path on a remote client,
        so ENV_FILE (topo.py shell) is right on either side."""
        return self.t.state_dir

    def push_env(self) -> Path:
        """Write <state_dir>/doltgres.env as the client sees the server, and copy it to a remote client."""
        path = Path(self.t.state_dir) / "doltgres.env"
        text = env_text(self.t.doltgres_env())
        if self.r.dry:
            print(f"[coordinator] write {path}:\n    " + text.rstrip("\n").replace("\n", "\n    "))
        else:
            path = write_env(self.t, "doltgres")
            log(f"wrote {path}")
        if not self.cli.local:
            sd = self.client_state_dir()
            self.r.run(self.cli, f"sudo -n mkdir -p {sd} && sudo -n chown $(id -un) {sd}")
            self.r.put(self.cli, f"{sd}/doltgres.env", text, mode=0o600)
        return path

    # ---- install ----
    def install(self) -> None:
        n = self.srv
        if not n.local:
            self.prepare_remote()
        url = (f"https://github.com/dolthub/doltgresql/releases/download/v{self.version}/"
               "doltgresql-linux-amd64.tar.gz")
        dl = f"{self.root}/dl/{self.version}"
        self.r.run(n, f"mkdir -p {dl} {self.root}/databases {self.root}/.doltcfg {self.home}/.dolt {self.logs} && "
                      f"if [ ! -x {self.bin} ]; then curl -sSL -o {dl}/dg.tar.gz {url} && "
                      f"tar xzf {dl}/dg.tar.gz -C {dl}; fi")
        # Doltgres 1.4.0 rejects max_connections and behavior keys; it takes 1,025 connections without them
        # (experiment 2). Loopback only when the runner is on this machine.
        host = "127.0.0.1" if self.same_host else "0.0.0.0"
        self.r.put(n, self.config, f"""log_level: warning
listener:
  host: {host}
  port: {self.port}
data_dir: {self.root}/databases
cfg_dir: {self.root}/.doltcfg
""")
        # dolt_commit needs a committer; DOLT_ROOT_PATH points Dolt's home here.
        cfg = f"{self.home}/.dolt/config_global.json"
        self.r.run(n, f"[ -f {cfg} ] || echo {shlex.quote(COMMITTER)} > {cfg}")
        self.r.run(n, f"{self.bin} --version")
        log(f"{n.name}: doltgres {self.version} installed in {self.root} (listener {host}:{self.port})")

    def prepare_remote(self) -> None:
        """apt packages; the SSD formatted (only if blank) and mounted at data_root; cpu_agent.py copied."""
        n = self.srv
        self.r.run(n, "export DEBIAN_FRONTEND=noninteractive; sudo -n apt-get update -qq >/dev/null 2>&1 || true; "
                      "sudo -n apt-get install -y -qq xfsprogs numactl rsync curl sysstat python3 >/dev/null")
        data_root = self.abspath(n, n.data_root)
        if n.ssd_dev:
            # never format a device that already holds a filesystem (blkid finds a signature)
            self.r.run(n, f"if ! mountpoint -q {data_root}; then "
                          f"if ! sudo -n blkid {n.ssd_dev} >/dev/null 2>&1; then sudo -n mkfs.xfs -f -q {n.ssd_dev}; fi; "
                          f"sudo -n mkdir -p {data_root} && sudo -n mount -o noatime {n.ssd_dev} {data_root}; fi; "
                          f"sudo -n chown $(id -un) {data_root} && df -h {data_root} | tail -1")
        else:
            self.r.run(n, f"mkdir -p {data_root}")
        self.r.run(n, f"mkdir -p {self.root}/bin")
        self.r.rsync(n, str(REPO / "scripts/setup/cpu_agent.py"), f"{self.root}/bin/cpu_agent.py")

    # ---- start / stop ----
    def start(self) -> None:
        n = self.srv
        if self.pid():
            die(f"doltgres already running on {n.name}: pid {self.pid()}")
        if not self.r.dry and self.r.run(n, f"test -x {self.bin}", check=False).returncode != 0:
            die(f"{self.bin} missing: run install first")
        env = {"DOLT_ROOT_PATH": self.home}
        ncpu = len(n.cpu_list("server"))
        if ncpu:
            env["GOMAXPROCS"] = ncpu
        # cd first: experiment 1 ran it in its root (WorkingDirectory), experiment 11 too; exec keeps the pid
        argv = ["bash", "-c", 'cd "$0" && exec "$@"', self.root, self.bin, "-config", self.config]
        if n.local:
            neon = self.r.out(n, f"pgrep -u $(id -u) -x '{NEON_PROCS}'", check=False)
            if neon:
                die(f"Neon is running on the same cores (pids {' '.join(neon.split())}): stop it first")
            if self.port_open():
                die(f"port {self.port} is taken")
            self.r.launch_local(n, "doltgres", argv, run_dir=self.root, log_dir=self.logs, env=env)
        else:
            self.r.systemd_unit(n, UNIT, argv, wrapper_dir=f"{self.root}/bin", env=env)
        self.r.wait_for(f"doltgres listening on :{self.port}", self.port_open, timeout=120)
        if not n.local:
            self.r.cpu_agent_service(n, f"{self.root}/bin")
        if n.local:
            log("per-run CPU sampler: uv run python " + " ".join(
                shlex.quote(a) for a in self.agent_argv(str(REPO / "scripts/setup/cpu_agent.py"),
                                                        "<data>/cpu/<run>.csv")[1:]))
        if self.r.dry:
            return
        pid = self.pid()
        if not pid:
            self.r.run(n, f"tail -20 {self.logs}/doltgres.log" if n.local else
                       f"sudo -n journalctl -u {UNIT} --no-pager | tail -20", check=False)
            die("the process on the port is not doltgres")
        log(f"doltgres pid {pid} on {n.name}, port {self.port}")
        self.r.run(n, f"taskset -cp {pid}; tr '\\0' '\\n' < /proc/{pid}/environ | "
                      f"grep -E '^(GOMAXPROCS|DOLT_ROOT_PATH)='; grep 'Max open files' /proc/{pid}/limits", check=False)
        self.select1()

    def select1(self) -> None:
        """SELECT 1 as the runner connects (from the client: also checks the listener and the route)."""
        host = self.srv.addr_from(self.cli)
        d = self.t.doltgres
        user, pw = d.get("user", "postgres"), d.get("password", "password")
        if self.cli.local:
            import psycopg2
            c = psycopg2.connect(host=host, port=self.port, user=user, password=pw, dbname="postgres",
                                 connect_timeout=10)
            c.cursor().execute("SELECT 1")
            c.close()
            log(f"SELECT 1 ok ({host}:{self.port})")
            return
        if self.r.run(self.cli, f"test -d {REMOTE_REPO}/.venv", check=False).returncode != 0:
            log(f"client {self.cli.name} has no deployed repo yet: SELECT 1 check skipped (run deploy-client)")
            return
        py = (f"import psycopg2; c = psycopg2.connect(host='{host}', port={self.port}, user='{user}', "
              f"password='{pw}', dbname='postgres', connect_timeout=10); c.cursor().execute('SELECT 1'); "
              f"print('SELECT 1 ok ({host}:{self.port}, from {self.cli.name})')")
        self.r.run(self.cli, f"cd {REMOTE_REPO} && export PATH=$HOME/.local/bin:$PATH && "
                             f"uv run python -c {shlex.quote(py)}")

    def stop(self) -> None:
        n = self.srv
        if n.local:
            pid = self.pid()
            if not pid and not self.r.dry:
                log("not running")
                Path(f"{self.root}/doltgres.pid").unlink(missing_ok=True)
                return
            # a large server can take minutes to flush on SIGTERM (experiment 11 allowed 600 s)
            self.r.stop_local(self.root, "doltgres", timeout=600)
            log(f"stopped {pid or '(dry run)'}")
        else:
            self.r.stop_unit(n, UNIT)
            log(f"{n.name}: stopped {UNIT} ({AGENT_UNIT} keeps running; Neon may share it)")

    def status(self) -> None:
        n = self.srv
        if n.local:
            pidcmd = (f"p=$(cat {self.root}/doltgres.pid 2>/dev/null); "
                      f"[ -n \"$p\" ] && [ \"$(cat /proc/$p/comm 2>/dev/null)\" = doltgres ] || p=")
        else:
            pidcmd = (f"systemctl is-active {UNIT} {AGENT_UNIT} | paste -sd' '; "
                      f"p=$(systemctl show -p MainPID --value {UNIT}); [ \"$p\" != 0 ] || p=")
        cpus = n.cpus_csv("server") or (f"$(cat /sys/devices/system/node/node{n.server_numa}/cpulist)"
                                         if n.server_numa else "ALL")
        script = f"""{pidcmd}
echo "== $(date -u +%FT%TZ) {n.name} doltgres pid=${{p:-none}}"
if [ -n "$p" ]; then
    taskset -cp "$p"
    tr '\\0' '\\n' < /proc/$p/environ | grep '^GOMAXPROCS='
    echo "-- server threads by CPU last run on (psr): count cpu"
    ps -L -o psr= -p "$p" | sort -n | uniq -c | tr '\\n' ';'; echo
    grep -E '^(VmRSS|Threads)' /proc/$p/status
    numastat -p "$p" 2>/dev/null | tail -3
    echo "-- established connections on :{self.port}: $(ss -tn state established '( sport = :{self.port} )' | tail -n +2 | wc -l)"
fi
echo "-- client processes (Cpus_allowed_list)"
for q in $(pgrep -f 'microbench.runner2|cpu_agent' || true); do
    printf '%s %s: %s\\n' "$q" "$(cut -c1-60 /proc/$q/cmdline 2>/dev/null | tr '\\0' ' ')" \\
        "$(grep Cpus_allowed_list /proc/$q/status 2>/dev/null | cut -f2)"
done | sort -k3 | uniq -f2 -c | head
echo "-- mpstat on the server CPUs ({cpus}), 3 s"
mpstat -P "{cpus}" 1 3 | grep -E 'Average' | head -22
echo "-- top CPU users on the machine"
ps -eo pid,user,psr,pcpu,comm --sort=-pcpu | head -8
du -sh {self.root}/databases 2>/dev/null
true"""
        self.r.run(n, script, check=False)

    # ---- data ----
    def seed(self) -> None:
        """ch_w<W>_seed, generated once by runner2's own seeding code on the client (the benchmark runs copy from it
        with --seed-db). Writes <state_dir>/doltgres_seed.json."""
        W = int(self.t.wl("doltgres")["W"])
        seed_db = f"ch_w{W}_seed"
        env = self.push_env()
        c = self.cli
        repo = str(REPO) if c.local else REMOTE_REPO
        env_on_client = str(env) if c.local else f"{self.client_state_dir()}/doltgres.env"
        result = f"{self.client_state_dir()}/doltgres_seed.result.json"
        bind = " ".join(shlex.quote(a) for a in Runner.bind_prefix(c, "client"))
        log(f"seed {seed_db} (W={W}) from {c.name}{' under ' + bind if bind else ''}")
        self.r.run(c, f"cd {repo} && export PATH=$HOME/.local/bin:$PATH PYTHONUNBUFFERED=1 && set -a && "
                      f". {env_on_client} && set +a && rm -f {result} && "
                      f"{bind} uv run python - {seed_db} {W} {result}", input=SEED_PY)
        if self.r.dry:
            print(f"(write {self.t.state_dir}/doltgres_seed.json from {c.name}:{result})")
            return
        res = json.loads(self.r.out(c, f"cat {result}"))
        out = Path(self.t.state_dir) / "doltgres_seed.json"
        if res["seconds"] is None and out.exists():     # existed: keep the generation time recorded then
            res["seconds"] = json.loads(out.read_text()).get("seconds")
        res["checked_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        out.write_text(json.dumps(res, indent=1) + "\n")
        log(f"wrote {out}: {sum(res['rows'].values()):,} rows")

    def deploy_client(self) -> None:
        self.r.deploy_repo(self.cli, REPO, REMOTE_REPO)
        self.push_env()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("topo")
    ap.add_argument("cmd", choices=["install", "start", "stop", "restart", "status", "seed", "env-init",
                                    "deploy-client"])
    ap.add_argument("--view", help="env-init: the node the runner runs on (default: the client)")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    try:
        topo = Topo(a.topo)
        d = Doltgres(topo, Runner(topo, dry_run=a.dry_run))
        if a.cmd == "install":
            d.install()
        elif a.cmd == "start":
            d.start()
        elif a.cmd == "stop":
            d.stop()
        elif a.cmd == "restart":
            d.stop()
            d.start()
        elif a.cmd == "status":
            d.status()
        elif a.cmd == "seed":
            d.seed()
        elif a.cmd == "env-init":
            if a.dry_run:
                print(env_text(topo.doltgres_env(a.view)), end="")
            else:
                print(write_env(topo, "doltgres", a.view))
        elif a.cmd == "deploy-client":
            d.deploy_client()
    except TopoError as e:
        print(f"topology error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
