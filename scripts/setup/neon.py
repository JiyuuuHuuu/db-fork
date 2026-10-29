#!/usr/bin/env python3
"""Deploy and operate self-hosted Neon as a topology file describes it (scripts/setup/topologies/*.toml).

Two deployments, chosen by the topology:
- one host, `transport = "uds"` (experiments 10/11): every service a local process (setsid, taskset/numactl of the
  node's server placement, pid files in <neon_root>/run), every connection a Unix socket in that directory;
- CloudLab nodes, `transport = "tcp"` (experiments 3/4/5/8): every service a systemd unit (neon-<service>) on its
  node over ssh, binaries rsynced from this machine (the coordinator), storage on the nodes' SSD, computes on a
  tmpfs where configured; LAN within a zone, public addresses across zones; the pageservers after the first join
  with scheduling Pause and share the first one's remote storage over NFS (shard-split moves shards to them).

  neon.py TOPO install             snapshot the build's binaries into <state_dir>/neon-bin, record the fork's commit,
                                   patch and hashes in <state_dir>/code/; prepare the remote nodes (packages, SSD,
                                   tmpfs, RemoveIPC=no, kernel.shmmni) and copy binaries, agent, template, hook stub
  neon.py TOPO start | stop | restart | status
  neon.py TOPO wipe --yes          stop, then delete all Neon state (storage, computes, seed, env files)
  neon.py TOPO seed [--scale W]    load CH-benCHmark W on the seeding node; writes <state_dir>/neon_seed.json, neon.env
  neon.py TOPO env-init [--view NODE]
                                   <state_dir>/neon.env (NEON_LOCAL_* as the client sees the cluster), or
                                   neon.<NODE>.env for another node's view
  neon.py TOPO shard-split --shards K
                                   split the seeded tenant over K pageservers (one-way); stores ps_opts
  neon.py TOPO purge | smoke | clock-check | metrics FILE
                                   scripts/setup/neon_ctl.py on the client node
  neon.py TOPO deploy-client       repo + uv venv on the client node, neon.env and neon_seed.json into its state_dir
  neon.py TOPO pull-archive DEST   every agent's archive (samples.jsonl gzipped, starts.jsonl, pg/*.log) -> DEST/<node>/

Every command takes --dry-run: print every command, ssh call, file (unit, wrapper, pageserver.toml) and launch
instead of running it.

Agents: on the single host one agent (computes on). On a cluster an agent runs on every Neon server node (exp 5):
on compute_agents and seed_compute nodes it starts computes; on all it samples the local services' /metrics into
its archive and serves /cpu and /time.
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import shlex
import signal
import subprocess
import sys
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))

from nodes import Runner, die, log  # noqa: E402
from topo import Node, Topo, TopoError, env_text, read_seed, write_env  # noqa: E402

BINARIES = ("pageserver", "safekeeper", "storage_broker", "storage_controller", "storcon_cli", "compute_ctl",
            "pagebench")
ROLE_BINARIES = {          # what each role needs from the snapshot ("pg": pg_install/v17)
    "broker": ("storage_broker",),
    "storcon": ("storage_controller", "storcon_cli", "pg"),     # + its Postgres
    "safekeeper": ("safekeeper",),
    "pageserver": ("pageserver", "pagebench", "pg"),            # WAL redo runs Postgres
    "compute": ("compute_ctl", "pg"),
}
SERVICES = ("broker", "storcon-pg", "hook", "storcon", "safekeeper", "pageserver", "agent")   # start order
SVC_FILES = ("neon_node_agent.py", "compute_config_template.json", "compute_hook_stub.py")

# Constants of the sources (override in [neon] where a key is named)
PS_PAGE_CACHE_PAGES = {"uds": 131072,   # 1 GiB (exp 10/11: preallocated; holds layer index blocks only, since
                       "tcp": 1048576}  # GetPage reads use direct I/O); 8 GiB (exp 5). [neon] ps_page_cache_size
STORCON_PG_MAX_CONNECTIONS = 300
SHMMNI = 32768                 # one SysV segment per compute; killed computes leave theirs (agent removes them)
LOCAL_DISK_GUARD_GB = 40       # exp 11: the agent refuses a compute start with less free space on the compute root
SAMPLE_MAX_COMPUTES = {"uds": 0, "tcp": 8}   # computes queried per sampler tick (exp 11: none, it costs server CPU)
SAMPLE_INTERVAL_S = 1
FANOUT_PORT = 8765             # compute binaries fanned out within a zone over HTTP (exp 5: across the WAN once)
EXPORT_OPTS = "rw,sync,no_subtree_check,no_root_squash"
NFS_OPTS = "rw,hard,noatime,vers=4.2"
REMOTE_REPO = "~/db-fork"
APT_PACKAGES = ("rsync", "curl")


def q(s) -> str:
    return shlex.quote(str(s))


class Neon:
    def __init__(self, topo: Topo, dry: bool):
        if not topo.neon:
            raise TopoError("topology has no [neon] section")
        self.t = topo
        self.n = topo.neon
        self.dry = dry
        self.r = Runner(topo, dry)
        self.uds = topo.uds
        self.tr = self.n["transport"]
        self.coord = Node("coordinator", {}, topo)
        self.state = topo.state_dir
        self.stage = f"{topo.state_dir}/neon-bin"
        self.sc = topo.node(self.n["storcon"])
        self.broker = topo.node(self.n["broker"])
        self.pss = [topo.node(x) for x in self.n["pageservers"]]
        self.sks = [topo.node(x) for x in self.n["safekeepers"]]
        self.seed_node = topo.node(self.n["seed_compute"])
        self.client = topo.node(self.n["client"])
        self.w = topo.wl("neon")
        self._home: dict[str, str] = {}
        if self.uds:
            # sun_path holds 108 bytes (the longest socket: a compute's c<slot>/.s.PGSQL.<port>), and the
            # walproposer splits neon.safekeepers on ':' and ',' (exp 10)
            longest = f"{self.run_dir}/c9999/.s.PGSQL.29999"
            if len(longest.encode()) > 107 or any(ch in self.run_dir for ch in ":,"):
                raise TopoError(f"socket directory {self.run_dir} is too long or contains ':' or ','")

    # ---------------------------------------------------------------- roles and paths
    def roles(self, node: Node) -> set[str]:
        n, r = self.n, set()
        for role in ("client", "storcon", "broker"):
            if n[role] == node.name:
                r.add(role)
        if node.name in n["pageservers"]:
            r.add("pageserver")
        if node.name in n["safekeepers"]:
            r.add("safekeeper")
        if node.name in n["compute_agents"] or node.name == n["seed_compute"]:
            r.add("compute")
        return r

    def server_nodes(self) -> list[Node]:
        return [self.t.node(x) for x in self.t.neon_nodes() if self.roles(self.t.node(x)) - {"client"}]

    def agent_nodes(self) -> list[Node]:
        """Single host: the one agent. Cluster: every server node (computes only where the role says so)."""
        if self.uds:
            return [self.t.node(self.n["compute_agents"][0])]
        return self.server_nodes()

    def placement(self) -> list[tuple[str, Node, int]]:
        """(service, node, id) in start order; id = position in the role's list (safekeepers, pageservers)."""
        out = [("broker", self.broker, 0), ("storcon-pg", self.sc, 0), ("hook", self.sc, 0), ("storcon", self.sc, 0)]
        out += [("safekeeper", n, k) for k, n in enumerate(self.sks, 1)]
        out += [("pageserver", n, k) for k, n in enumerate(self.pss, 1)]
        out += [("agent", n, 0) for n in self.agent_nodes()]
        return out

    def home(self, node: Node) -> str:
        if node.local:
            return os.path.expanduser("~")
        if node.name not in self._home:
            # dry run: CloudLab's home directory layout instead of asking the node
            self._home[node.name] = (f"/users/{self.t.ssh_user or getpass.getuser()}" if self.dry
                                     else self.r.out(node, "echo $HOME"))
        return self._home[node.name]

    def x(self, node: Node, path: str) -> str:
        """Expand ~ for the node: wrapper scripts and units quote their arguments, so ~ would stay literal."""
        return self.home(node) + path[1:] if path.startswith("~") else path

    def root(self, node: Node) -> str:
        return self.x(node, node.neon_root)

    def d(self, node: Node, sub: str) -> str:
        """<neon_root>/<sub>: ps, ps-remote, sk, sk-remote, storcon_db, archive, logs, svc, computes-disk."""
        return f"{self.root(node)}/{sub}"

    def bin(self, node: Node) -> str:
        return self.stage if node.local else self.d(node, "bin")

    def croot(self, node: Node) -> str:
        return self.x(node, node.compute_root)

    def disk_croot(self, node: Node) -> str:
        """Data directories of computes started with pgdata_on_disk: on the node's disk when computes are on a tmpfs
        (exp 5: a data directory holds a 16 MB WAL segment, too much for hundreds per node in RAM)."""
        return self.d(node, "computes-disk") if node.tmpfs_size else ""

    @property
    def run_dir(self) -> str:
        return self.t.neon_run_dir()

    def user(self, node: Node) -> str:
        return self.t.ssh_user if (not node.local and self.t.ssh_user) else getpass.getuser()

    def lib_env(self, node: Node) -> dict:
        return {"LD_LIBRARY_PATH": f"{self.bin(node)}/pg_install/v17/lib"}

    # ---------------------------------------------------------------- helpers
    def curl(self, viewer: Node, name: str, port_key: str, target: Node, path: str, opts: str = "-sf") -> str:
        """curl of a service endpoint, run on `viewer`."""
        if self.uds:
            return f"curl {opts} --unix-socket {self.t.sock(name)} http://localhost{path}"
        return f"curl {opts} http://{target.addr_from(viewer)}:{self.t.ports[port_key]}{path}"

    def wait(self, node: Node, what: str, check: str, secs: int) -> None:
        """Poll `check` on the node once a second (one ssh call)."""
        self.r.run(node, f"for i in $(seq {secs}); do {check} >/dev/null 2>&1 && exit 0; sleep 1; done; "
                         f"echo 'timeout waiting for {what}' >&2; exit 1")

    def write_coord(self, path: str, text: str) -> None:
        """A file on the coordinator."""
        if self.dry:
            print(f"[coordinator] write {path}:")
            print("    " + text.rstrip("\n").replace("\n", "\n    "))
            return
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text)

    def to_file(self, node: Node, cmd: str, dst: str) -> None:
        """stdout of `cmd` on the node into the local file dst."""
        argv = ["bash", "-c", cmd] if node.local else self.r.ssh_argv(node) + ["bash -c " + q(cmd)]
        if self.dry:
            print(f"[{node.name}] $ {cmd} > (coordinator) {dst}")
            return
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        with open(dst + ".tmp", "wb") as f:
            subprocess.run(argv, stdout=f, check=True)
        os.replace(dst + ".tmp", dst)

    def parallel(self, fn, items) -> None:
        items = list(items)
        if self.dry or len(items) < 2:
            for it in items:
                fn(it)
            return
        with ThreadPoolExecutor(min(16, len(items))) as ex:
            list(ex.map(fn, items))

    def ensure_state_dir(self, node: Node) -> None:
        """The topology's state_dir on a remote node (env files, seed json): root-owned parents need sudo."""
        s = self.state
        self.r.run(node, f"mkdir -p {s} 2>/dev/null || {{ sudo -n mkdir -p {s} && "
                         f"sudo -n chown $(id -u):$(id -g) {s}; }}")

    def apt(self, pkgs) -> str:
        pk = " ".join(pkgs)
        return (f"export DEBIAN_FRONTEND=noninteractive; need=''; for p in {pk}; do dpkg -s $p >/dev/null 2>&1 "
                f"|| need=\"$need $p\"; done; if [ -n \"$need\" ]; then sudo -n apt-get update -qq >/dev/null "
                f"2>&1 || true; sudo -n apt-get install -y -qq $need >/dev/null; fi")

    def remove_ipc_no(self, node: Node) -> None:
        # logind deletes a user's SysV/POSIX shared memory when their last session ends: it kills every Postgres
        # (storcon's database, the computes) and the runner's semaphores (exp 3, exp 5)
        self.r.run(node, "grep -q '^RemoveIPC=no' /etc/systemd/logind.conf || { echo RemoveIPC=no | sudo -n tee -a "
                         "/etc/systemd/logind.conf >/dev/null && sudo -n systemctl restart systemd-logind; }")

    def check_linger(self) -> None:
        """Single host (no sudo): logind's RemoveIPC applies unless the user lingers (exp 10)."""
        out = self.r.out(self.coord, "loginctl show-user $(id -un) -p Linger 2>/dev/null", check=False)
        if out and out.strip() != "Linger=yes":
            log("warning: this user does not linger (loginctl enable-linger): logind may delete the services' "
                "shared memory when the last session ends")

    # ---------------------------------------------------------------- install
    def install(self, fanout: bool = True) -> None:
        build = self.n["build_dir"]
        rel = f"{build}/target/release"
        missing = [b for b in BINARIES if not Path(f"{rel}/{b}").exists()]
        if not Path(f"{build}/pg_install/v17/bin/postgres").exists():
            missing.append("pg_install/v17")
        if missing:
            (log if self.dry else die)(f"{'warning: ' if self.dry else ''}not built in {build}: {missing}")
        if any(n.local for n in self.server_nodes()) and not self.dry:
            alive = self.running()
            if alive:
                die(f"stop the local services first ({', '.join(alive)} run on the staged binaries)")
        c, st, code = self.coord, self.stage, f"{self.state}/code"
        log(f"snapshot {build} -> {st}")
        self.r.run(c, f"mkdir -p {st} {code} && for b in {' '.join(BINARIES)}; do cp -f {rel}/$b {st}/$b; done && "
                      f"rm -rf {st}/pg_install && mkdir -p {st}/pg_install && cp -a {build}/pg_install/v17 "
                      f"{st}/pg_install/v17")
        self.r.run(c, f"cd {st} && sha256sum {' '.join(BINARIES)} pg_install/v17/bin/postgres "
                      f"pg_install/v17/lib/postgresql/neon.so > {code}/bin_sha256.txt")
        self.r.run(c, f"cd {build} && {{ echo \"branch $(git branch --show-current) commit $(git rev-parse HEAD)\"; "
                      f"git status --short; }} > {code}/neon_fork_commit.txt")
        self.r.run(c, f"cd {build} && {{ git diff HEAD -- . ':(exclude)experiments'; git ls-files --others "
                      f"--exclude-standard -- . ':(exclude)experiments' | while read -r f; do "
                      f"git diff --no-index /dev/null \"$f\" || true; done; }} > {code}/neon_fork.patch")
        nodes = self.server_nodes()
        remote = [n for n in nodes if not n.local]
        if any(n.local for n in nodes):
            self.check_linger()
        log(f"prepare {len(remote)} remote nodes")
        self.parallel(self.prepare, remote)
        # binaries: compute-only nodes of one zone fetch them from the first such node over the LAN
        direct, fan = list(remote), {}
        if fanout:
            by_zone: dict[str, list[Node]] = {}
            for n in remote:
                if self.roles(n) - {"client"} == {"compute"}:
                    by_zone.setdefault(n.zone, []).append(n)
            for z, group in by_zone.items():
                if len(group) > 2:
                    fan[group[0].name] = group[1:]
                    direct = [n for n in direct if n not in group[1:]]
        log(f"binaries to {len(direct)} nodes" + (f", fanned out from {', '.join(fan)}" if fan else ""))
        self.parallel(self.push_binaries, direct)
        for src, group in fan.items():
            self.fanout(self.t.node(src), group)
        for n in nodes:
            svc = self.d(n, "svc")
            self.r.run(n, f"mkdir -p {svc}")
            files = [f for f in SVC_FILES if f != "compute_hook_stub.py" or n.name == self.sc.name]
            for f in files:
                self.r.rsync(n, str(HERE / f), f"{svc}/{f}")
        log(f"installed; fork state in {code}/")

    def prepare(self, node: Node) -> None:
        """Experiment 5's node setup: packages, SSD (formatted only without a filesystem), tmpfs, logind, shmmni."""
        pk = list(APT_PACKAGES) + (["xfsprogs"] if node.ssd_dev else [])
        lines = ["set -euo pipefail", self.apt(pk)]
        dr = self.x(node, node.data_root)
        if node.ssd_dev:
            dev = node.ssd_dev
            lines += [f"if ! mountpoint -q {dr}; then",
                      f"    [ -b {dev} ] || {{ echo \"$(hostname -s): no {dev}\" >&2; exit 1; }}",
                      f"    sudo -n blkid {dev} >/dev/null 2>&1 || sudo -n mkfs.xfs -f -q {dev}",
                      f"    sudo -n mkdir -p {dr} && sudo -n mount -o noatime {dev} {dr}",
                      "fi",
                      f"sudo -n chown $(id -u):$(id -g) {dr}"]
        if node.tmpfs_size:
            mnt, size = os.path.dirname(self.croot(node).rstrip("/")), node.tmpfs_size
            opts = f"size={size},mode=0755,uid=$(id -u)"
            lines += [f"if ! mountpoint -q {mnt}; then sudo -n mkdir -p {mnt} && sudo -n mount -t tmpfs -o {opts} "
                      f"tmpfs {mnt}; else sudo -n mount -o remount,size={size} {mnt}; fi",
                      f"sudo -n sed -i '\\| {mnt} tmpfs |d' /etc/fstab",
                      f"echo \"tmpfs {mnt} tmpfs {opts} 0 0\" | sudo -n tee -a /etc/fstab >/dev/null"]
        if "compute" in self.roles(node):
            lines += [f"echo 'kernel.shmmni = {SHMMNI}' | sudo -n tee /etc/sysctl.d/90-neon-computes.conf >/dev/null",
                      "sudo -n sysctl -q -p /etc/sysctl.d/90-neon-computes.conf"]
        lines += [f"mkdir -p {self.d(node, 'bin')} {self.d(node, 'svc')}",
                  f"echo \"  $(hostname -s): $(findmnt -no SOURCE,FSTYPE,SIZE {dr} 2>/dev/null | head -1)\""]
        self.r.run(node, "\n".join(lines))
        self.remove_ipc_no(node)

    def binaries_of(self, node: Node) -> list[str]:
        out: list[str] = []
        for role in sorted(self.roles(node) - {"client"}):
            out += [b for b in ROLE_BINARIES[role] if b not in out]
        return out

    def push_binaries(self, node: Node) -> None:
        inc = []
        for b in self.binaries_of(node):
            inc += ["--include", "/pg_install/***"] if b == "pg" else ["--include", f"/{b}"]
        self.r.rsync(node, f"{self.stage}/", self.d(node, "bin"), extra=["-z", *inc, "--exclude", "*"], delete=True)

    def fanout(self, src: Node, group: list[Node]) -> None:
        """Exp 5: one tarball over HTTP from src, a transient systemd unit (a background process over ssh would
        keep the session open)."""
        root = self.root(src)
        self.r.run(src, f"tar -C {root} -czf ~/neon-bin.tgz bin && sudo -n systemd-run --unit=neon-binsrv "
                        f"--uid=$(id -u) --working-directory=$HOME python3 -m http.server {FANOUT_PORT} "
                        f"--bind {src.lan} >/dev/null && sleep 2")
        try:
            def get(n: Node) -> None:
                self.r.run(n, f"mkdir -p {self.root(n)} && curl -sf http://{src.addr_from(n)}:{FANOUT_PORT}/"
                              f"neon-bin.tgz | tar -C {self.root(n)} -xzf - && ls {self.d(n, 'bin')} | paste -sd ' '")
            self.parallel(get, group)
        finally:
            self.r.run(src, "sudo -n systemctl stop neon-binsrv 2>/dev/null; rm -f ~/neon-bin.tgz", check=False)

    # ---------------------------------------------------------------- service control
    def launch(self, node: Node, svc: str, argv: list[str]) -> None:
        if node.local:
            self.r.launch_local(node, svc, argv, self.run_dir, self.d(node, "logs"), self.lib_env(node))
        else:
            self.r.systemd_unit(node, f"neon-{svc}", argv, self.d(node, "svc"), self.lib_env(node))

    def running(self) -> list[str]:
        """'service@node' of every service that runs (local: pid file; remote: systemctl)."""
        out, remote = [], {}
        for svc, node, _ in self.placement():
            if node.local:
                if Runner.local_pid(self.run_dir, svc):
                    out.append(f"{svc}@{node.name}")
            else:
                remote.setdefault(node.name, []).append(svc)
        for name, svcs in remote.items():
            if self.dry:
                continue
            st = self.r.out(self.t.node(name), "systemctl is-active " + " ".join(f"neon-{s}" for s in svcs),
                            check=False).split()
            out += [f"{s}@{name}" for s, a in zip(svcs, st) if a == "active"]
        return out

    def guard_doltgres(self) -> None:
        """Neon and Doltgres share the single host's server CPUs: one at a time."""
        if not any(n.local for n in self.server_nodes()):
            return
        r = subprocess.run(["pgrep", "-u", str(os.getuid()), "-x", "doltgres"], capture_output=True, text=True)
        if r.returncode == 0:
            msg = f"a Doltgres server of this user runs (pid {r.stdout.split()[0]}); it shares the server CPUs"
            if self.dry:
                log("warning: " + msg)
            else:
                die(msg + ": stop it first")

    def start(self) -> None:
        self.guard_doltgres()
        alive = [] if self.dry else self.running()
        if alive:
            die(f"already running: {', '.join(alive)} (stop or restart)")
        if any(n.local for n in self.server_nodes()) and not self.dry and not Path(f"{self.stage}/pageserver").exists():
            die(f"no binaries in {self.stage}: run install first")
        t, P = self.t, self.t.ports
        for n in self.server_nodes():
            dirs = [self.d(n, "archive"), self.d(n, "logs")]
            if n.local and self.uds:
                dirs.append(self.run_dir)
            if "compute" in self.roles(n):
                dirs += [self.croot(n)] + ([self.disk_croot(n)] if self.disk_croot(n) else [])
            self.r.run(n, "mkdir -p " + " ".join(dirs))
        if self.uds:
            self.r.run(self.sc, f"rm -f {self.run_dir}/*.sock {self.run_dir}/.s.PGSQL.*")

        # storage broker
        b = self.broker
        self.launch(b, "broker", [f"{self.bin(b)}/storage_broker", f"--listen-addr={t.listen('broker', 'broker')}"])

        # Postgres of the storage controller (neon_rmgr: it must replay Neon's own WAL records after a crash)
        sc, sbin = self.sc, self.bin(self.sc)
        pg = f"{sbin}/pg_install/v17/bin"
        db, user, port = self.d(sc, "storcon_db"), self.user(sc), P["storcon_pg"]
        lib = self.lib_env(sc)["LD_LIBRARY_PATH"]
        self.r.run(sc, f"[ -d {db} ] || LD_LIBRARY_PATH={lib} {pg}/initdb -D {db} -U {user} >/dev/null")
        if self.uds:
            listen, sockdir, host = "", self.run_dir, self.run_dir
            db_url = (f"postgresql://{user}@{urllib.parse.quote(self.run_dir, safe='')}:{port}/storage_controller")
        else:
            listen, sockdir, host = "localhost", "", "localhost"
            db_url = f"postgresql://{user}@localhost:{port}/storage_controller"
        self.launch(sc, "storcon-pg", [f"{pg}/postgres", "-D", db, "-p", str(port), "-c", f"listen_addresses={listen}",
                                       "-c", f"unix_socket_directories={sockdir}",
                                       "-c", f"max_connections={STORCON_PG_MAX_CONNECTIONS}",
                                       "-c", "shared_preload_libraries=neon_rmgr"])
        self.wait(sc, "storcon-pg", f"LD_LIBRARY_PATH={lib} {pg}/pg_isready -h {host} -p {port}", 60)
        self.r.run(sc, f"LD_LIBRARY_PATH={lib} {pg}/createdb -h {host} -p {port} storage_controller 2>/dev/null "
                       "|| true")

        # compute-notification stub, then the storage controller
        self.launch(sc, "hook", ["python3", f"{self.d(sc, 'svc')}/compute_hook_stub.py",
                                 "--listen", t.listen("hook", "hook")])
        self.launch(sc, "storcon", [f"{sbin}/storage_controller", "--dev", "--listen", t.listen("storcon", "storcon"),
                                    "--database-url", db_url,
                                    "--tenant-rate-limit", str(self.w["storcon_rate_limit"]),
                                    "--control-plane-url", t.http_url("hook", "hook", sc, sc)])
        self.wait(sc, "storcon", self.curl(sc, "storcon", "storcon", sc, "/status"), 120)

        # safekeepers (id = position in the list)
        for k, n in enumerate(self.sks, 1):
            sk, skr = self.d(n, "sk"), self.d(n, "sk-remote")
            self.r.run(n, f"mkdir -p {sk} {skr}")
            adv = f"{self.run_dir}:{P['sk_pg']}" if self.uds else f"{self.sk_advertise(n)}:{P['sk_pg']}"
            self.launch(n, "safekeeper", [
                f"{self.bin(n)}/safekeeper", "-D", sk, f"--id={k}", f"--listen-pg={t.listen('sk', 'sk_pg', 'pg')}",
                f"--advertise-pg={adv}", f"--listen-http={t.listen('sk-http', 'sk_http')}",
                f"--broker-endpoint={t.http_url('broker', 'broker', self.broker, n)}",
                f"--availability-zone=az{k}", f'--remote-storage={{local_path="{skr}"}}'])

        # pageservers: one remote storage for all (NFS), then config, identity (id = position) and start
        self.nfs_remote_storage()
        page_cache = self.n.get("ps_page_cache_size", PS_PAGE_CACHE_PAGES[self.tr])
        for k, n in enumerate(self.pss, 1):
            ps, psr = self.d(n, "ps"), self.d(n, "ps-remote")
            self.r.run(n, f"mkdir -p {ps} {psr}")
            self.r.put(n, f"{ps}/pageserver.toml", f"""\
listen_pg_addr    = "{t.listen('ps', 'ps_pg', 'pg')}"
listen_http_addr  = "{t.listen('ps-http', 'ps_http')}"
pg_distrib_dir    = "{self.bin(n)}/pg_install"
broker_endpoint   = "{t.http_url('broker', 'broker', self.broker, n)}"
control_plane_api = "{t.http_url('storcon', 'storcon', sc, n, '/upcall/v1/')}"
availability_zone = "{n.zone}"
remote_storage = {{ local_path = "{psr}" }}
max_file_descriptors = 65536
page_cache_size      = {page_cache}
disk_usage_based_eviction = {{ enabled = false }}
""")
            self.r.put(n, f"{ps}/identity.toml", f"id={k}\n")
            self.launch(n, "pageserver", [f"{self.bin(n)}/pageserver", "-D", ps])
        for n in self.sks:
            self.wait(n, f"safekeeper on {n.name}", self.curl(n, "sk-http", "sk_http", n, "/v1/status"), 60)
        for n in self.pss:
            self.wait(n, f"pageserver on {n.name}", self.curl(n, "ps-http", "ps_http", n, "/v1/status"), 120)
        self.register_pageservers()

        # agents
        for n in self.agent_nodes():
            self.launch(n, "agent", self.agent_argv(n))
        for n in self.agent_nodes():
            self.wait(n, f"agent on {n.name}", self.curl(n, "agent", "agent", n, "/time"), 30)
        # CPU sampling for the runner's DBFORK_REMOTE_CPU (topo.remote_cpu). A single host gets a per-run sampler
        # from the workload scripts instead.
        for n in self.server_nodes():
            if not n.local:
                self.r.cpu_agent_service(n, self.d(n, "svc"))
        log("started")
        if not self.dry:
            self.status()

    def sk_advertise(self, node: Node) -> str:
        """Address the pageservers reach this safekeeper on (published through the broker)."""
        if all(p.zone == node.zone for p in self.pss):
            return node.lan
        return node.public

    def register_pageservers(self) -> None:
        sc, P = self.sc, self.t.ports
        lst = self.curl(sc, "storcon", "storcon", sc, "/control/v1/node", "-s")
        for k, n in enumerate(self.pss, 1):
            if self.uds:
                pg_addr, http_addr = self.run_dir, f"unix:{self.t.sock('ps-http')}"
            else:
                pg_addr = http_addr = n.addr_from(sc)
            body = {"node_id": k, "listen_pg_addr": pg_addr, "listen_pg_port": P["ps_pg"], "listen_grpc_addr": None,
                    "listen_grpc_port": None, "listen_http_addr": http_addr, "listen_http_port": P["ps_http"],
                    "listen_https_port": None, "availability_zone_id": n.zone, "node_ip_addr": None}
            json_h = "-H 'Content-Type: application/json'"
            cmd = (f"if ! {lst} | grep -Eq '\"(node_)?id\":{k},'; then "
                   + self.curl(sc, "storcon", "storcon", sc, "/control/v1/node",
                               f"-sf -X POST {json_h} -d {q(json.dumps(body))}"))
            if k > 1:
                # every pageserver but the first starts paused: no tenant lands on it until shard-split moves one
                cmd += " && sleep 2 && " + self.curl(sc, "storcon", "storcon", sc, f"/control/v1/node/{k}/config",
                                                     f"-sf -X PUT {json_h} -d "
                                                     + q(json.dumps({"node_id": k, "scheduling": "Pause"})))
            self.r.run(sc, cmd + "; fi")
        self.wait(sc, "pageservers Active",
                  f"[ $({lst} | grep -o '\"availability\":\"Active\"' | wc -l) -ge {len(self.pss)} ]", 120)
        out = self.r.out(sc, lst, check=False)
        if out:
            log(f"storage controller nodes: {out}")

    def nfs_remote_storage(self) -> None:
        """Exp 5 (setup_remote_storage_nfs.sh): shards move between pageservers only through remote storage, so all
        of them use the first one's directory, exported over NFS and mounted at each other's ps-remote."""
        if len(self.pss) < 2:
            return
        srv = self.pss[0]
        path = self.d(srv, "ps-remote")
        clients = [(n, n.addr_from(srv)) for n in self.pss[1:]]
        line = path + " " + " ".join(f"{a}({EXPORT_OPTS})" for _, a in clients)
        self.r.run(srv, "\n".join([
            "set -euo pipefail", self.apt(["nfs-kernel-server"]), f"mkdir -p {path}",
            f"sudo -n sed -i '\\|^{path} |d' /etc/exports",
            f"echo {q(line)} | sudo -n tee -a /etc/exports >/dev/null",
            "sudo -n systemctl enable --now nfs-kernel-server >/dev/null 2>&1",
            "sudo -n exportfs -ra"]))
        for n, _ in clients:
            mnt, src = self.d(n, "ps-remote"), f"{srv.addr_from(n)}:{path}"
            self.r.run(n, "\n".join([
                "set -euo pipefail", self.apt(["nfs-common"]), f"mkdir -p {mnt}",
                f"if ! mountpoint -q {mnt}; then",
                # this pageserver holds no tenant yet, so its own remote directory must be empty
                f"    if [ -n \"$(ls -A {mnt})\" ]; then echo \"{mnt} not empty on $(hostname -s)\" >&2; exit 1; fi",
                f"    sudo -n mount -t nfs -o {NFS_OPTS} {src} {mnt}",
                "fi",
                f"sudo -n sed -i '\\| {mnt} nfs |d' /etc/fstab",
                f"echo '{src} {mnt} nfs {NFS_OPTS} 0 0' | sudo -n tee -a /etc/fstab >/dev/null",
                f"findmnt -no SOURCE,FSTYPE {mnt}"]))

    def agent_services(self, node: Node) -> str:
        """label=url of the /metrics endpoints the node's agent samples (its own services)."""
        t, out = self.t, []
        if self.uds:
            out = [f"pageserver={t.http_url('ps-http', 'ps_http', self.pss[0], node, '/metrics')}",
                   f"sk1={t.http_url('sk-http', 'sk_http', self.sks[0], node, '/metrics')}",
                   f"storcon={t.http_url('storcon', 'storcon', self.sc, node, '/metrics')}"]
            return ",".join(out)
        P = t.ports
        if node.name in self.n["pageservers"]:
            out.append(f"pageserver=http://127.0.0.1:{P['ps_http']}/metrics")
        if node.name == self.sc.name:
            out.append(f"storcon=http://127.0.0.1:{P['storcon']}/metrics")
        if node.name in self.n["safekeepers"]:
            out.append(f"sk{self.n['safekeepers'].index(node.name) + 1}=http://127.0.0.1:{P['sk_http']}/metrics")
        return ",".join(out)

    def compute_host(self, node: Node) -> str:
        """The address the agent hands out for its computes: as the client sees the node (a seeding-only node is
        used by the seeding client on itself)."""
        if node.name not in self.n["compute_agents"]:
            return node.lan
        a = node.addr_from(self.client)
        return node.lan if a == "127.0.0.1" and not node.local else a

    def agent_argv(self, node: Node) -> list[str]:
        t, P, w = self.t, self.t.ports, self.w
        svc = self.d(node, "svc")
        argv = ["python3", f"{svc}/neon_node_agent.py", "--listen", t.listen("agent", "agent"),
                "--transport", self.tr, "--archive-dir", self.d(node, "archive"),
                "--sample-interval", str(SAMPLE_INTERVAL_S), "--sample-max-computes", str(SAMPLE_MAX_COMPUTES[self.tr])]
        services = self.agent_services(node)
        if services:
            argv += ["--services", services]
        numa = node.server_numa or (node.server_mem if node.server_mem.isdigit() else "")
        if numa:
            argv += ["--numa-node", numa]
        if "compute" not in self.roles(node):
            return argv
        argv += ["--computes", "--neon-bin", self.bin(node), "--template", f"{svc}/compute_config_template.json",
                 "--compute-root", self.croot(node)]
        if self.disk_croot(node):
            argv += ["--disk-compute-root", self.disk_croot(node)]
        if self.uds:
            argv += ["--sock-dir", self.run_dir, "--pageserver", f"{self.run_dir}:{P['ps_pg']}",
                     "--safekeepers", f"{self.run_dir}:{P['sk_pg']}"]
        else:
            argv += ["--host", self.compute_host(node),
                     "--pageserver", f"{self.pss[0].addr_from(node)}:{P['ps_pg']}",
                     "--safekeepers", ",".join(f"{s.addr_from(node)}:{P['sk_pg']}" for s in self.sks)]
        if node.name in self.n["compute_agents"]:
            argv += ["--shared-buffers", w["shared_buffers"], "--lfc-mb", str(w["lfc_mb"])]
        else:   # seeding-only node: big shared buffers, no LFC, no statement log (exp 4: gigabytes of log)
            argv += ["--shared-buffers", w["seed_shared_buffers"], "--lfc-mb", "0", "--no-statement-log"]
        argv += ["--lfc-chunk", str(w["lfc_chunk_pages"]), "--eic", str(w["eic"])]
        if node.local:
            argv += ["--disk-guard-gb", str(LOCAL_DISK_GUARD_GB)]
        return argv

    def stop(self) -> None:
        # the agents first: each stops its computes
        for n in self.agent_nodes():
            self.r.run(n, self.curl(n, "agent", "agent", n, "/computes", "-s -m 600 -X DELETE") + " >/dev/null",
                       check=False)
        for svc, node, _ in reversed(self.placement()):
            if node.local:
                self.r.stop_local(self.run_dir, svc, signal.SIGINT if svc == "storcon-pg" else signal.SIGTERM)
            else:
                self.r.stop_unit(node, f"neon-{svc}")
        for n in self.agent_nodes():
            if "compute" not in self.roles(n):
                continue
            if n.local:
                self.kill_compute_leftovers(n)
            # SysV segments of computes that were killed (a killed agent leaves its computes behind)
            self.r.run(n, "ipcs -m | awk -v u=$(id -un) '$3==u && $6==0 {print $2}' | xargs -r -n1 ipcrm -m",
                       check=False)
        log("stopped")

    def kill_compute_leftovers(self, node: Node) -> None:
        """compute_ctl and Postgres of this user left by a killed agent. A /proc scan, not pkill -f: pkill from a
        shell whose command line holds the pattern kills that shell (exp 11)."""
        cc, croot = f"{self.bin(node)}/compute_ctl", self.croot(node) + "/"
        if self.dry:
            print(f"[{node.name}] SIGTERM this user's processes running {cc} or under {croot}")
            return
        for pid in os.listdir("/proc"):
            if not pid.isdigit() or int(pid) == os.getpid():
                continue
            try:
                if os.stat(f"/proc/{pid}").st_uid != os.getuid():
                    continue
                argv = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            except OSError:
                continue
            args = [a.decode(errors="replace") for a in argv if a]
            try:   # a Postgres child left by its postmaster shows only "postgres: checkpointer"; its cwd tells
                cwd = os.readlink(f"/proc/{pid}/cwd").removesuffix(" (deleted)") + "/"
            except OSError:
                cwd = ""
            if cwd.startswith(croot):
                sig = signal.SIGKILL       # its data directory is gone; it cannot shut down cleanly
            elif args and (args[0] == cc or any(a.startswith(croot) for a in args)):
                sig = signal.SIGTERM
            else:
                continue
            try:
                os.kill(int(pid), sig)
            except ProcessLookupError:
                pass

    def status(self) -> None:
        print(f"{self.t.name}: Neon over {self.tr}; state_dir {self.state}")
        by_node: dict[str, list[str]] = {}
        for svc, node, _ in self.placement():
            by_node.setdefault(node.name, []).append(svc)
        for name, svcs in by_node.items():
            node = self.t.node(name)
            if node.local:
                for s in svcs:
                    pid = Runner.local_pid(self.run_dir, s)
                    if not pid:
                        print(f"  {name:8} {s:11} not running")
                        continue
                    st = {}
                    try:
                        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
                            k, _, v = line.partition(":")
                            st[k] = v.strip()
                    except OSError:
                        pass
                    rss = int(st.get("VmRSS", "0 kB").split()[0]) // 1024
                    print(f"  {name:8} {s:11} pid {pid:<8} cpus {st.get('Cpus_allowed_list', '?'):14} "
                          f"rss {rss:6} MB threads {st.get('Threads', '?')}")
            else:
                units = " ".join(f"neon-{s}" for s in svcs)
                cmd = (f"for u in {units}; do s=$(systemctl is-active $u); p=$(systemctl show -p MainPID --value $u); "
                       "a=$( [ \"$p\" != 0 ] && taskset -pc $p 2>/dev/null | awk '{print $NF}'); "
                       f"printf '  {name:8} %-16s %-9s pid %-8s cpus %s\\n' $u $s $p \"$a\"; done; "
                       f"findmnt -no TARGET,SOURCE,FSTYPE,SIZE {self.x(node, node.data_root)} "
                       + (os.path.dirname(self.croot(node)) if node.tmpfs_size else "")
                       + f" 2>/dev/null | sed 's/^/  {name:8} mount /' || true")
                r = self.r.run(node, cmd, check=False, capture=not self.dry)
                if r is not None and not self.dry:
                    print(r.stdout.rstrip())
        sc = self.sc
        nodes = self.r.out(sc, self.curl(sc, "storcon", "storcon", sc, "/control/v1/node", "-s -m 10"), check=False)
        if nodes:
            try:
                for e in json.loads(nodes):
                    print(f"  storcon node {e.get('id')}: {e.get('listen_http_addr')} {e.get('availability')} "
                          f"{e.get('scheduling')}")
            except ValueError:
                print(f"  storcon nodes: {nodes}")
        for n in self.agent_nodes():
            if "compute" not in self.roles(n):
                continue
            s = self.r.out(n, self.curl(n, "agent", "agent", n, "/stats", "-s -m 30"), check=False)
            if s:
                try:
                    j = json.loads(s)
                    print(f"  {n.name:8} computes {j.get('computes')} (running {j.get('running')}), "
                          f"rss {j.get('rss_mb', 0):.0f} MB, available {j.get('mem_available_mb', 0):.0f} MB")
                except ValueError:
                    print(f"  {n.name:8} agent: {s}")
        sha = f"{self.state}/code/bin_sha256.txt"
        if Path(sha).exists() or self.dry:
            r = self.r.run(self.coord, f"cd {self.stage} && sha256sum -c --quiet {sha} >/dev/null 2>&1", check=False)
            if not self.dry:
                print(f"  binary snapshot: {'MATCH' if r.returncode == 0 else 'MISMATCH'} ({self.stage})")
        seed = read_seed(self.t, "neon")
        print(f"  seed: {('tenant ' + seed['tenant_id'] + ' W=' + str(seed.get('W'))) if seed else 'none'}")

    def wipe(self, yes: bool) -> None:
        if not yes and not self.dry:
            die("wipe deletes the storage, safekeeper WAL, storcon database, computes and the seed: pass --yes")
        self.stop()
        for n in self.pss[1:]:      # NFS mounts of the first pageserver's remote storage: unmount, do not delete
            mnt = self.d(n, "ps-remote")
            self.r.run(n, f"if mountpoint -q {mnt}; then sudo -n umount {mnt}; fi; "
                          f"sudo -n sed -i '\\| {mnt} nfs |d' /etc/fstab", check=False)
        for n in self.server_nodes():
            r = self.roles(n)
            dirs = []
            if "pageserver" in r:
                dirs += [self.d(n, "ps"), self.d(n, "ps-remote")]
            if "safekeeper" in r:
                dirs += [self.d(n, "sk"), self.d(n, "sk-remote")]
            if "storcon" in r:
                dirs.append(self.d(n, "storcon_db"))
            if "compute" in r:
                dirs += [self.croot(n)] + ([self.disk_croot(n)] if self.disk_croot(n) else [])
            if dirs:
                self.r.run(n, "rm -rf " + " ".join(dirs))
        files = " ".join(f"{self.state}/{f}" for f in ("neon_seed.json", "neon.env", "neon_seeding.env",
                                                       "neon_shard_split.json"))
        for n in {x.name: x for x in [self.coord, self.client, self.seed_node]}.values():
            if n is self.coord or not n.local:
                self.r.run(n, f"rm -f {files}", check=False)
        log("wiped (agent archives kept: pull-archive, then delete <neon_root>/archive by hand)")

    # ---------------------------------------------------------------- neon_ctl on a node
    def deploy(self, node: Node) -> None:
        """deploy-client logic: RemoveIPC=no (the runner's semaphores), repo and venv on the node, its state_dir."""
        if node.local:
            return
        self.remove_ipc_no(node)
        self.r.deploy_repo(node, REPO, REMOTE_REPO)

    def ctl(self, node: Node, env: dict, env_file: str, args: list) -> None:
        """scripts/setup/neon_ctl.py on the node with an environment file for that node's view."""
        text = env_text(env)
        self.write_coord(env_file, text)
        if node.local:
            bind = " ".join(q(a) for a in Runner.bind_prefix(node, "client"))
            cmd = f"cd {REPO} && {bind} uv run python scripts/setup/neon_ctl.py --env {env_file} "
        else:
            self.ensure_state_dir(node)
            self.r.put(node, env_file, text, mode=0o600)
            cmd = (f"export PATH=$HOME/.local/bin:$PATH && cd {REMOTE_REPO} && "
                   f"uv run python scripts/setup/neon_ctl.py --env {env_file} ")
        self.r.run(node, cmd + " ".join(q(a) for a in args))

    def need_seed(self) -> dict:
        seed = read_seed(self.t, "neon")
        if not seed:
            if self.dry:
                log("(no seed yet: placeholder ids in the dry run)")
                return {"tenant_id": "<tenant>", "seed_timeline_id": "<seed timeline>"}
            die(f"no {self.state}/neon_seed.json: run seed first")
        return seed

    def seed(self, scale: int | None, deploy: bool) -> None:
        if read_seed(self.t, "neon"):
            die(f"{self.state}/neon_seed.json exists: the deployment is seeded (wipe first to reseed)")
        node, w = self.seed_node, self.w
        out = f"{self.state}/neon_seed.json"
        if deploy:
            self.deploy(node)
        env = self.t.neon_env(node.name, None, seeding=True)
        self.ctl(node, env, f"{self.state}/neon_seeding.env",
                 ["seed", "--scale", scale or w["W"], "--shared-buffers", w["seed_shared_buffers"],
                  "--pageserver-node", 1, "--out", out])
        if not node.local:
            self.to_file(node, f"cat {out}", out)
        self.env_init(None)

    def env_init(self, view: str | None, out: str | None = None) -> None:
        view = view or self.client.name
        out = out or (f"{self.state}/neon.env" if view == self.client.name else f"{self.state}/neon.{view}.env")
        if self.dry:
            self.write_coord(out, env_text(self.t.neon_env(view, read_seed(self.t, "neon"))))
            return
        print(write_env(self.t, "neon", view, out))

    def shard_split(self, shards: int) -> None:
        seed = self.need_seed()
        if self.uds:
            die("the single-host (uds) deployment has one pageserver")
        if seed.get("ps_opts"):
            die("the tenant is split already (neon_seed.json has ps_opts)")
        if not 2 <= shards <= len(self.pss):
            die(f"--shards must be 2..{len(self.pss)} (one pageserver per shard; [neon] pageservers)")
        stripe = int(self.w["stripe_pages"])
        log("warning: the split is one-way; runs on one pageserver need a new seed afterwards")
        node = self.seed_node
        out = f"{self.state}/neon_shard_split.json"
        self.ctl(node, self.t.neon_env(node.name, seed, seeding=True), f"{self.state}/neon_seeding.env",
                 ["shard-split", "--shards", shards, "--stripe-pages", stripe, "--out", out])
        if not node.local:
            self.to_file(node, f"cat {out}", out)
        if self.dry:
            print("(then: neon_seed.json gets ps_opts = the shards' pageservers as the compute agents see them, "
                  "in shard order, and the stripe; env-init)")
            return
        split = json.loads(Path(out).read_text())
        shards_after = sorted(split["after"]["shards"], key=lambda s: s["shard_id"])
        views = {self.pss[s["node_id"] - 1].addr_from(self.t.node(a)) for s in shards_after
                 for a in self.n["compute_agents"]}
        if len(views) > len(shards_after):
            log("warning: compute agents see the pageservers on different addresses; ps_opts uses the first "
                "agent's view")
        viewer = self.t.node(self.n["compute_agents"][0])
        seed["ps_opts"] = {"pageservers": [f"{self.pss[s['node_id'] - 1].addr_from(viewer)}:{self.t.ports['ps_pg']}"
                                           for s in shards_after],
                           "stripe_size": split["after"]["shard_params"]["stripe_size"]}
        seed["shard_split"] = {"shards": shards, "stripe_pages": stripe, "seconds": split.get("seconds"),
                               "placement": {s["shard_id"]: s["node_id"] for s in shards_after}}
        Path(f"{self.state}/neon_seed.json").write_text(json.dumps(seed, indent=2))
        self.env_init(None)

    def client_ctl(self, args: list, fetch: tuple[str, str] | None = None) -> None:
        """neon_ctl on the client node with neon.env (client view); fetch = (remote file, local file)."""
        c = self.client
        self.ctl(c, self.t.neon_env(c.name, self.need_seed()), f"{self.state}/neon.env", args)
        if fetch and not c.local:
            self.to_file(c, f"cat {fetch[0]}", fetch[1])

    def metrics(self, file: str) -> None:
        c, t = self.client, self.t
        targets = [f"pageserver{'' if k == 1 else k}={t.http_url('ps-http', 'ps_http', n, c)}"
                   for k, n in enumerate(self.pss, 1)]
        targets += [f"safekeeper{k}={t.http_url('sk-http', 'sk_http', n, c)}" for k, n in enumerate(self.sks, 1)]
        targets.append(f"storcon={t.http_url('storcon', 'storcon', self.sc, c)}")
        dst = os.path.abspath(file)
        tmp = dst if c.local else f"{self.state}/metrics.txt"
        self.client_ctl(["metrics", tmp, "--targets", ",".join(targets)], fetch=(tmp, dst))

    def clock_check(self) -> None:
        c, out = self.client, f"{self.state}/clock_offsets.json"
        agents = []
        for n in self.agent_nodes():
            url = (f"http+unix://{self.t.sock('agent').encode().hex()}" if self.uds
                   else f"http://{n.addr_from(c)}:{self.t.ports['agent']}")
            agents.append(f"{n.name}={url}")
        self.client_ctl(["clock-check", "--agents", ",".join(agents), "--out", out], fetch=(out, out))

    def deploy_client(self) -> None:
        c = self.client
        if c.local:
            log(f"{c.name} is local: repo and {self.state} are already here")
            return
        self.deploy(c)
        self.ensure_state_dir(c)
        self.env_init(None)
        for f in ("neon.env", "neon_seed.json"):
            p = f"{self.state}/{f}"
            if Path(p).exists():
                self.r.put(c, p, Path(p).read_text(), mode=0o600)
            elif self.dry:
                print(f"[{c.name}] write {p} (copy of the coordinator's)")
            else:
                log(f"no {p} yet (seed first)")

    def pull_archive(self, dest: str) -> None:
        """Exp 5's pull_data.sh: everything but the 1 s samples by rsync; samples gzipped on the way (plain they are
        gigabytes per node)."""
        for n in self.agent_nodes():
            arch = self.d(n, "archive")
            dst = f"{os.path.abspath(dest)}/{n.name}"
            try:
                self.r.fetch(n, f"{arch}/", dst, extra=["--exclude", "samples.jsonl"])
                self.to_file(n, f"gzip -c {arch}/samples.jsonl", f"{dst}/samples.jsonl.gz")
            except subprocess.CalledProcessError as e:
                log(f"{n.name}: archive not (fully) pulled: {e}")


def main(argv=None) -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dry-run", action="store_true", help="print commands, files and launches; run nothing")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
                                 parents=[common])
    ap.add_argument("topo", help="topology TOML (scripts/setup/topologies/)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("install", parents=[common])
    p.add_argument("--no-fanout", action="store_true", help="rsync binaries to every node from here")
    for c in ("start", "stop", "restart", "status", "purge", "smoke", "clock-check", "deploy-client"):
        sub.add_parser(c, parents=[common])
    p = sub.add_parser("wipe", parents=[common])
    p.add_argument("--yes", action="store_true")
    p = sub.add_parser("seed", parents=[common])
    p.add_argument("--scale", type=int, help="warehouses (default workload.W)")
    p.add_argument("--no-deploy", action="store_true", help="the seeding node already has the repo")
    p = sub.add_parser("env-init", parents=[common])
    p.add_argument("--view", help="node whose view of the cluster to write (default: the client)")
    p.add_argument("--out")
    p = sub.add_parser("shard-split", parents=[common])
    p.add_argument("--shards", type=int, required=True)
    p = sub.add_parser("metrics", parents=[common])
    p.add_argument("file")
    p = sub.add_parser("pull-archive", parents=[common])
    p.add_argument("dest")
    a = ap.parse_args(argv)
    try:
        nn = Neon(Topo(a.topo), a.dry_run)
        if a.dry_run:
            print(f"# dry run: neon.py {a.topo} {a.cmd} (nothing is executed)")
        cmd = a.cmd
        if cmd == "install":
            nn.install(fanout=not a.no_fanout)
        elif cmd == "start":
            nn.start()
        elif cmd == "stop":
            nn.stop()
        elif cmd == "restart":
            nn.stop()
            nn.start()
        elif cmd == "status":
            nn.status()
        elif cmd == "wipe":
            nn.wipe(a.yes)
        elif cmd == "seed":
            nn.seed(a.scale, not a.no_deploy)
        elif cmd == "env-init":
            nn.env_init(a.view, a.out)
        elif cmd == "shard-split":
            nn.shard_split(a.shards)
        elif cmd == "purge":
            nn.client_ctl(["purge"])
        elif cmd == "smoke":
            nn.client_ctl(["smoke"])
        elif cmd == "metrics":
            nn.metrics(a.file)
        elif cmd == "clock-check":
            nn.clock_check()
        elif cmd == "deploy-client":
            nn.deploy_client()
        elif cmd == "pull-archive":
            nn.pull_archive(a.dest)
    except TopoError as e:
        print(f"topology error: {e}", file=sys.stderr)
        return 2
    except (RuntimeError, subprocess.CalledProcessError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
