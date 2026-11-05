#!/usr/bin/env python3
"""Deploy and operate the pgcopy backend (experiment 13) as a topology's [pgcopy] section describes it.

Every node of [pgcopy] (seed + hosts) runs two systemd units: pgcopy-agent (scripts/setup/pgcopy_node_agent.py:
instances, copies) and dbfork-cpu-agent (scripts/setup/cpu_agent.py), both listening on the node's LAN address only.
The coordinator may be outside the cluster (experiment 13: pc20 at UMass, ssh to the nodes' public names): it never
calls an agent itself. Everything that talks to the agents runs on a cluster node through scripts/setup/pgcopy_ctl.py
(on this machine when it is a node of the topology, else over ssh on the seed node).

  pgcopy.py TOPO install            PGDG PostgreSQL 17 (no default cluster), kernel.shmmni, RemoveIPC=no, the
                                    cluster-internal ssh key (~/.ssh/cluster_ed25519 on every node, LAN only), agent
                                    files; on every node in parallel
  pgcopy.py TOPO start | stop | restart | status
  pgcopy.py TOPO seed [--scale W]   the seed instance on the seed node, loaded with CH-benCHmark W ->
                                    <state_dir>/pgcopy_seed.json, then env-init
  pgcopy.py TOPO env-init           <state_dir>/pgcopy.env (the client's view) and pgcopy.<node>.env for every node
  pgcopy.py TOPO deploy-client [--nodes n1,n2]
                                    repo + uv venv on every host (default), its own view as <state_dir>/pgcopy.env,
                                    pgcopy_seed.json
  pgcopy.py TOPO purge | smoke | clock-check
  pgcopy.py TOPO wipe --yes         stop, then delete every instance (the seed too) and the state files
  pgcopy.py TOPO pull-archive DEST  every agent's archive (forks.jsonl, failures, kept logs) -> DEST/<node>/

Every command takes --dry-run (print commands and files, run nothing).
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))

from nodes import Runner, die, log  # noqa: E402
from topo import Node, Topo, TopoError, env_text, read_seed, write_env  # noqa: E402

REMOTE_REPO = "~/db-fork"
AGENT_UNIT = "pgcopy-agent"
SVC_FILES = ("pgcopy_node_agent.py", "cpu_agent.py")
SHMMNI = 32768
APT_PACKAGES = ("rsync", "curl", "ca-certificates", "postgresql-common")
CLUSTER_KEY = "~/.ssh/cluster_ed25519"
PGDG_KEY_URL = "https://www.postgresql.org/media/keys/ACCC4CF8.asc"


def q(s) -> str:
    return shlex.quote(str(s))


class PgCopy:
    def __init__(self, topo: Topo, dry: bool):
        if not topo.pgcopy:
            raise TopoError("topology has no [pgcopy] section")
        self.t, self.p, self.dry = topo, topo.pgcopy, dry
        self.r = Runner(topo, dry)
        self.coord = Node("coordinator", {}, topo)
        self.state = topo.state_dir
        self.seed_node = topo.node(self.p["seed"])
        self.hosts = [topo.node(x) for x in self.p["hosts"]]
        self.client = topo.node(self.p["client"])
        self.nodes = topo.pgcopy_nodes()
        self.w = topo.wl("pgcopy")
        self.pgv = int(self.p["pg_version"])
        self._home: dict[str, str] = {}

    # ---------------------------------------------------------------- paths
    def home(self, node: Node) -> str:
        if node.name not in self._home:
            self._home[node.name] = f"/users/{self.t.ssh_user}" if self.dry else self.r.out(node, "echo $HOME")
        return self._home[node.name]

    def x(self, node: Node, path: str) -> str:
        return self.home(node) + path[1:] if path.startswith("~") else path

    def root(self, node: Node) -> str:
        return self.x(node, self.t.pgcopy_root(node))

    def d(self, node: Node, sub: str) -> str:
        return f"{self.root(node)}/{sub}"

    def here(self) -> Node | None:
        """The topology node this machine is (its LAN or public address is configured here), if any."""
        for n in self.nodes:
            if n.is_here():
                return n
        return None

    def parallel(self, fn, items) -> list:
        items = list(items)
        if self.dry or len(items) < 2:
            return [fn(it) for it in items]
        with ThreadPoolExecutor(min(16, len(items))) as ex:
            return list(ex.map(fn, items))

    def write_coord(self, path: str, text: str) -> None:
        if self.dry:
            print(f"[coordinator] write {path}:\n    " + text.rstrip("\n").replace("\n", "\n    "))
            return
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text)

    def to_file(self, node: Node, cmd: str, dst: str) -> None:
        argv = self.r.ssh_argv(node) + ["bash -c " + q(cmd)]
        if self.dry:
            print(f"[{node.name}] $ {cmd} > (coordinator) {dst}")
            return
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        with open(dst + ".tmp", "wb") as f:
            subprocess.run(argv, stdout=f, check=True)
        os.replace(dst + ".tmp", dst)

    def ensure_state_dir(self, node: Node) -> None:
        s = self.state
        self.r.run(node, f"mkdir -p {s} 2>/dev/null || {{ sudo -n mkdir -p {s} && "
                         f"sudo -n chown $(id -u):$(id -g) {s}; }}")

    # ---------------------------------------------------------------- install
    def install(self) -> None:
        self.cluster_key()
        log(f"prepare {len(self.nodes)} nodes (PostgreSQL {self.pgv} from PGDG, sysctl, logind, ssh key, agent files)")
        self.parallel(self.prepare, self.nodes)
        log("installed")

    def cluster_key(self) -> None:
        """A key only the nodes use to reach each other over the LAN (the multi-node driver, ssh to the own node);
        made here once, never the coordinator's own key."""
        k = os.path.expanduser(CLUSTER_KEY)
        if self.dry:
            print(f"[coordinator] ssh-keygen -t ed25519 -N '' -f {k} (unless it exists)")
            return
        if not Path(k).exists():
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", f"{self.t.name} cluster-internal",
                            "-f", k], check=True)
            log(f"made {k}")

    def prepare(self, node: Node) -> None:
        dr = self.x(node, node.data_root)
        v = self.pgv
        pk = " ".join(APT_PACKAGES)
        lines = [
            "set -euo pipefail",
            "export DEBIAN_FRONTEND=noninteractive",
            # no default cluster: the agent runs every instance (a packaged 17/main would listen on 5432)
            "sudo -n mkdir -p /etc/postgresql-common/createcluster.d",
            "echo 'create_main_cluster = false' | sudo -n tee /etc/postgresql-common/createcluster.d/"
            "90-no-main.conf >/dev/null",
            f"need=''; for p in {pk}; do dpkg -s $p >/dev/null 2>&1 || need=\"$need $p\"; done",
            "if [ -n \"$need\" ]; then sudo -n apt-get update -qq >/dev/null 2>&1 || true; "
            "sudo -n apt-get install -y -qq $need >/dev/null; fi",
            f"if ! dpkg -s postgresql-{v} >/dev/null 2>&1; then",
            "  sudo -n install -d /usr/share/postgresql-common/pgdg",
            f"  sudo -n curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc {PGDG_KEY_URL}",
            "  echo \"deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] "
            "https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main\" | "
            "sudo -n tee /etc/apt/sources.list.d/pgdg.list >/dev/null",
            "  sudo -n apt-get update -qq >/dev/null 2>&1",
            f"  sudo -n apt-get install -y -qq postgresql-{v} postgresql-client-{v} >/dev/null",
            "fi",
            f"for c in $(pg_lsclusters -h 2>/dev/null | awk '{{print $1\"/\"$2}}'); do "
            "sudo -n pg_dropcluster --stop ${c%/*} ${c#*/}; done",
            "sudo -n systemctl disable --now postgresql >/dev/null 2>&1 || true",
            f"echo 'kernel.shmmni = {SHMMNI}' | sudo -n tee /etc/sysctl.d/90-pgcopy.conf >/dev/null",
            "sudo -n sysctl -q -p /etc/sysctl.d/90-pgcopy.conf",
            "grep -q '^RemoveIPC=no' /etc/systemd/logind.conf || { echo RemoveIPC=no | sudo -n tee -a "
            "/etc/systemd/logind.conf >/dev/null && sudo -n systemctl restart systemd-logind; }",
            f"sudo -n chown $(id -u):$(id -g) {dr}",
            f"mkdir -p {self.d(node, 'svc')} {self.d(node, 'instances')} {self.d(node, 'archive')}",
            f"echo \"  $(hostname -s): $(/usr/lib/postgresql/{v}/bin/postgres --version), "
            "shmmni $(sysctl -n kernel.shmmni), $(df -h --output=avail " + dr + " | tail -1) free\"",
        ]
        self.r.run(node, "\n".join(lines))
        self.r.run(node, f"mkdir -p {self.state} 2>/dev/null || {{ sudo -n mkdir -p {self.state} && "
                         f"sudo -n chown $(id -u):$(id -g) {self.state}; }}")
        for f in SVC_FILES:
            self.r.rsync(node, str(HERE / f), f"{self.d(node, 'svc')}/{f}")
        self.install_key(node)

    def install_key(self, node: Node) -> None:
        k = os.path.expanduser(CLUSTER_KEY)
        if self.dry:
            print(f"[{node.name}] install {CLUSTER_KEY}(.pub), authorized_keys line, ~/.ssh/config for the LAN")
            return
        priv, pub = Path(k).read_text(), Path(k + ".pub").read_text().strip()
        self.r.put(node, CLUSTER_KEY, priv, mode=0o600)
        self.r.put(node, CLUSTER_KEY + ".pub", pub + "\n", mode=0o644)
        lan = ".".join(self.seed_node.lan.split(".")[:3]) + ".*"
        cfg = (f"# pgcopy cluster-internal key ({self.t.name})\nHost {lan}\n    IdentityFile {CLUSTER_KEY}\n"
               "    StrictHostKeyChecking accept-new\n    BatchMode yes\n")
        self.r.run(node, f"touch ~/.ssh/authorized_keys ~/.ssh/config && chmod 600 ~/.ssh/authorized_keys ~/.ssh/config"
                         f" && (grep -qF {q(pub)} ~/.ssh/authorized_keys || echo {q(pub)} >> ~/.ssh/authorized_keys)"
                         f" && (grep -qF {q('Host ' + lan)} ~/.ssh/config || printf %s {q(cfg)} >> ~/.ssh/config)")

    # ---------------------------------------------------------------- services
    def agent_argv(self, node: Node) -> list[str]:
        port = self.t.ports["pgcopy_agent"]
        return ["python3", f"{self.d(node, 'svc')}/pgcopy_node_agent.py", "--listen", f"{node.lan}:{port}",
                "--host", node.lan, "--pg-bin", f"/usr/lib/postgresql/{self.pgv}/bin",
                "--instance-root", self.d(node, "instances"), "--archive-dir", self.d(node, "archive"),
                "--protect", "seed", "--shared-buffers", str(self.w["shared_buffers"]),
                "--pg-port-base", str(self.t.ports["pgcopy_pg"]), "--disk-guard-gb", str(self.w["disk_guard_gb"]),
                "--fstrim"]

    def start(self) -> None:
        def one(node: Node) -> None:
            svc = self.d(node, "svc")
            self.r.cpu_agent_service(node, svc, bind=node.lan)
            self.r.systemd_unit(node, AGENT_UNIT, self.agent_argv(node), wrapper_dir=svc, which="client")
            port = self.t.ports["pgcopy_agent"]
            self.r.run(node, f"for i in $(seq 120); do curl -sf http://{node.lan}:{port}/time >/dev/null && "
                             f"curl -sf 'http://{node.lan}:{self.t.ports['cpu_agent']}/cpu?set=server' >/dev/null "
                             f"&& exit 0; sleep 1; done; echo 'agents did not answer' >&2; exit 1")
        self.parallel(one, self.nodes)
        log(f"agents up on {len(self.nodes)} nodes")

    def stop(self) -> None:
        self.parallel(lambda n: self.r.stop_unit(n, AGENT_UNIT), self.nodes)
        log("pgcopy agents stopped (their instances with them; the seed is restarted by the next start)")

    def status(self) -> None:
        def one(node: Node) -> str:
            return self.r.out(node, f"echo \"{node.name}: agent $(systemctl is-active {AGENT_UNIT}), cpu "
                                    f"$(systemctl is-active {self.r.CPU_UNIT}), $(pgrep -u $(id -u) -x postgres | wc -l)"
                                    f" postgres processes, $(df -h --output=avail {self.x(node, node.data_root)} | "
                                    "tail -1) free\"", check=False)
        for line in self.parallel(one, self.nodes):
            print(line)
        if read_seed(self.t, "pgcopy") or self.dry:
            self.ctl(["status"])

    def wipe(self, yes: bool) -> None:
        if not yes:
            die("wipe deletes every instance, the seed too: pass --yes")
        self.stop()
        self.parallel(lambda n: self.r.run(n, f"rm -rf {self.d(n, 'instances')} && mkdir -p {self.d(n, 'instances')}"),
                      self.nodes)
        files = " ".join(f"{self.state}/{f}" for f in ("pgcopy_seed.json", "pgcopy.env", "pgcopy_seeding.env"))
        self.r.run(self.coord, f"rm -f {files} {self.state}/pgcopy.n*.env", check=False)
        self.parallel(lambda n: self.r.run(n, f"rm -f {files}", check=False), self.nodes)
        log("wiped (agent archives kept: pull-archive, then delete <data_root>/pgcopy/archive by hand)")

    # ---------------------------------------------------------------- ctl on a node
    def deploy(self, node: Node) -> None:
        self.r.deploy_repo(node, REPO, REMOTE_REPO)
        self.ensure_state_dir(node)

    def ctl(self, args: list, node: Node | None = None, env: dict | None = None, env_file: str | None = None) -> None:
        """scripts/setup/pgcopy_ctl.py on a cluster node: here if this machine is one, else the seed node over ssh."""
        me = self.here()
        node = node or me or self.seed_node
        if env is None:
            env = self.t.pgcopy_env(node.name)
        env_file = env_file or f"{self.state}/pgcopy.ctl.{node.name}.env"
        text = env_text(env)
        argv = " ".join(q(a) for a in args)
        if me is not None and node.name == me.name:
            self.write_coord(env_file, text)
            cmd = f"cd {REPO} && uv run python scripts/setup/pgcopy_ctl.py --env {env_file} {argv}"
            if self.dry:
                print(f"[{node.name}, here] $ {cmd}")
                return
            subprocess.run(["bash", "-c", f"export PATH=$HOME/.local/bin:$PATH && {cmd}"], check=True)
            return
        self.r.put(node, env_file, text, mode=0o600)
        self.r.run(node, f"export PATH=$HOME/.local/bin:$PATH && cd {REMOTE_REPO} && "
                         f"uv run python scripts/setup/pgcopy_ctl.py --env {env_file} {argv}")

    def seed(self, scale: int | None, deploy: bool) -> None:
        if read_seed(self.t, "pgcopy"):
            die(f"{self.state}/pgcopy_seed.json exists: the deployment is seeded (wipe first to reseed)")
        node, out = self.seed_node, f"{self.state}/pgcopy_seed.json"
        if deploy and not node.is_here():
            self.deploy(node)
        env = self.t.pgcopy_env(node.name, seeding=True)
        self.ctl(["seed", "--scale", scale or self.w["W"], "--shared-buffers", self.w["seed_shared_buffers"],
                  "--out", out], node=node, env=env, env_file=f"{self.state}/pgcopy_seeding.env")
        if not node.is_here():
            self.to_file(node, f"cat {out}", out)
        self.env_init()

    def env_init(self) -> None:
        print(write_env(self.t, "pgcopy", None, f"{self.state}/pgcopy.env") if not self.dry
              else f"(write {self.state}/pgcopy.env)")
        for n in self.nodes:
            p = f"{self.state}/pgcopy.{n.name}.env"
            self.write_coord(p, env_text(self.t.pgcopy_env(n.name)))

    def deploy_client(self, names: list[str] | None) -> None:
        """Every host is a client node: repo + venv, its own view as pgcopy.env, the seed record."""
        self.env_init()
        seed = Path(f"{self.state}/pgcopy_seed.json")
        targets = [self.t.node(x) for x in names] if names else self.hosts

        def one(node: Node) -> None:
            if not node.is_here():
                self.deploy(node)
            view = env_text(self.t.pgcopy_env(node.name))
            self.r.put(node, f"{self.state}/pgcopy.env", view, mode=0o600)
            if seed.exists():
                self.r.put(node, str(seed), seed.read_text(), mode=0o600)
            elif self.dry:
                print(f"[{node.name}] write {seed} (copy of the coordinator's)")
        self.parallel(one, targets)
        if not seed.exists() and not self.dry:
            log(f"no {seed} yet (seed first, then deploy-client again)")
        log(f"deployed to {len(targets)} nodes")

    def pull_archive(self, dest: str) -> None:
        for n in self.nodes:
            dst = f"{os.path.abspath(dest)}/{n.name}"
            try:
                self.r.fetch(n, f"{self.d(n, 'archive')}/", dst)
            except subprocess.CalledProcessError as e:
                log(f"{n.name}: archive not (fully) pulled: {e}")


def main(argv=None) -> int:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--dry-run", action="store_true", help="print commands and files; run nothing")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
                                 parents=[common])
    ap.add_argument("topo")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for c in ("install", "start", "stop", "restart", "status", "env-init", "purge", "smoke", "clock-check"):
        sub.add_parser(c, parents=[common])
    p = sub.add_parser("seed", parents=[common])
    p.add_argument("--scale", type=int)
    p.add_argument("--no-deploy", action="store_true")
    p = sub.add_parser("deploy-client", parents=[common])
    p.add_argument("--nodes", help="comma list (default: every host)")
    p = sub.add_parser("wipe", parents=[common])
    p.add_argument("--yes", action="store_true")
    p = sub.add_parser("pull-archive", parents=[common])
    p.add_argument("dest")
    a = ap.parse_args(argv)
    try:
        pc = PgCopy(Topo(a.topo), a.dry_run)
        if a.dry_run:
            print(f"# dry run: pgcopy.py {a.topo} {a.cmd} (nothing is executed)")
        c = a.cmd
        if c == "install":
            pc.install()
        elif c == "start":
            pc.start()
        elif c == "stop":
            pc.stop()
        elif c == "restart":
            pc.stop()
            pc.start()
        elif c == "status":
            pc.status()
        elif c == "seed":
            pc.seed(a.scale, not a.no_deploy)
        elif c == "env-init":
            pc.env_init()
        elif c == "deploy-client":
            pc.deploy_client(a.nodes.split(",") if a.nodes else None)
        elif c in ("purge", "smoke"):
            pc.ctl([c])
        elif c == "clock-check":
            out = f"{pc.state}/clock_offsets.json"
            pc.ctl(["clock-check", "--out", out])
        elif c == "wipe":
            pc.wipe(a.yes)
        elif c == "pull-archive":
            pc.pull_archive(a.dest)
    except TopoError as e:
        print(f"topology error: {e}", file=sys.stderr)
        return 2
    except (RuntimeError, subprocess.CalledProcessError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
