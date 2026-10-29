"""Run setup actions on the nodes of a topology: locally (host "local") or over ssh (stdlib only).

Shared by setup/neon.py and setup/doltgres.py. With dry_run=True nothing is executed: every command, file and unit
that would be written is printed instead (the way to check a CloudLab deployment without a cluster).

Two ways to run a long-lived service:
- local node: `launch_local` starts it with setsid + nohup under taskset/numactl, writes <run_dir>/<name>.pid;
- remote node: `systemd_unit` writes a wrapper script and a unit (User=<ssh user>, LimitNOFILE, KillMode
  control-group, Restart=no) and starts it; `unit_pid` reads its MainPID.
"""
from __future__ import annotations

import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

from topo import Node, Topo

SSH_OPTS = ["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=15",
            "-o", "ServerAliveInterval=30"]


def log(msg: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


class Runner:
    def __init__(self, topo: Topo, dry_run: bool = False):
        self.topo = topo
        self.dry = dry_run

    # ---- commands ----
    def ssh_argv(self, node: Node) -> list[str]:
        argv = ["ssh", *SSH_OPTS]
        if self.topo.ssh_key:
            argv += ["-i", self.topo.ssh_key]
        return argv + [node.ssh_target()]

    def run(self, node: Node, cmd: str, check: bool = True, capture: bool = False, timeout: float | None = None,
            input: str | None = None) -> subprocess.CompletedProcess | None:
        """Run a bash command on the node. Over ssh the command goes base64-free as one bash -c argument, quoted
        once (ssh re-splits its arguments, so never pass argv lists through it)."""
        if self.dry:
            print(f"[{node.name}] $ {cmd}")
            return subprocess.CompletedProcess(cmd, 0, "", "")
        if node.local:
            argv = ["bash", "-c", cmd]
        else:
            argv = self.ssh_argv(node) + ["bash -c " + shlex.quote(cmd)]
        r = subprocess.run(argv, text=True, capture_output=capture, timeout=timeout, input=input)
        if check and r.returncode != 0:
            err = (r.stderr or "").strip() if capture else ""
            raise RuntimeError(f"[{node.name}] command failed ({r.returncode}): {cmd}\n{err}")
        return r

    def out(self, node: Node, cmd: str, check: bool = True, timeout: float | None = None) -> str:
        r = self.run(node, cmd, check=check, capture=True, timeout=timeout)
        return (r.stdout or "").strip() if r else ""

    def put(self, node: Node, path: str, content: str, mode: int = 0o644, sudo: bool = False) -> None:
        """Write a file on the node."""
        if self.dry:
            print(f"[{node.name}] write {path} (mode {mode:o}{', sudo' if sudo else ''}):")
            print("    " + content.rstrip("\n").replace("\n", "\n    "))
            return
        if node.local and not sudo:
            p = Path(os.path.expanduser(path))
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content)
            p.chmod(mode)
            return
        s = "sudo " if sudo else ""
        cmd = (f"{s}mkdir -p $(dirname {path}) && {s}tee {path} >/dev/null && {s}chmod {mode:o} {path}")
        self.run(node, cmd, input=content)

    def rsync(self, node: Node, src: str, dst: str, extra: list[str] | None = None, delete: bool = False) -> None:
        """Copy src (a local path; trailing / copies the contents) to dst on the node."""
        argv = ["rsync", "-a", *(["--delete"] if delete else []), *(extra or [])]
        if node.local:
            argv += [src, os.path.expanduser(dst)]
        else:
            ssh = " ".join(shlex.quote(x) for x in ["ssh", *SSH_OPTS, *(["-i", self.topo.ssh_key]
                                                                        if self.topo.ssh_key else [])])
            argv += ["-e", ssh, src, f"{node.ssh_target()}:{dst}"]
        if self.dry:
            print(f"[{node.name}] $ {' '.join(shlex.quote(a) for a in argv)}")
            return
        if not node.local:
            self.run(node, f"mkdir -p {dst.rstrip('/')}" if src.endswith("/") else f"mkdir -p $(dirname {dst})")
        subprocess.run(argv, check=True)

    def fetch(self, node: Node, src: str, dst: str, extra: list[str] | None = None) -> None:
        """Copy src on the node to the local path dst (rsync; extra: more rsync options, e.g. --exclude)."""
        if node.local:
            argv = ["rsync", "-a", *(extra or []), os.path.expanduser(src), dst]
        else:
            ssh = " ".join(shlex.quote(x) for x in ["ssh", *SSH_OPTS, *(["-i", self.topo.ssh_key]
                                                                        if self.topo.ssh_key else [])])
            argv = ["rsync", "-a", *(extra or []), "-e", ssh, f"{node.ssh_target()}:{src}", dst]
        if self.dry:
            print(f"[{node.name}] $ {' '.join(shlex.quote(a) for a in argv)}")
            return
        Path(dst).mkdir(parents=True, exist_ok=True)
        subprocess.run(argv, check=True)

    # ---- placement ----
    @staticmethod
    def bind_prefix(node: Node, which: str = "server") -> list[str]:
        """taskset/numactl prefix for the node's server or client placement (empty if none is configured).
        taskset, not numactl --physcpubind: numactl rejects CPUs outside its caller's affinity."""
        cpus = node.cpus_csv(which)
        mem = node.server_mem if which == "server" else node.client_mem
        numa = node.server_numa if which == "server" else node.client_numa
        if cpus:
            return ["taskset", "-c", cpus, "numactl", f"--membind={mem}"] if mem else ["taskset", "-c", cpus]
        if numa:
            return ["numactl", f"--cpunodebind={numa}", f"--membind={numa}"]
        return []

    # ---- local daemons ----
    def launch_local(self, node: Node, name: str, argv: list[str], run_dir: str, log_dir: str,
                     env: dict | None = None, which: str = "server") -> None:
        """setsid nohup <bind> argv > log_dir/name.log; pid to run_dir/name.pid."""
        prefix = self.bind_prefix(node, which)
        full = prefix + argv
        envs = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in (env or {}).items())
        logf = f"{log_dir}/{name}.log"
        if self.dry:
            print(f"[{node.name}] launch {name}: {envs} {' '.join(shlex.quote(a) for a in full)} > {logf}")
            return
        Path(run_dir).mkdir(parents=True, exist_ok=True)
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        with open(logf, "ab") as lf:
            p = subprocess.Popen(full, stdout=lf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                 env={**os.environ, **{k: str(v) for k, v in (env or {}).items()}},
                                 start_new_session=True)
        Path(f"{run_dir}/{name}.pid").write_text(f"{p.pid}\n")
        log(f"started {name} pid {p.pid} ({' '.join(prefix) or 'no binding'})")

    @staticmethod
    def local_pid(run_dir: str, name: str) -> int | None:
        try:
            pid = int(Path(f"{run_dir}/{name}.pid").read_text())
        except (OSError, ValueError):
            return None
        return pid if Path(f"/proc/{pid}").exists() else None

    def stop_local(self, run_dir: str, name: str, sig: int = signal.SIGTERM, timeout: float = 60) -> None:
        pid = self.local_pid(run_dir, name)
        if self.dry:
            print(f"[local] stop {name} (pid file {run_dir}/{name}.pid, signal {sig})")
            return
        if not pid:
            return
        try:
            os.killpg(pid, sig)
        except ProcessLookupError:
            return
        except PermissionError:
            os.kill(pid, sig)
        t0 = time.time()
        while time.time() - t0 < timeout and Path(f"/proc/{pid}").exists():
            time.sleep(0.2)
        if Path(f"/proc/{pid}").exists():
            log(f"{name} (pid {pid}) did not stop in {timeout:.0f} s: SIGKILL")
            try:
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        Path(f"{run_dir}/{name}.pid").unlink(missing_ok=True)

    # ---- remote services (systemd) ----
    def systemd_unit(self, node: Node, unit: str, argv: list[str], wrapper_dir: str, env: dict | None = None,
                     which: str = "server", after: str = "network-online.target", start: bool = True) -> None:
        """Write <wrapper_dir>/<unit>.sh (env + exec of the bound command) and /etc/systemd/system/<unit>.service,
        then (re)start it."""
        prefix = self.bind_prefix(node, which)
        lines = ["#!/bin/bash"] + [f"export {k}={shlex.quote(str(v))}" for k, v in (env or {}).items()]
        lines.append("exec " + " ".join(shlex.quote(a) for a in prefix + argv))
        wrapper = f"{wrapper_dir}/{unit}.sh"
        self.put(node, wrapper, "\n".join(lines) + "\n", mode=0o755)
        user = self.topo.ssh_user or "$(id -un)"
        unit_text = f"""[Unit]
Description={unit} (db-fork benchmark)
After={after}

[Service]
User={user}
ExecStart={wrapper}
LimitNOFILE=1048576
LimitNPROC=infinity
TasksMax=infinity
KillMode=control-group
TimeoutStopSec=120
Restart=no

[Install]
WantedBy=multi-user.target
"""
        self.put(node, f"/etc/systemd/system/{unit}.service", unit_text, sudo=True)
        if start:
            self.run(node, f"sudo systemctl daemon-reload && sudo systemctl restart {unit}")

    def unit_pid(self, node: Node, unit: str) -> str:
        return self.out(node, f"systemctl show -p MainPID --value {unit}", check=False)

    def stop_unit(self, node: Node, unit: str) -> None:
        self.run(node, f"sudo systemctl stop {unit} 2>/dev/null || true", check=False)

    # ---- CPU sampling on remote server nodes ----
    CPU_UNIT = "dbfork-cpu-agent"
    # process groups by /proc/<pid>/comm (15 characters at most: storage_controller shows as storage_control);
    # cpu_agent.py files any descendant of the pageserver under walredo first
    CPU_COMM_GROUPS = {"pageserver": "pageserver", "safekeeper": "safekeeper", "storcon": "storage_control",
                       "broker": "storage_broker", "compute_ctl": "compute_ctl", "computes": "postgres",
                       "doltgres": "doltgres"}

    def cpu_agent_service(self, node: Node, svc_dir: str) -> None:
        """One scripts/setup/cpu_agent.py unit per remote node, shared by Neon and Doltgres (they may share a node,
        and the runner's DBFORK_REMOTE_CPU expects ports.cpu_agent): sets server (+ clients when configured), every
        backend's process groups by name, the disk and the server's memory node. Service mode: no CSV. Left running
        by `stop`, it is cheap and the other backend may use it."""
        script = f"{svc_dir}/cpu_agent.py"
        self.rsync(node, str(Path(__file__).with_name("cpu_agent.py")), script)
        server = node.cpus_csv("server") or (f"node:{node.server_numa}" if node.server_numa else "all")
        argv = ["python3", script, "--port", str(self.topo.ports["cpu_agent"]), "--bind", "0.0.0.0",
                "--set", f"server={server}"]
        clients = node.cpus_csv("client") or (f"node:{node.client_numa}" if node.client_numa else "")
        if clients:
            argv += ["--set", f"clients={clients}"]
        for g, comm in self.CPU_COMM_GROUPS.items():
            argv += ["--group-comm", f"{g}={comm}"]
        if node.disk_dev:
            argv += ["--disk", node.disk_dev]
        if node.server_numa:
            argv += ["--numa-node", node.server_numa]
        self.systemd_unit(node, self.CPU_UNIT, argv, wrapper_dir=svc_dir, which="client")

    # ---- waiting ----
    def wait_for(self, what: str, fn, timeout: float = 120, every: float = 1.0) -> None:
        if self.dry:
            print(f"(wait for {what})")
            return
        t0 = time.time()
        while True:
            try:
                if fn():
                    return
            except Exception:
                pass
            if time.time() - t0 > timeout:
                raise RuntimeError(f"timed out after {timeout:.0f} s waiting for {what}")
            time.sleep(every)

    # ---- client deployment (CloudLab: the benchmark runs on the client node) ----
    def deploy_repo(self, node: Node, repo: Path, dst: str = "~/db-fork") -> None:
        """rsync the repo (without data and secrets), then uv sync + build_protos on the node."""
        if node.local:
            log(f"{node.name}: local node, the repo is already here")
            return
        excl = ["--exclude", ".git", "--exclude", ".venv", "--exclude", "experiment/*/data", "--exclude", ".env",
                "--exclude", "*_pb2.py", "--exclude", "__pycache__", "--exclude", "run_stats*"]
        self.rsync(node, f"{repo}/", dst, extra=excl)
        self.run(node, "command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh")
        # build_protos.py only warns without protoc, and the *_pb2.py files are excluded above: install protoc 32
        # (the protobuf 6.x runtime pinned in pyproject.toml), as experiment 1's setup did.
        self.run(node, "~/.local/bin/protoc --version 2>/dev/null | grep -q 'libprotoc 32' || { "
                       "mkdir -p ~/.local/bin ~/.local/include /tmp/protoc32 && "
                       "curl -sSL -o /tmp/protoc32.zip https://github.com/protocolbuffers/protobuf/releases/download/"
                       "v32.1/protoc-32.1-linux-x86_64.zip && "
                       "(command -v unzip >/dev/null || sudo -n apt-get install -y -qq unzip >/dev/null) && "
                       "unzip -oq /tmp/protoc32.zip -d /tmp/protoc32 && cp /tmp/protoc32/bin/protoc ~/.local/bin/ && "
                       "cp -r /tmp/protoc32/include/* ~/.local/include/; }")
        self.run(node, f"cd {dst} && export PATH=$HOME/.local/bin:$PATH && uv python install 3.13 && "
                       "uv sync --group dev && uv run python build_protos.py")


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)
