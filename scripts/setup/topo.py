#!/usr/bin/env python3
"""Deployment topology for the shared setup and workload scripts (stdlib only, Python >= 3.11).

A topology is a TOML file (examples in scripts/setup/topologies/) naming the machines, the role each one plays for
Neon and Doltgres, CPU/NUMA placement, directories, ports and the workload parameters. Everything else reads it
through this module: setup/neon.py and setup/doltgres.py to deploy, the workload scripts to find the client
placement and the backend's environment file.

  topo.py check  TOPO                      validate; print a summary
  topo.py shell  TOPO [--backend B]        print `export` lines for bash (eval "$(topo.py shell ...)")
  topo.py get    TOPO KEY                  one value by dotted key, e.g. workload.W or neon.pageservers
  topo.py env    TOPO --backend B [--view NODE] [--out F]
                                           write the backend's environment file (NEON_LOCAL_* / DOLT_* /
                                           DBFORK_REMOTE_CPU) as seen from NODE (default: the client); default
                                           output <state_dir>/<backend>.env. Never the repo's .env.

Addresses: a role on the same host as the viewer is reached on a Unix socket (Neon, transport "uds") or loopback;
on another host in the same zone on its LAN address; in another zone on its public address (CloudLab LANs of
different sites do not route to each other).

pgcopy (experiment 13): a seed node (the seed instance and every run root) and host nodes (branch instances, each
with its benchmark client: co-located). Its agents and instances listen on LAN addresses only, so its addresses are
always LAN addresses, also from the node itself (no loopback; the client connects to its own instances on
127.0.0.1 through dblib/pgcopy.py).

Neon transport "rdma" (experiment 12): every service-to-service Postgres-protocol connection (GetPage, WAL push, WAL
pull, basebackup) runs over an RDMA byte stream: listeners `rdma:0.0.0.0:<port>`, peers `rdma:<ip>:<port>`. RDMA has
no loopback device, so a role on the viewer's own host is reached on the host's LAN address. HTTP endpoints stay TCP
(the benchmark client and the agents use them).
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import tomllib
from pathlib import Path

LOCAL = "local"

PORT_DEFAULTS = {  # experiment 5's ports
    "storcon": 1234, "storcon_pg": 1235, "broker": 50051, "hook": 9999, "ps_http": 9898, "ps_pg": 64000,
    "sk_pg": 5454, "sk_http": 7676, "agent": 9000, "cpu_agent": 9010, "doltgres": 5433,
    "pgcopy_agent": 9100, "pgcopy_pg": 24000,
}

WORKLOAD_DEFAULTS = {  # experiment 8's (the user's defaults since 3 Nov 2026; experiment 5 used W=4, prewarm 3400)
    "W": 3,                      # CH-benCHmark warehouses (experiment 8: the LFC then holds 28% of the stock heap)
    "shared_buffers": "8MB",     # compute shared_buffers
    "lfc_mb": 36,                # compute local file cache, MB (28% of the stock heap at W=3 with the prewarm below)
    "lfc_chunk_pages": 1,        # LFC chunk in 8 kB pages (Neon's default 128 collapses the hit ratio)
    "eic": 2,                    # effective_io_concurrency
    "prewarm_heap_blocks": 3674,  # stock heap pages prewarmed (heap first, then stock_pkey); 28.6% of point reads hit
    "seed_shared_buffers": "8GB",
    "storcon_rate_limit": 1000,
    "stripe_pages": 256,         # shard stripe for shard-split
    "table": "stock",
    "updates_per_branch": 100,   # untimed divergence per setup branch
    "setup_parallelism": 64,
    "warmup_fraction": 0.1,
    "ns_data": [1, 4, 16, 64, 128, 256, 512, 1024],
    "ns_branch": [1, 4, 16, 64, 128, 256, 512],
    "creates": 100,
    "connect_every": 5,
    "background": "MIXED",       # 80% point READ / 20% UPDATE; READ = experiments 7 and 11
    "cold_runs": 3,
    "cold_reads": 30000,
    "cold_pause_s": 30,
    "max_procs": 0,              # client processes at most; 0 = number of client CPUs (or 16 without a CPU list)
    "run_timeout_s": 5400,
    "disk_guard_gb": 20,
    "mem_guard_gb": 4,
}

NEON_ROLES_ONE = ("client", "storcon", "broker", "seed_compute")
NEON_ROLES_MANY = ("pageservers", "safekeepers", "compute_agents")


class TopoError(ValueError):
    pass


_ADDRS: set | None = None


def local_addrs() -> set:
    global _ADDRS
    if _ADDRS is None:
        import subprocess
        try:
            out = subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=5).stdout
        except (OSError, subprocess.SubprocessError):
            out = ""
        _ADDRS = set(out.split())
    return _ADDRS


def expand_cpus(spec: str) -> list[int]:
    """'2,6,10-14,20-38:4' -> CPU ids. a-b:s is a stride range."""
    out: list[int] = []
    for part in str(spec).split(","):
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


class Node:
    def __init__(self, name: str, d: dict, topo: "Topo"):
        self.name = name
        self.d = d
        self.topo = topo
        self.host = d.get("host", LOCAL)
        self.local = self.host == LOCAL
        self.zone = d.get("zone", "z0")
        self.lan = d.get("lan", "127.0.0.1" if self.local else None)
        self.public = d.get("public", self.lan)
        if not self.local and not self.lan:
            raise TopoError(f"node {name}: a remote node needs 'lan' (and 'public' if other zones reach it)")
        self.ssd_dev = d.get("ssd_dev", "")
        if self.local:
            base = topo.state_dir
        else:
            base = d.get("data_root", "/mnt/ssd" if self.ssd_dev else "~/bench")
        self.data_root = base
        self.neon_root = d.get("neon_root", f"{base}/neon")
        self.doltgres_root = d.get("doltgres_root", f"{base}/doltgres")
        self.tmpfs_size = d.get("tmpfs_size", "")
        self.compute_root = d.get("compute_root",
                                  "/mnt/tmpfs/computes" if self.tmpfs_size else f"{self.neon_root}/computes")
        self.disk_dev = d.get("disk_dev", "")
        self.server_cpus = d.get("server_cpus", "")
        self.server_mem = str(d.get("server_mem", ""))
        self.client_cpus = d.get("client_cpus", "")
        self.client_mem = str(d.get("client_mem", ""))
        self.server_numa = str(d.get("server_numa", ""))   # NUMA node for numactl --cpunodebind/--membind
        self.client_numa = str(d.get("client_numa", ""))

    def is_here(self) -> bool:
        """True on the machine this node names: host "local", or one of its addresses is configured here
        (CloudLab hostnames differ from the names nodes give themselves)."""
        return self.local or bool({self.lan, self.public} & local_addrs())

    def ssh_target(self) -> str:
        return f"{self.topo.ssh_user}@{self.host}" if self.topo.ssh_user else self.host

    def addr_from(self, viewer: "Node") -> str:
        """Address of this node as seen from `viewer`."""
        if viewer.host == self.host:
            return "127.0.0.1"
        if viewer.zone == self.zone:
            return self.lan
        if not self.public:
            raise TopoError(f"node {self.name} is reached from zone {viewer.zone} but has no 'public' address")
        return self.public

    def cpu_list(self, which: str) -> list[int]:
        spec = self.server_cpus if which == "server" else self.client_cpus
        return expand_cpus(spec) if spec else []

    def cpus_csv(self, which: str) -> str:
        return ",".join(map(str, self.cpu_list(which)))


class Topo:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path).resolve()
        with open(self.path, "rb") as f:
            self.raw = tomllib.load(f)
        r = self.raw
        self.name = r.get("name") or self.path.stem
        self.state_dir = r.get("state_dir")
        if not self.state_dir:
            raise TopoError("state_dir is required (local state: binaries, data, sockets, env files)")
        ssh = r.get("ssh", {})
        self.ssh_user = ssh.get("user", "")
        self.ssh_key = os.path.expanduser(ssh.get("key", "")) if ssh.get("key") else ""
        self.ports = {**PORT_DEFAULTS, **r.get("ports", {})}
        self.workload = {**WORKLOAD_DEFAULTS, **r.get("workload", {})}
        unknown = set(r.get("workload", {})) - set(WORKLOAD_DEFAULTS)
        if unknown:
            raise TopoError(f"unknown workload keys: {sorted(unknown)}")
        nodes = r.get("nodes", {})
        if not nodes:
            raise TopoError("no [nodes.*] defined")
        self.nodes = {n: Node(n, d, self) for n, d in nodes.items()}
        self.neon = r.get("neon")
        self.doltgres = r.get("doltgres")
        self.pgcopy = r.get("pgcopy")
        if self.neon:
            self._check_neon()
        if self.doltgres:
            self._check_doltgres()
        if self.pgcopy:
            self._check_pgcopy()

    # ---- validation ----
    def node(self, name: str) -> Node:
        if name not in self.nodes:
            raise TopoError(f"role names unknown node {name!r} (nodes: {', '.join(self.nodes)})")
        return self.nodes[name]

    def _check_neon(self) -> None:
        n = self.neon
        for r in NEON_ROLES_ONE:
            if r not in n:
                raise TopoError(f"[neon] needs {r} = <node>")
            self.node(n[r])
        for r in NEON_ROLES_MANY:
            if not n.get(r):
                raise TopoError(f"[neon] needs {r} = [<node>, ...]")
            for x in n[r]:
                self.node(x)
        if len(set(n["safekeepers"])) != len(n["safekeepers"]):
            raise TopoError("[neon] safekeepers must be on distinct nodes (one safekeeper per node)")
        if len(set(n["pageservers"])) != len(n["pageservers"]):
            raise TopoError("[neon] pageservers must be on distinct nodes")
        # safekeeper_groups = G (experiment 12, write phase): the safekeepers form G sets of equal size, in list
        # order; each timeline's WAL goes to one set, chosen by the compute agent from the timeline id. Default 1.
        g = n.setdefault("safekeeper_groups", 1)
        if not isinstance(g, int) or g < 1 or len(n["safekeepers"]) % g:
            raise TopoError(f"[neon] safekeeper_groups = {g!r} must divide the {len(n['safekeepers'])} safekeepers")
        n.setdefault("transport", "tcp")
        if n["transport"] not in ("uds", "tcp", "rdma"):
            raise TopoError("[neon] transport must be 'uds', 'tcp' or 'rdma'")
        if n["transport"] == "rdma":
            for x in self.neon_nodes():
                nd = self.node(x)
                if nd.lan in (None, "127.0.0.1", "localhost"):
                    raise TopoError(f"[neon] transport 'rdma' needs a real 'lan' address on node {x} (RDMA has no "
                                    "loopback device)")
        if n["transport"] == "uds":
            if (len(n["pageservers"]) != 1 or len(n["safekeepers"]) != 1 or len(n["compute_agents"]) != 1
                    or n["safekeeper_groups"] != 1):
                raise TopoError("[neon] transport 'uds' (one host) supports one pageserver, one safekeeper and "
                                "one compute agent")
            hosts = {self.node(x).host for x in self.neon_nodes()}
            if len(hosts) != 1:
                raise TopoError("[neon] transport 'uds' needs every Neon role (and the client) on one host; "
                                f"these roles span {sorted(hosts)}")
        if not n.get("build_dir"):
            raise TopoError("[neon] build_dir (the Neon checkout with built binaries) is required")

    def _check_doltgres(self) -> None:
        d = self.doltgres
        for r in ("server", "client"):
            if r not in d:
                raise TopoError(f"[doltgres] needs {r} = <node>")
            self.node(d[r])
        d.setdefault("version", "1.4.0")

    def _check_pgcopy(self) -> None:
        p = self.pgcopy
        if "seed" not in p:
            raise TopoError("[pgcopy] needs seed = <node>")
        self.node(p["seed"])
        if not p.get("hosts"):
            raise TopoError("[pgcopy] needs hosts = [<node>, ...]")
        for x in p["hosts"]:
            self.node(x)
        if len(set(p["hosts"])) != len(p["hosts"]):
            raise TopoError("[pgcopy] hosts must be distinct")
        if p["seed"] in p["hosts"]:
            raise TopoError("[pgcopy] the seed node cannot also be a host (a branch there would be a local copy)")
        p.setdefault("client", p["hosts"][0])
        if p["client"] not in p["hosts"]:
            raise TopoError("[pgcopy] client must be one of the hosts (clients are co-located with their branches)")
        p.setdefault("pg_version", 17)
        for x in [p["seed"], *p["hosts"]]:
            nd = self.node(x)
            if nd.local or nd.lan in (None, "127.0.0.1", "localhost"):
                raise TopoError(f"[pgcopy] node {x} needs a real 'lan' address (agents listen on the LAN only)")

    def pgcopy_nodes(self) -> list[Node]:
        p = self.pgcopy
        return [self.node(x) for x in dict.fromkeys([p["seed"], *p["hosts"]])]

    def pgcopy_root(self, node: Node) -> str:
        return node.d.get("pgcopy_root", f"{node.data_root}/pgcopy")

    def pgcopy_agent_url(self, node: Node) -> str:
        return f"http://{node.lan}:{self.ports['pgcopy_agent']}"

    def pgcopy_env(self, viewer_name: str | None = None, seeding: bool = False) -> dict:
        """PGCOPY_* (+ DBFORK_REMOTE_CPU) for a runner on `viewer` (default: the client). Agents by LAN address."""
        p = self.pgcopy
        viewer = self.node(viewer_name or p["client"])
        w = self.wl("pgcopy")
        env = {
            "PGCOPY_AGENTS": ",".join(self.pgcopy_agent_url(self.node(x)) for x in p["hosts"]),
            "PGCOPY_ROOT_AGENT": self.pgcopy_agent_url(self.node(p["seed"])),
            "PGCOPY_LOCAL_AGENT": self.pgcopy_agent_url(viewer) if viewer.name in p["hosts"] else "",
            "PGCOPY_SEED_INSTANCE": "seed",
            "PGCOPY_PG_USER": "bench",
            "PGCOPY_PG_PASSWORD": "bench",
            "PGCOPY_DB": "bench",
            "PGCOPY_SHARED_BUFFERS": w["seed_shared_buffers"] if seeding else w["shared_buffers"],
            "DBFORK_REMOTE_CPU": "" if seeding else self.remote_cpu("pgcopy", viewer),
        }
        return env

    # ---- roles ----
    def neon_nodes(self) -> list[str]:
        n = self.neon
        out = [n[r] for r in NEON_ROLES_ONE]
        for r in NEON_ROLES_MANY:
            out += n[r]
        return list(dict.fromkeys(out))

    def section(self, backend: str | None) -> dict | None:
        return {"neon": self.neon, "doltgres": self.doltgres, "pgcopy": self.pgcopy}.get(backend or "")

    def role_node(self, backend: str, role: str) -> Node:
        sec = self.section(backend)
        if not sec:
            raise TopoError(f"topology has no [{backend}] section")
        return self.node(sec[role])

    def client(self, backend: str) -> Node:
        return self.role_node(backend, "client")

    def wl(self, backend: str | None) -> dict:
        """Workload parameters: [workload], overridden by [neon.workload] / [doltgres.workload]."""
        sec = self.section(backend) or {}
        over = sec.get("workload", {})
        unknown = set(over) - set(WORKLOAD_DEFAULTS)
        if unknown:
            raise TopoError(f"unknown [{backend}.workload] keys: {sorted(unknown)}")
        return {**self.workload, **over}

    def max_procs(self, backend: str) -> int:
        m = int(self.wl(backend)["max_procs"])
        if m > 0:
            return m
        cpus = self.client(backend).cpu_list("client")
        return len(cpus) if cpus else 16

    # ---- Neon addresses ----
    @property
    def uds(self) -> bool:
        return bool(self.neon) and self.neon["transport"] == "uds"

    @property
    def rdma(self) -> bool:
        return bool(self.neon) and self.neon["transport"] == "rdma"

    def rdma_host(self, node: Node, viewer: Node) -> str:
        """IP of `node` for an RDMA connection from `viewer`: its LAN address on the same host (no loopback)."""
        return node.lan if viewer.host == node.host else node.addr_from(viewer)

    def neon_run_dir(self) -> str:
        """Sockets and pid files of the single-host deployment (sun_path is limited to 108 bytes)."""
        return f"{self.node(self.neon['storcon']).neon_root}/run"

    def sock(self, name: str) -> str:
        return f"{self.neon_run_dir()}/{name}.sock"

    def pg_sock(self, port_key: str) -> str:
        return f"{self.neon_run_dir()}/.s.PGSQL.{self.ports[port_key]}"

    def http_url(self, name: str, port_key: str, node: Node, viewer: Node, path: str = "") -> str:
        """HTTP/gRPC endpoint as the Neon services and dblib take it (http+unix://<hex path> under uds)."""
        if self.uds:
            return f"http+unix://{self.sock(name).encode().hex()}{path}"
        return f"http://{node.addr_from(viewer)}:{self.ports[port_key]}{path}"

    def svc_url(self, name: str, port_key: str, node: Node, viewer: Node, path: str = "") -> str:
        """A service-to-service HTTP/gRPC endpoint (broker, storage controller upcalls): http+rdma://<ip>:<port>
        under rdma, else as http_url."""
        if self.rdma:
            return f"http+rdma://{self.rdma_host(node, viewer)}:{self.ports[port_key]}{path}"
        return self.http_url(name, port_key, node, viewer, path)

    def rdma_listen(self, port_key: str) -> str:
        """The extra RDMA HTTP listener of a service under rdma (its TCP listener stays for the client)."""
        return f"rdma:0.0.0.0:{self.ports[port_key]}"

    def pg_host(self, node: Node, viewer: Node) -> str:
        """libpq host of a Neon pg listener (a socket directory under uds, rdma:<ip> under rdma)."""
        if self.rdma:
            return "rdma:" + self.rdma_host(node, viewer)
        return self.neon_run_dir() if self.uds else node.addr_from(viewer)

    def listen(self, name: str, port_key: str, kind: str = "http") -> str:
        """What a Neon service binds: unix:<path> under uds, else all interfaces."""
        if self.uds:
            return "unix:" + (self.pg_sock(port_key) if kind == "pg" else self.sock(name))
        if self.rdma and kind == "pg":
            return f"rdma:0.0.0.0:{self.ports[port_key]}"
        return f"0.0.0.0:{self.ports[port_key]}"

    def safekeeper_groups(self) -> list[list[str]]:
        """The safekeeper node names in their [neon] safekeeper_groups sets (one set by default)."""
        sks, g = self.neon["safekeepers"], self.neon.get("safekeeper_groups", 1)
        k = len(sks) // g
        return [sks[i * k:(i + 1) * k] for i in range(g)]

    def neon_env(self, viewer_name: str | None = None, seed: dict | None = None, seeding: bool = False) -> dict:
        """NEON_LOCAL_* (+ DBFORK_REMOTE_CPU) for a runner on `viewer` (default: the client node)."""
        n = self.neon
        viewer = self.node(viewer_name or n["client"])
        sc = self.node(n["storcon"])
        pss = [self.node(x) for x in n["pageservers"]]
        sks = [self.node(x) for x in n["safekeepers"]]
        agents = [self.node(n["seed_compute"])] if seeding else [self.node(x) for x in n["compute_agents"]]
        if self.uds:
            agent_urls = [f"http+unix://{self.sock('agent').encode().hex()}"]
            sk_urls = [self.http_url("sk-http", "sk_http", sks[0], viewer)]
        else:
            agent_urls = [f"http://{a.addr_from(viewer)}:{self.ports['agent']}" for a in agents]
            sk_urls = [self.http_url("", "sk_http", s, viewer) for s in sks]
        w = self.wl("neon")
        env = {
            "NEON_LOCAL_STORCON_URL": self.http_url("storcon", "storcon", sc, viewer),
            "NEON_LOCAL_PAGESERVER_URL": self.http_url("ps-http", "ps_http", pss[0], viewer),
            "NEON_LOCAL_SAFEKEEPER_URLS": ",".join(sk_urls),
            "NEON_LOCAL_AGENTS": ",".join(agent_urls),
            "NEON_LOCAL_PG_USER": "bench",
            "NEON_LOCAL_PG_PASSWORD": "bench",
            "NEON_LOCAL_DB": "bench",
            "NEON_LOCAL_SHARED_BUFFERS": w["seed_shared_buffers"] if seeding else w["shared_buffers"],
            "NEON_LOCAL_PURGE_PARALLEL": "64",
            "DBFORK_REMOTE_CPU": "" if seeding else self.remote_cpu("neon", viewer),
        }
        if seed:
            env["NEON_LOCAL_TENANT_ID"] = seed["tenant_id"]
            env["NEON_LOCAL_SEED_TIMELINE_ID"] = seed["seed_timeline_id"]
            if seed.get("ps_opts"):
                env["NEON_LOCAL_PS_OPTS"] = json.dumps(seed["ps_opts"])
        return env

    # ---- Doltgres ----
    def doltgres_env(self, viewer_name: str | None = None) -> dict:
        d = self.doltgres
        viewer = self.node(viewer_name or d["client"])
        srv = self.node(d["server"])
        env = {
            "DOLT_HOST": srv.addr_from(viewer),
            "DOLT_PORT": str(self.ports["doltgres"]),
            "DOLT_USER": d.get("user", "postgres"),
            "DOLT_PASSWORD": d.get("password", "password"),
            "DBFORK_REMOTE_CPU": self.remote_cpu("doltgres", viewer),
        }
        if srv.host == viewer.host:  # the runner purges .dolt_dropped_databases and reads the server pid locally
            env["DOLT_DATA_DIR"] = f"{srv.doltgres_root}/databases"
        else:
            env["DBFORK_SERVER_HOST"] = env["DOLT_HOST"]
        return env

    # ---- CPU sampling (scripts/setup/cpu_agent.py on every server node) ----
    def server_nodes(self, backend: str) -> list[Node]:
        if backend == "pgcopy":
            return self.pgcopy_nodes()
        if backend == "neon":
            n = self.neon
            other_roles = [n[r] for r in NEON_ROLES_ONE if r != "client"] + sum((n[r] for r in NEON_ROLES_MANY), [])
            return [self.node(x) for x in self.neon_nodes() if x in other_roles]
        return [self.node(self.doltgres["server"])]

    def remote_cpu(self, backend: str, viewer: Node) -> str:
        """DBFORK_REMOTE_CPU for runner2's RemoteCpuSampler: label=url;... One label per server node ('server'
        when there is one, else the node name), plus per-group labels of the single-host Neon deployment."""
        port = self.ports["cpu_agent"]
        if backend == "pgcopy":
            # the viewer's own node (its instances and its clients) and the seed node (every copy reads from it);
            # LAN addresses: the CPU agents listen on the LAN only. Groups: postgres processes, pg_basebackup.
            p = self.pgcopy
            seed = self.node(p["seed"])
            out = []
            if viewer.name in p["hosts"]:
                base = f"http://{viewer.lan}:{port}/cpu"
                out += [f"server={base}?set=server", f"computes={base}?group=computes", f"copy={base}?group=copy"]
            out.append(f"seed=http://{seed.lan}:{port}/cpu?set=server")
            return ";".join(out)
        nodes = self.server_nodes(backend)
        out = []
        for nd in nodes:
            label = "server" if len(nodes) == 1 else nd.name
            out.append(f"{label}=http://{nd.addr_from(viewer)}:{port}/cpu?set=server")
        if len(nodes) == 1:
            nd = nodes[0]
            base = f"http://{nd.addr_from(viewer)}:{port}/cpu"
            client_node = self.client(backend)
            if client_node.host == nd.host and client_node.cpu_list("client"):
                out.append(f"clients={base}?set=clients")
            groups = (["pageserver", "walredo", "safekeeper", "computes", "compute_ctl", "storcon", "storcon_pg",
                       "broker", "agent"] if backend == "neon" else ["doltgres"])
            out += [f"{g}={base}?group={g}" for g in groups]
        return ";".join(out)

    # ---- shell export ----
    def shell_vars(self, backend: str | None = None) -> dict:
        w = self.wl(backend)
        v = {
            "TOPO_FILE": str(self.path), "TOPO_NAME": self.name, "STATE_DIR": self.state_dir,
            "SSH_USER": self.ssh_user, "SSH_KEY": self.ssh_key,
            "W": w["W"], "SB": w["shared_buffers"], "LFC_MB": w["lfc_mb"], "LFC_CHUNK": w["lfc_chunk_pages"],
            "EIC": w["eic"], "PREWARM_HEAP_BLOCKS": w["prewarm_heap_blocks"], "TABLE": w["table"],
            "UPDATES_PER_BRANCH": w["updates_per_branch"], "SETUP_PARALLELISM": w["setup_parallelism"],
            "WARMUP_FRACTION": w["warmup_fraction"],
            "NS_DATA": ",".join(map(str, w["ns_data"])), "NS_BRANCH": ",".join(map(str, w["ns_branch"])),
            "CREATES": w["creates"], "CONNECT_EVERY": w["connect_every"], "BACKGROUND": w["background"],
            "COLD_RUNS": w["cold_runs"], "COLD_READS": w["cold_reads"], "COLD_PAUSE_S": w["cold_pause_s"],
            "RUN_TIMEOUT": w["run_timeout_s"], "DISK_GUARD_GB": w["disk_guard_gb"], "MEM_GUARD_GB": w["mem_guard_gb"],
            "CPU_AGENT_PORT": self.ports["cpu_agent"],
        }
        if backend:
            c = self.client(backend)
            srv = (self.node(self.neon["storcon"]) if backend == "neon" else
                   c if backend == "pgcopy" else self.node(self.doltgres["server"]))
            v.update({
                "BACKEND": backend,
                "CLIENT_NODE": c.name, "CLIENT_HOST": c.host, "CLIENT_IS_LOCAL": int(c.is_here()),
                "CLIENT_CPUS": c.cpus_csv("client"), "CLIENT_MEM": c.client_mem, "CLIENT_NUMA": c.client_numa,
                "SERVER_NODE": srv.name, "SERVER_CPUS": srv.cpus_csv("server"), "SERVER_MEM": srv.server_mem,
                "SERVER_NUMA": srv.server_numa,
                # pgcopy: the instances share the client's node, but there is no single server process to watch
                # (no DBFORK_SERVER_PID, no per-run CPU agent): the node's CPU agent runs as a service
                "SERVER_SAME_HOST": 0 if backend == "pgcopy" else int(srv.host == c.host),
                "MAX_PROCS": self.max_procs(backend),
                "ENV_FILE": f"{self.state_dir}/{backend}.env",
                "SEED_DB": "bench" if backend in ("neon", "pgcopy") else f"ch_w{w['W']}_seed",
                "RUNNER_BACKEND": {"neon": "neon_local", "pgcopy": "pgcopy"}.get(backend, "dolt"),
            })
            if backend == "neon":
                v["NEON_RUN_DIR"] = self.neon_run_dir()
                v["NEON_TRANSPORT"] = self.neon["transport"]
            if backend == "pgcopy":
                v["PGCOPY_HOSTS"] = ",".join(self.pgcopy["hosts"])
                v["PGCOPY_SEED_NODE"] = self.pgcopy["seed"]
        return v


def env_text(env: dict) -> str:
    """KEY=VALUE lines readable by bash `set -a; source` and python-dotenv (JSON values in single quotes)."""
    lines = []
    for k, val in env.items():
        s = str(val)
        lines.append(f"{k}='{s}'" if any(c in s for c in "\"; {}&") else f"{k}={s}")
    return "\n".join(lines) + "\n"


def read_seed(topo: Topo, backend: str) -> dict | None:
    p = Path(topo.state_dir) / f"{backend}_seed.json"
    return json.loads(p.read_text()) if p.exists() else None


def write_env(topo: Topo, backend: str, view: str | None = None, out: str | None = None,
              seeding: bool = False) -> Path:
    if backend == "neon":
        env = topo.neon_env(view, read_seed(topo, "neon"), seeding=seeding)
    elif backend == "pgcopy":
        env = topo.pgcopy_env(view, seeding=seeding)
    else:
        env = topo.doltgres_env(view)
    path = Path(out or f"{topo.state_dir}/{backend}.env")
    if path.name == ".env":
        raise TopoError("refusing to write a .env (other experiments share the checkout)")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(env_text(env))
    return path


def get_key(topo: Topo, key: str):
    cur = topo.raw
    if key.startswith("workload."):
        return topo.workload[key.split(".", 1)[1]]
    for b in ("neon", "doltgres", "pgcopy"):
        if key.startswith(f"{b}.workload."):
            return topo.wl(b)[key.split(".", 2)[2]]
    if key.startswith("ports."):
        return topo.ports[key.split(".", 1)[1]]
    for part in key.split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise TopoError(f"no key {key}")
        cur = cur[part]
    return cur


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("check"); p.add_argument("topo")
    p = sub.add_parser("shell"); p.add_argument("topo"); p.add_argument("--backend", choices=["neon", "doltgres", "pgcopy"])
    p = sub.add_parser("get"); p.add_argument("topo"); p.add_argument("key")
    p = sub.add_parser("env"); p.add_argument("topo"); p.add_argument("--backend", required=True,
                                                                    choices=["neon", "doltgres", "pgcopy"])
    p.add_argument("--view"); p.add_argument("--out"); p.add_argument("--seeding", action="store_true")
    a = ap.parse_args(argv)
    try:
        t = Topo(a.topo)
        if a.cmd == "check":
            print(f"{t.name}: {len(t.nodes)} nodes, state_dir {t.state_dir}")
            for nd in t.nodes.values():
                print(f"  {nd.name:8} {nd.host:36} zone={nd.zone} lan={nd.lan} public={nd.public}")
            if t.neon:
                n = t.neon
                print(f"  neon ({n['transport']}): client={n['client']} storcon={n['storcon']} "
                      f"pageservers={n['pageservers']} safekeepers={n['safekeepers']} "
                      f"agents={n['compute_agents']} seed_compute={n['seed_compute']}")
            if t.doltgres:
                print(f"  doltgres {t.doltgres['version']}: server={t.doltgres['server']} "
                      f"client={t.doltgres['client']}")
            if t.pgcopy:
                p = t.pgcopy
                print(f"  pgcopy (PostgreSQL {p['pg_version']}): seed={p['seed']} client={p['client']} "
                      f"hosts={p['hosts']}")
            print(f"  workload: W={t.workload['W']} SB={t.workload['shared_buffers']} LFC={t.workload['lfc_mb']}MB "
                  f"ns_data={t.workload['ns_data']} ns_branch={t.workload['ns_branch']}")
        elif a.cmd == "shell":
            for k, v in t.shell_vars(a.backend).items():
                print(f"export {k}={shlex.quote(str(v))}")
        elif a.cmd == "get":
            v = get_key(t, a.key)
            print(" ".join(map(str, v)) if isinstance(v, list) else v)
        elif a.cmd == "env":
            print(write_env(t, a.backend, a.view, a.out, a.seeding))
    except TopoError as e:
        print(f"topology error: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
