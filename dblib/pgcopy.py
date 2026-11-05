"""Shared-nothing branching by copying PostgreSQL data directories to cluster nodes (``pgcopy``, experiment 13).

A branch is a fresh PostgreSQL 17 instance on some node whose data directory was copied over the network from the
parent's live instance (``pg_basebackup``), then started. Instances are run by the pgcopy node agent
(scripts/setup/pgcopy_node_agent.py), one per node; this module only talks HTTP to the agents.

- ``branch``: one ``POST /instances`` on the agent that will host the branch: copy from the parent, start, wait
  until it accepts connections. The timed BRANCH row is copy + start (+ HTTP); the agent's split (``copy_ms``,
  ``start_ms``, ``bytes``) is kept in ``PgCopyToolSuite.OBSERVED`` and in the agent's archive (forks.jsonl).
- ``commit`` records the WAL flush LSN; ``delete`` stops the instance and removes its directory. diff, merge,
  rebase, revert, reset and commit refs are unsupported (as for ``neon_local``).
- The run root (branch "main") is a copy of the seed instance on the root agent (the seed node); every other
  branch is copied from its parent, so a branch of main is always a network copy from the seed node.

Instance names derive from the run nonce and the branch name (as ``neon_local``'s timeline ids), so every process
resolves a branch from its name alone. ``setup_run`` sets the nonce and the run root in ``os.environ``.

Placement: with ``PGCOPY_LOCAL_AGENT`` set (co-located clients, the experiment's mode) every branch this process
creates is placed on that agent and connections to it go to 127.0.0.1; otherwise a branch goes to the agent its id
hashes to in ``PGCOPY_AGENTS`` (numbered branches ``*_<i>`` spread round-robin).

Environment:
    PGCOPY_AGENTS           http://10.10.1.2:9100,... (host agents)
    PGCOPY_ROOT_AGENT       http://10.10.1.1:9100 (the seed node: seed instance and run roots)
    PGCOPY_LOCAL_AGENT      this node's agent (co-located mode; optional)
    PGCOPY_SEED_INSTANCE    seed (default)
    PGCOPY_PG_USER, PGCOPY_PG_PASSWORD, PGCOPY_DB (default bench)
    PGCOPY_SHARED_BUFFERS   shared_buffers of new instances (default: the agent's)
    PGCOPY_INSTANCE_OPTS    optional JSON merged into every create request (e.g. {"statement_log": false})
    PGCOPY_START_ON_BRANCH  1 (default): branch = copy + start; 0: copy only, start on first connect
    PGCOPY_PURGE_PARALLEL   agents purged at once (default: all)
    PGCOPY_RUN_NONCE, PGCOPY_RUN_ROOT   set by setup_run
"""

import hashlib
import json
import os
import re
import secrets
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import psycopg2

from dblib import result_collector as rc
from dblib.db_api import DBToolSuite, Ref
from dblib.neon_local import http

MAIN_BRANCH = "main"
HTTP_TIMEOUT = 900


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        raise RuntimeError(f"{name} is not set (see dblib/pgcopy.py)")
    return v


def _list(name: str) -> list:
    return [x.strip().rstrip("/") for x in os.environ.get(name, "").split(",") if x.strip()]


def seed_instance() -> str:
    return os.environ.get("PGCOPY_SEED_INSTANCE") or "seed"


def root_agent() -> str:
    return _env("PGCOPY_ROOT_AGENT").rstrip("/")


def local_agent() -> str:
    return (os.environ.get("PGCOPY_LOCAL_AGENT") or "").strip().rstrip("/")


def agents() -> list:
    return _list("PGCOPY_AGENTS")


_NUMBERED = re.compile(r"_(\d+)$")


def instance_id_for(branch: str) -> str:
    """The run root for "main", else md5(nonce/branch). A numbered branch (``*_<i>``) is shifted so that it maps to
    agent i mod (number of agents) in agent_for when no local agent is set (as neon_local.timeline_id_for)."""
    if branch == MAIN_BRANCH:
        return _env("PGCOPY_RUN_ROOT")
    iid = int(hashlib.md5(f"{_env('PGCOPY_RUN_NONCE')}/{branch}".encode()).hexdigest(), 16)
    m = _NUMBERED.search(branch)
    k = len(agents())
    if m and k > 1:
        iid = iid - iid % k + (int(m.group(1)) - 1) % k
        if iid < 0:
            iid += k
    return f"{iid:032x}"


def agent_for(instance_id: str) -> str:
    """The agent that holds (or will hold) an instance: the root agent for the seed and run roots, this node's
    agent in co-located mode, else the agent the id hashes to."""
    if instance_id in (seed_instance(), os.environ.get("PGCOPY_RUN_ROOT")) or instance_id.startswith("root_"):
        return root_agent()
    if local_agent():
        return local_agent()
    ags = agents()
    if not ags:
        raise RuntimeError("PGCOPY_AGENTS is empty and no PGCOPY_LOCAL_AGENT is set")
    try:
        return ags[int(instance_id, 16) % len(ags)]
    except ValueError:   # a non-hex name (smoke tests, cold_<k>): hash it
        return ags[int(hashlib.md5(instance_id.encode()).hexdigest(), 16) % len(ags)]


_LOCAL_ADDRS: Optional[set] = None


def _local_addrs() -> set:
    global _LOCAL_ADDRS
    if _LOCAL_ADDRS is None:
        out = {"127.0.0.1", "localhost"}
        try:
            out |= {a[4][0] for a in socket.getaddrinfo(socket.gethostname(), None)}
        except OSError:
            pass
        try:
            import subprocess
            out |= set(subprocess.run(["hostname", "-I"], capture_output=True, text=True, timeout=5).stdout.split())
        except Exception:
            pass
        _LOCAL_ADDRS = out
    return _LOCAL_ADDRS


def connect_host(agent: str, host: str) -> str:
    """Address a client uses for an instance the agent reports at `host`: loopback when the agent is this node's."""
    if agent == local_agent() or host in _local_addrs():
        return "127.0.0.1"
    return host


def _opts() -> dict:
    raw = os.environ.get("PGCOPY_INSTANCE_OPTS", "").strip()
    return json.loads(raw) if raw else {}


def _start_on_branch() -> bool:
    return os.environ.get("PGCOPY_START_ON_BRANCH", "1").strip() not in ("0", "false", "no")


def get_instance(instance_id: str, agent: Optional[str] = None) -> Optional[dict]:
    status, body = http("GET", f"{agent or agent_for(instance_id)}/instances/{instance_id}", timeout=120)
    if status == 404:
        return None
    if status != 200:
        raise RuntimeError(f"instance {instance_id}: HTTP {status} {body}")
    return body


def create_instance(instance_id: str, parent_id: Optional[str], parent_agent: Optional[str] = None,
                    agent: Optional[str] = None, start: Optional[bool] = None, init: bool = False,
                    parent_role: bool = False, extra: Optional[dict] = None) -> dict:
    """Copy parent_id's data directory to a new instance on `agent` (default agent_for) and (by default) start it.
    Returns the agent's reply: {name, host, port, copy_ms, start_ms, bytes, started, agent}."""
    agent = agent or agent_for(instance_id)
    body = {"name": instance_id, "init": init, "parent_role": parent_role,
            "start": _start_on_branch() if start is None else start}
    if parent_id is not None:
        pa = parent_agent or agent_for(parent_id)
        p = get_instance(parent_id, pa)
        if p is None:
            raise RuntimeError(f"parent instance {parent_id} not found on {pa}")
        body["parent"] = {"host": p["host"], "port": p["port"], "name": parent_id}
    sb = os.environ.get("PGCOPY_SHARED_BUFFERS")
    if sb:
        body["shared_buffers"] = sb
    body.update(_opts())
    body.update(extra or {})
    status, resp = http("POST", f"{agent}/instances", body, timeout=HTTP_TIMEOUT)
    if status != 200:
        raise RuntimeError(f"instance create {instance_id} from {parent_id}: HTTP {status} {resp}")
    resp["agent"] = agent
    return resp


def start_instance(instance_id: str, agent: Optional[str] = None) -> dict:
    agent = agent or agent_for(instance_id)
    status, resp = http("POST", f"{agent}/instances/{instance_id}/start", {}, timeout=HTTP_TIMEOUT)
    if status != 200:
        raise RuntimeError(f"instance start {instance_id}: HTTP {status} {resp}")
    resp["agent"] = agent
    return resp


def delete_instance(instance_id: str, agent: Optional[str] = None) -> None:
    status, body = http("DELETE", f"{agent or agent_for(instance_id)}/instances/{instance_id}", timeout=300)
    if status not in (200, 404):
        raise RuntimeError(f"instance delete {instance_id}: HTTP {status} {body}")


def instance_sql(instance_id: str, sql: str, db: str = "postgres", agent: Optional[str] = None) -> str:
    status, resp = http("POST", f"{agent or agent_for(instance_id)}/instances/{instance_id}/sql",
                        {"sql": sql, "db": db}, timeout=7200)
    if status != 200:
        raise RuntimeError(f"SQL on {instance_id}: HTTP {status} {resp}")
    return resp["stdout"]


def all_agents() -> list:
    return list(dict.fromkeys([root_agent()] + agents() + ([local_agent()] if local_agent() else [])))


def purge(log=print, parallel: Optional[int] = None) -> int:
    """Delete every instance on every agent except the seed (the root agent protects it). Returns the count."""
    ags = all_agents()
    parallel = parallel or int(os.environ.get("PGCOPY_PURGE_PARALLEL") or len(ags))
    with ThreadPoolExecutor(max(1, min(parallel, len(ags)))) as ex:
        res = list(ex.map(lambda a: http("DELETE", f"{a}/instances", timeout=1800), ags))
    bad = [(a, r) for a, r in zip(ags, res) if r[0] != 200]
    if bad:
        raise RuntimeError(f"purge: {bad}")
    n = sum((r[1] or {}).get("deleted", 0) for r in res if isinstance(r[1], dict))
    if n:
        log(f"pgcopy purge: deleted {n} instances on {len(ags)} agents")
    return n


def new_run(db_name: str) -> str:
    """Nonce and run-root name for a run, published in os.environ."""
    nonce = secrets.token_hex(6)
    os.environ["PGCOPY_RUN_NONCE"] = nonce
    root = "root_" + hashlib.md5(f"{nonce}/{db_name}/root".encode()).hexdigest()[:26]
    os.environ["PGCOPY_RUN_ROOT"] = root
    return root


def create_run_root(root: str, log=print) -> dict:
    r = create_instance(root, seed_instance(), parent_agent=root_agent(), agent=root_agent(), start=True,
                        parent_role=True)
    log(f"pgcopy run root {root} (branch '{MAIN_BRANCH}') copied from {seed_instance()} in "
        f"{r.get('copy_ms', 0):.0f} ms + start {r.get('start_ms', 0):.0f} ms, nonce {os.environ['PGCOPY_RUN_NONCE']}")
    return r


def setup_run(db_name: str, log=print) -> str:
    """Start a run: delete leftovers, copy the run root ("main") from the seed instance on the root agent."""
    purge(log=log)
    root = new_run(db_name)
    create_run_root(root, log)
    return root


def teardown_run(db_name: str, log=print) -> None:
    purge(log=log)


# ----------------------------------------------------------------------------
# The suite
# ----------------------------------------------------------------------------


class PgCopyToolSuite(DBToolSuite):
    BACKEND_NAME = "pgcopy"
    STORAGE_SCOPE = "server"
    SCHEMA_DIALECT = "postgres"
    SUPPORTS_COMMIT_REFS = False
    SUPPORTS_MULTI_REF_EXEC = False

    # instance id -> (host, port), shared by the suites of one process
    _INSTANCES: dict = {}
    _LOCK = threading.Lock()
    # fork phase totals of this process (the summary's backend_observations; per-fork rows: agent forks.jsonl)
    OBSERVED: dict = {}

    @classmethod
    def _observe(cls, **kv) -> None:
        with cls._LOCK:
            for k, v in kv.items():
                if v is not None:
                    cls.OBSERVED[k] = cls.OBSERVED.get(k, 0) + v

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str, default_branch: str,
                       measure_storage: bool = False):
        return cls(collector, default_branch or MAIN_BRANCH, measure_storage)

    def __init__(self, collector: rc.ResultCollector, default_branch: str = MAIN_BRANCH,
                 measure_storage: bool = False):
        super().__init__(None, collector, measure_storage)
        self._current_ref = Ref(default_branch)   # known, not connected (the first statement connects)
        self.last_fork: Optional[dict] = None

    # -- connections ---------------------------------------------------

    def _address(self, instance_id: str, refresh: bool = False):
        with self._LOCK:
            hit = None if refresh else self._INSTANCES.get(instance_id)
        if hit:
            return hit
        agent = agent_for(instance_id)
        info = get_instance(instance_id, agent)
        if info is None:
            raise RuntimeError(f"no instance {instance_id} on {agent}")
        if not info.get("running"):
            info = start_instance(instance_id, agent)
        hp = (connect_host(agent, info["host"]), int(info["port"]))
        with self._LOCK:
            self._INSTANCES[instance_id] = hp
        return hp

    def _open(self, instance_id: str):
        last = None
        for attempt in range(3):
            host, port = self._address(instance_id, refresh=attempt > 0)
            try:
                conn = psycopg2.connect(host=host, port=port, dbname=_env("PGCOPY_DB", "bench"),
                                        user=_env("PGCOPY_PG_USER", "bench"),
                                        password=os.environ.get("PGCOPY_PG_PASSWORD", ""), connect_timeout=60)
                conn.autocommit = True
                return conn
            except psycopg2.OperationalError as e:
                last = e
        raise last

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            raise ValueError("pgcopy does not support commit refs")
        iid = instance_id_for(ref.branch)
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
        self.conn = self._open(iid)

    def _connect(self, ref: Ref, exec_id: int, timed: bool, label: str, fallback: bool):
        if self.conn is None or getattr(self.conn, "closed", 0):
            self._current_ref = None
        return super()._connect(ref, exec_id, timed, label, fallback)

    def _ensure_conn(self):
        if self.conn is None or getattr(self.conn, "closed", 0):
            ref = self._current_ref or Ref(MAIN_BRANCH)
            self._connect_impl(ref)
            self._current_ref = ref
        return self.conn

    def get_current_connection(self):
        return self._ensure_conn()

    def _execute(self, query: str, vars=None):
        self._ensure_conn()
        return super()._execute(query, vars)

    def close_connection(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = None

    # -- verbs -----------------------------------------------------------

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        iid = instance_id_for(name)
        r = create_instance(iid, instance_id_for(from_ref.branch))
        self.last_fork = r
        if r.get("running"):
            with self._LOCK:
                self._INSTANCES[iid] = (connect_host(r["agent"], r["host"]), int(r["port"]))
        self._observe(forks=1, fork_copy_ms=r.get("copy_ms"), fork_start_ms=r.get("start_ms"),
                      fork_bytes=r.get("bytes"))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        if self._current_ref != Ref(ref.branch) or self.conn is None:
            self._connect_impl(Ref(ref.branch))
            self._current_ref = Ref(ref.branch)
        return self._execute("SELECT pg_current_wal_flush_lsn()::text")[0][0]

    def _delete_impl(self, ref: Ref) -> None:
        if ref.branch == MAIN_BRANCH:
            raise ValueError("the run root cannot be deleted")
        iid = instance_id_for(ref.branch)
        if self._current_ref and self._current_ref.branch == ref.branch:
            self.close_connection()
            self._current_ref = None
        delete_instance(iid)
        with self._LOCK:
            self._INSTANCES.pop(iid, None)

    def _storage_bytes(self) -> int:
        total = 0
        for a in all_agents():
            status, body = http("GET", f"{a}/stats", timeout=60)
            if status == 200 and isinstance(body, dict):
                total += int(body.get("data_bytes") or 0)
        return total
