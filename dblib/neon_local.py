"""Self-hosted Neon cluster backend (``neon_local``).

A Neon deployment run by hand (storage controller, pageserver, safekeepers;
see experiment/3-neon-branch-scaling) rather than Neon's cloud API, which
``dblib/neon.py`` drives. Branches are pageserver timelines of one tenant; each
branch is served by its own Postgres compute, started on first use by the
compute-node agent (experiment/3-neon-branch-scaling/scripts/neon_node_agent.py).

- ``branch``: one storage-controller timeline create
  (``POST /v1/tenant/<t>/timeline`` with the parent as ancestor, branching at
  the parent's last ingested LSN). Nothing else is timed.
- The compute starts lazily on the first connection to a branch, so the
  CONNECT row recorded for that switch is compute start + first connection.
- ``commit`` records the WAL flush LSN; ``delete`` stops the compute and deletes
  the timeline. diff, merge, rebase, revert, reset and commit refs are
  unsupported.

Timeline ids are derived from the run nonce and the branch name, so every
process resolves a name without a registry. ``setup_run`` sets the nonce and
the run root (the branch "main") in ``os.environ``; client processes spawned
afterwards inherit both.

Environment:
    NEON_LOCAL_STORCON_URL      http://10.10.1.4:1234
    NEON_LOCAL_PAGESERVER_URL   http://10.10.1.3:9898 (deletion polling)
    NEON_LOCAL_SAFEKEEPER_URLS  http://10.10.1.4:7676,... (timeline cleanup)
    NEON_LOCAL_AGENTS           http://10.10.1.2:9000,... (compute nodes)
    NEON_LOCAL_TENANT_ID, NEON_LOCAL_SEED_TIMELINE_ID
    NEON_LOCAL_PG_USER, NEON_LOCAL_PG_PASSWORD, NEON_LOCAL_DB (default bench)
    NEON_LOCAL_SHARED_BUFFERS   compute shared_buffers (default: the agent's, 1MB)
    NEON_LOCAL_RUN_NONCE, NEON_LOCAL_RUN_ROOT  set by setup_run
"""

import hashlib
import json
import os
import secrets
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import psycopg2

from dblib import result_collector as rc
from dblib.db_api import DBToolSuite, Ref

MAIN_BRANCH = "main"


# ----------------------------------------------------------------------------
# Cluster access (environment read at call time: runner2 imports every adapter)
# ----------------------------------------------------------------------------


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        raise RuntimeError(f"{name} is not set (see dblib/neon_local.py)")
    return v


def _list(name: str) -> list:
    return [x.strip().rstrip("/") for x in _env(name).split(",") if x.strip()]


def http(method: str, url: str, body=None, timeout: float = 120):
    """(status, parsed JSON or text) of one HTTP request."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"} if data else {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    try:
        return status, json.loads(raw or b"null")
    except ValueError:
        return status, raw.decode(errors="replace")


def tenant() -> str:
    return _env("NEON_LOCAL_TENANT_ID")


def seed_timeline() -> str:
    return _env("NEON_LOCAL_SEED_TIMELINE_ID")


def timeline_id_for(branch: str) -> str:
    if branch == MAIN_BRANCH:
        return _env("NEON_LOCAL_RUN_ROOT")
    return hashlib.md5(f"{_env('NEON_LOCAL_RUN_NONCE')}/{branch}".encode()).hexdigest()


def create_timeline(timeline_id: str, ancestor: str) -> None:
    status, body = http("POST", f"{_env('NEON_LOCAL_STORCON_URL')}/v1/tenant/{tenant()}/timeline",
                        {"new_timeline_id": timeline_id, "ancestor_timeline_id": ancestor})
    if status not in (200, 201):
        raise RuntimeError(f"timeline create {timeline_id} from {ancestor}: HTTP {status} {body}")


def list_timelines() -> list:
    status, body = http("GET", f"{_env('NEON_LOCAL_STORCON_URL')}/v1/tenant/{tenant()}/timeline")
    if status != 200:
        raise RuntimeError(f"timeline list: HTTP {status} {body}")
    return body


def delete_timeline(timeline_id: str, timeout: float = 600) -> None:
    """Delete on the pageserver (through the storage controller; asynchronous:
    polled on the pageserver until the timeline is gone) and on every safekeeper."""
    path = f"/v1/tenant/{tenant()}/timeline/{timeline_id}"
    deadline = time.time() + timeout
    while True:
        status, body = http("DELETE", _env("NEON_LOCAL_STORCON_URL") + path)
        if status in (200, 202, 404):
            break
        if time.time() > deadline:
            raise RuntimeError(f"timeline delete {timeline_id}: HTTP {status} {body}")
        time.sleep(0.5)   # 409/412/503: another operation on the tenant; retry
    while http("GET", _env("NEON_LOCAL_PAGESERVER_URL") + path)[0] != 404:
        if time.time() > deadline:
            raise RuntimeError(f"timeline {timeline_id} still present after delete")
        time.sleep(0.2)
    for sk in _list("NEON_LOCAL_SAFEKEEPER_URLS"):
        http("DELETE", f"{sk}{path}", timeout=60)


def agent_for(timeline_id: str) -> str:
    agents = _list("NEON_LOCAL_AGENTS")
    return agents[int(timeline_id, 16) % len(agents)]


def ensure_compute(timeline_id: str, shared_buffers: Optional[str] = None) -> dict:
    body = {"timeline_id": timeline_id, "tenant_id": tenant()}
    sb = shared_buffers or os.environ.get("NEON_LOCAL_SHARED_BUFFERS")
    if sb:
        body["shared_buffers"] = sb
    status, resp = http("POST", f"{agent_for(timeline_id)}/computes", body, timeout=600)
    if status != 200:
        raise RuntimeError(f"compute start for {timeline_id}: HTTP {status} {resp}")
    return resp


def stop_compute(timeline_id: str) -> None:
    http("DELETE", f"{agent_for(timeline_id)}/computes/{timeline_id}", timeout=300)


def compute_sql(timeline_id: str, sql: str) -> str:
    """Run SQL as cloud_admin on the timeline's (running) compute."""
    status, resp = http("POST", f"{agent_for(timeline_id)}/computes/{timeline_id}/sql", {"sql": sql},
                        timeout=3600)
    if status != 200:
        raise RuntimeError(f"admin SQL on {timeline_id}: HTTP {status} {resp}")
    return resp["stdout"]


def stop_all_computes() -> int:
    agents = _list("NEON_LOCAL_AGENTS")
    with ThreadPoolExecutor(len(agents)) as ex:
        res = list(ex.map(lambda a: http("DELETE", f"{a}/computes", timeout=1800), agents))
    return sum((r[1] or {}).get("stopped", 0) for r in res if isinstance(r[1], dict))


def purge(keep=(), log=print, parallel: int = 16) -> int:
    """Stop every compute and delete every timeline except the seed and
    ``keep``, children before parents. Returns the number deleted."""
    stopped = stop_all_computes()
    keep = set(keep) | {seed_timeline()}
    deleted = 0
    while True:
        tls = list_timelines()
        parents = {t.get("ancestor_timeline_id") for t in tls}
        leaves = [t["timeline_id"] for t in tls if t["timeline_id"] not in keep
                  and t["timeline_id"] not in parents]
        if not leaves:
            break
        with ThreadPoolExecutor(parallel) as ex:
            list(ex.map(delete_timeline, leaves))
        deleted += len(leaves)
    if stopped or deleted:
        log(f"neon_local purge: stopped {stopped} computes, deleted {deleted} timelines")
    return deleted


def setup_run(db_name: str, log=print) -> str:
    """Start a run: clean leftovers, branch the run root ("main") off the
    seed timeline, publish nonce and root in os.environ. Returns the root id."""
    purge(log=log)
    nonce = secrets.token_hex(6)
    os.environ["NEON_LOCAL_RUN_NONCE"] = nonce
    root = hashlib.md5(f"{nonce}/{db_name}/root".encode()).hexdigest()
    os.environ["NEON_LOCAL_RUN_ROOT"] = root
    create_timeline(root, seed_timeline())
    log(f"neon_local run root {root} (branch '{MAIN_BRANCH}') from seed {seed_timeline()}, nonce {nonce}")
    return root


def teardown_run(db_name: str, log=print) -> None:
    purge(log=log)


# ----------------------------------------------------------------------------
# The suite
# ----------------------------------------------------------------------------


class NeonLocalToolSuite(DBToolSuite):
    BACKEND_NAME = "neon_local"
    STORAGE_SCOPE = "server"
    SCHEMA_DIALECT = "postgres"
    SUPPORTS_COMMIT_REFS = False
    SUPPORTS_MULTI_REF_EXEC = False

    # timeline id -> (host, port), shared by the suites of one process
    _COMPUTES: dict = {}
    _LOCK = threading.Lock()

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str, default_branch: str,
                       measure_storage: bool = False):
        return cls(collector, default_branch or MAIN_BRANCH, measure_storage)

    def __init__(self, collector: rc.ResultCollector, default_branch: str = MAIN_BRANCH,
                 measure_storage: bool = False):
        super().__init__(None, collector, measure_storage)
        # Known but not connected: the first statement or switch connects.
        # (1,024 workers must not all open a connection on the default branch.)
        self._current_ref = Ref(default_branch)
        self.last_compute_start_ms: Optional[float] = None

    # -- connections ---------------------------------------------------

    def _compute(self, timeline_id: str, refresh: bool = False):
        with self._LOCK:
            hit = None if refresh else self._COMPUTES.get(timeline_id)
        if hit:
            return hit
        resp = ensure_compute(timeline_id)
        if resp.get("started"):
            self.last_compute_start_ms = resp.get("start_ms")
        hp = (resp["host"], int(resp["port"]))
        with self._LOCK:
            self._COMPUTES[timeline_id] = hp
        return hp

    def _open(self, timeline_id: str):
        last = None
        for attempt in range(3):
            host, port = self._compute(timeline_id, refresh=attempt > 0)
            try:
                conn = psycopg2.connect(host=host, port=port, dbname=_env("NEON_LOCAL_DB", "bench"),
                                        user=_env("NEON_LOCAL_PG_USER", "bench"),
                                        password=os.environ.get("NEON_LOCAL_PG_PASSWORD", ""),
                                        connect_timeout=60)
                conn.autocommit = True
                return conn
            except psycopg2.OperationalError as e:
                last = e
        raise last

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            raise ValueError("neon_local does not support commit refs")
        timeline_id = timeline_id_for(ref.branch)
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
        self.conn = self._open(timeline_id)

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
        create_timeline(timeline_id_for(name), timeline_id_for(from_ref.branch))

    def _commit_impl(self, ref: Ref, message: str) -> str:
        if self._current_ref != Ref(ref.branch) or self.conn is None:
            self._connect_impl(Ref(ref.branch))
            self._current_ref = Ref(ref.branch)
        return self._execute("SELECT pg_current_wal_flush_lsn()::text")[0][0]

    def _delete_impl(self, ref: Ref) -> None:
        if ref.branch == MAIN_BRANCH:
            raise ValueError("the run root cannot be deleted")
        timeline_id = timeline_id_for(ref.branch)
        if self._current_ref and self._current_ref.branch == ref.branch:
            self.close_connection()
            self._current_ref = None
        stop_compute(timeline_id)
        with self._LOCK:
            self._COMPUTES.pop(timeline_id, None)
        delete_timeline(timeline_id)

    def _storage_bytes(self) -> int:
        return 0
