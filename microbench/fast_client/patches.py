"""Client-side speedups for the benchmark runner (microbench/runner2.py), applied as runtime patches so that the
shared modules stay unchanged. Opt-in (see sitecustomize.py in this directory):

    BENCH_FAST_CLIENT=1 PYTHONPATH=<repo>/microbench/fast_client[:...]  <any runner command>

Why (experiment 12, 3 Nov 2026): on an xl170 client node the runner spent about 1,900 µs of client CPU per insert or
update against about 380 µs per point read, so one client node capped writes at about 8k/s. The three costs below
are client-side only. None is inside the latency timer (which wraps only the SQL execute), so measured latencies
mean the same with or without the patches. What the server receives is the same, except that text values are
drawn from a pool of Faker values instead of generated per row.

1. microbench.datagen.DynamicDataGenerator.generate_value: every value that comes from Faker (text, names, dates,
   booleans, ...) is drawn from a per-process pool of POOL_SIZE values that Faker generated, instead of calling
   Faker per row (Faker lorem text: ~420 µs per stock row in a tight loop, ~1,300 µs when interleaved with network
   waits). Numeric columns keep their per-row random values, so primary keys stay as unique as before. Same value
   distribution, repeated values.
2. util.sql_parse._remove_sql_comments: returns the text unchanged when it holds no "--" and no "/*" (the original
   returns it unchanged in that case too, after a per-character Python loop: ~155 µs on a 500-character insert).
3. microbench.runner2.WorkerContext.track_modified_key / untrack_modified_key: membership in a per-branch set kept
   alongside the list (the original scans the list: O(keys modified so far) per write). The list stays, with the
   same contents and order, because select_random_key draws from it.

Cross-node start (scripts/workloads/multi_client, experiment 12): with BENCH_SYNC_ADDR=host:port and BENCH_SYNC_KEY
set, the runner's start barrier (TimedPhase, ProcessTimedPhase) is extended past this node: after the local barrier
each client process (its first thread; the others wait for it) connects to the coordinator, sends the key and
blocks until the coordinator answers GO, which it does once every client process of every node has arrived. The
timed window then starts on every node at once (window start = GO; the runner's own start bookkeeping is
unchanged otherwise). ABORT (another node failed) or a timeout (BENCH_SYNC_TIMEOUT s, default 3600) fails the run.
In the same mode dblib.neon_local.setup_run skips its purge and teardown_run does nothing: both stop every compute
and delete every timeline of the tenant, which would remove the other nodes' branches (found in the first trial:
a node's setup deleted another node's timelines 0.35 s after they were created). The orchestrator purges once per
point instead. Names cannot collide: each runner's run root and branches derive from its own random run nonce.
"""
from __future__ import annotations

import hashlib
import os
import random
import socket
import sys
import threading
import time
from pathlib import Path

POOL_SIZE = int(os.environ.get("BENCH_FAST_CLIENT_POOL", "512"))

_NAME_HINTS = ("city", "state", "zip", "postal", "phone", "first", "last", "email")
_NUMERIC = {"smallint", "int2", "int", "integer", "bigint", "int4", "int8", "decimal", "numeric"}

_applied: set = set()


def _log(msg: str) -> None:
    try:
        import multiprocessing
        if multiprocessing.parent_process() is not None:
            return  # only the runner's main process reports
    except Exception:
        pass
    print(f"[fast_client] {msg}", file=sys.stderr, flush=True)


def files_md5() -> str:
    d = Path(__file__).resolve().parent
    return " ".join(f"{f.name}:{hashlib.md5(f.read_bytes()).hexdigest()[:8]}" for f in sorted(d.glob("*.py")))


# ---------------------------------------------------------------- 1. data generator
_POOLS: dict = {}  # (ddl, column) -> list of Faker-made values, shared by every generator of the process


def patch_datagen(mod) -> None:
    if "datagen" in _applied:
        return
    cls = mod.DynamicDataGenerator
    orig = cls.generate_value

    def _direct(col: dict) -> bool:
        name, col_type = col["name"].lower(), col["type"].split()[0]
        return col_type in _NUMERIC and not any(h in name for h in _NAME_HINTS)

    def generate_value(self, column_name: str):
        col = self.columns[column_name]
        if _direct(col):
            return orig(self, column_name)
        key = (self.ddl, column_name)
        pool = _POOLS.get(key)
        if pool is None:
            pool = _POOLS[key] = [orig(self, column_name) for _ in range(POOL_SIZE)]
        return random.choice(pool)

    cls.generate_value = generate_value
    _applied.add("datagen")


# ---------------------------------------------------------------- 2. SQL classifier
def patch_sql_parse(mod) -> None:
    if "sql_parse" in _applied:
        return
    orig = mod._remove_sql_comments

    def _remove_sql_comments(sql: str) -> str:
        if "--" not in sql and "/*" not in sql:
            return sql
        return orig(sql)

    mod._remove_sql_comments = _remove_sql_comments
    _applied.add("sql_parse")


# ---------------------------------------------------------------- 3. modified-key tracking
def patch_runner2(mod) -> None:
    if "runner2" in _applied:
        return
    cls = mod.WorkerContext

    def _keyset(self, branch):
        """The set for `branch`, rebuilt when the list changed behind its back (setup appends to it directly)."""
        lst = self._modified_keys.get(branch, [])
        sets = self.__dict__.setdefault("_fc_modified_sets", {})
        entry = sets.get(branch)
        if entry is None or entry[1] != len(lst):
            entry = [set(lst), len(lst)]
            sets[branch] = entry
        return entry

    def track_modified_key(self, key):
        b = self.current_branch
        entry = _keyset(self, b)
        if key not in entry[0]:
            self._modified_keys.setdefault(b, []).append(key)
            entry[0].add(key)
            entry[1] += 1

    def untrack_modified_key(self, key):
        b = self.current_branch
        if b in self._modified_keys:
            entry = _keyset(self, b)
            if key in entry[0]:
                self._modified_keys[b].remove(key)  # first occurrence only, as the original
                # the list may hold the key twice (setup appends directly): rebuild the set on next use
                # (untrack is rare: deletes only)
                self.__dict__["_fc_modified_sets"].pop(b, None)

    cls.track_modified_key = track_modified_key
    cls.untrack_modified_key = untrack_modified_key
    sync = patch_cross_node_start(mod)
    sample = patch_background_sample(mod)
    _applied.add("runner2")
    _log(f"active: Faker value pool ({POOL_SIZE}/column), comment-strip fast path, set-based key tracking"
         f"{'; cross-node start ' + sync if sync else ''}"
         f"{f'; background rows sampled 1/{sample}' if sample > 1 else ''}; {files_md5()}")


# ---------------------------------------------------------------- 3b. background row sampling (experiment 13)
def bg_sample() -> int:
    """BENCH_BG_SAMPLE=K: background clients record 1 operation in K (default 20 in multi-client mode, else off)."""
    v = os.environ.get("BENCH_BG_SAMPLE", "")
    if v.strip():
        return max(1, int(v))
    return 20 if os.environ.get("BENCH_SYNC_ADDR") else 1


def patch_background_sample(mod) -> int:
    """The runner keeps every result row in memory until the run ends. Background clients (OperationRunner.run_until,
    used only by them) running fast operations for minutes produce millions of rows: about 5 GB per client process at
    pgcopy's ~0.5 ms MIXED ops over a 7-minute window, which ran a 62 GB node out of memory (experiment 13,
    scaling_branch N = 256). With K > 1 a background client records only every K-th operation (recording is
    per thread, so the timed workers are untouched): the background load is unchanged, its statement-latency quantiles
    come from a 1/K sample, and its ops_in_window / throughput in the summary are 1/K of the real ones (multiply by
    K; scripts/workloads/multi_client/multi_scaling.py run-branch does)."""
    k = bg_sample()
    if k <= 1:
        return 1
    cls = mod.OperationRunner

    def run_until(self, stopped):
        rcol = self.context.db_tools.result_collector
        done = failed = 0
        first_error = ""
        i = 0
        try:
            while not stopped():
                rcol.set_recording(i % k == 0)
                i += 1
                try:
                    self.operation.execute(self.context)
                    done += 1
                except Exception as e:  # noqa: BLE001 (as the original)
                    failed += 1
                    first_error = first_error or f"{type(e).__name__}: {e}"[:300]
        finally:
            rcol.set_recording(True)
        return done, failed, first_error

    cls.run_until = run_until
    return k


# ---------------------------------------------------------------- 4. cross-node start
SYNC_ADDR = os.environ.get("BENCH_SYNC_ADDR", "")
SYNC_KEY = os.environ.get("BENCH_SYNC_KEY", "")
SYNC_TIMEOUT = float(os.environ.get("BENCH_SYNC_TIMEOUT", "3600"))


def sync_go() -> None:
    """Block until the coordinator says GO for SYNC_KEY; raise on ABORT, a closed connection or the timeout."""
    host, port = SYNC_ADDR.rsplit(":", 1)
    t0 = time.time()
    with socket.create_connection((host, int(port)), timeout=60) as s:
        s.sendall(f"{SYNC_KEY}\n".encode())
        s.settimeout(SYNC_TIMEOUT)
        line = s.makefile("r").readline().strip()
    if line != "GO":
        raise RuntimeError(f"cross-node start {SYNC_KEY}: {line or 'coordinator closed the connection'}")
    print(f"[fast_client] cross-node start {SYNC_KEY}: GO after {time.time() - t0:.2f}s", file=sys.stderr, flush=True)


def _gate(obj) -> None:
    """Once per client process: the first thread to call it waits for GO, the others wait for that thread."""
    st = obj.__dict__.setdefault("_fc_gate", {"lock": threading.Lock(), "event": threading.Event(),
                                               "first": True, "err": None})
    with st["lock"]:
        first, st["first"] = st["first"], False
    if first:
        try:
            sync_go()
        except BaseException as e:  # noqa: BLE001 (re-raised in every waiting thread)
            st["err"] = e
        st["event"].set()
    else:
        st["event"].wait()
    if st["err"] is not None:
        raise RuntimeError(f"cross-node start failed: {st['err']}")


def patch_cross_node_start(mod) -> str:
    if not (SYNC_ADDR and SYNC_KEY):
        return ""
    tp, ptp = mod.TimedPhase, mod.ProcessTimedPhase
    orig_mark = tp._mark_start

    def _mark_start(self):  # TimedPhase (one client process): the barrier action, before any worker is released
        sync_go()
        orig_mark(self)

    def wait(self):  # ProcessTimedPhase.wait with the cross-node gate between the local barrier and the start
        idx = self._barrier.wait()
        _gate(self)
        if idx == 0:
            with self._lock:
                self._times[0] = time.time()
                cpu = mod.server_cpu_seconds()
                self._times[2] = -1.0 if cpu is None else cpu
        with self._local_lock:
            if self._client_cpu[0] is None:
                self._client_cpu[0] = mod.self_cpu_seconds()

    tp._mark_start = _mark_start
    ptp.wait = wait
    return f"{SYNC_KEY}@{SYNC_ADDR}"


def patch_neon_local(mod) -> None:
    if "neon_local" in _applied or not (SYNC_ADDR and SYNC_KEY):
        return
    import hashlib as _h
    import secrets as _s

    def setup_run(db_name, log=print):
        """dblib.neon_local.setup_run without the tenant-wide purge (multi-client: the orchestrator purges)."""
        nonce = _s.token_hex(6)
        os.environ["NEON_LOCAL_RUN_NONCE"] = nonce
        root = _h.md5(f"{nonce}/{db_name}/root".encode()).hexdigest()
        os.environ["NEON_LOCAL_RUN_ROOT"] = root
        mod.create_timeline(root, mod.seed_timeline())
        log(f"neon_local run root {root} (branch '{mod.MAIN_BRANCH}') from seed {mod.seed_timeline()}, nonce {nonce} "
            "(multi-client: no purge)")
        return root

    def teardown_run(db_name, log=print):
        log("neon_local teardown: skipped (multi-client: the orchestrator purges after every node finished)")

    mod.setup_run, mod.teardown_run = setup_run, teardown_run
    _applied.add("neon_local")


def patch_pgcopy(mod) -> None:
    """dblib.pgcopy in multi-client mode (experiment 13): as patch_neon_local, setup_run copies this runner's run
    root without the cluster-wide purge (it would delete the other nodes' instances), teardown_run does nothing."""
    if "pgcopy" in _applied or not (SYNC_ADDR and SYNC_KEY):
        return

    def setup_run(db_name, log=print):
        root = mod.new_run(db_name)
        mod.create_run_root(root, log=lambda m: log(m + " (multi-client: no purge)"))
        return root

    def teardown_run(db_name, log=print):
        log("pgcopy teardown: skipped (multi-client: the orchestrator purges after every node finished)")

    mod.setup_run, mod.teardown_run = setup_run, teardown_run
    _applied.add("pgcopy")


# ---------------------------------------------------------------- 5. background-only nodes (experiment 13)
HOLD_KEY = os.environ.get("BENCH_HOLD_KEY", "")


def patch_branch_ops(mod) -> None:
    """With BENCH_HOLD_KEY (and BENCH_SYNC_ADDR) set, a node of a multi-node scaling_branch point only keeps its busy
    branches busy: its runner is the creator runner with --branch-create-ops 1, and that one create, instead of
    creating a branch, blocks until the coordinator answers GO for BENCH_HOLD_KEY, which it does when the measuring
    node's creator has finished. The node's timed window (and its background load) thus spans the measuring node's
    creates; it records no BRANCH row."""
    if "branch_ops" in _applied or not (SYNC_ADDR and HOLD_KEY):
        return
    cls = mod.BranchCreateOperation

    def execute(self, context):
        host, port = SYNC_ADDR.rsplit(":", 1)
        t0 = time.time()
        with socket.create_connection((host, int(port)), timeout=60) as s:
            s.sendall(f"{HOLD_KEY}\n".encode())
            s.settimeout(SYNC_TIMEOUT)
            line = s.makefile("r").readline().strip()
        print(f"[fast_client] hold {HOLD_KEY}: {line or 'closed'} after {time.time() - t0:.1f}s", file=sys.stderr,
              flush=True)

    cls.execute = execute
    _applied.add("branch_ops")


TARGETS = {"microbench.datagen": patch_datagen, "util.sql_parse": patch_sql_parse,
           "microbench.runner2": patch_runner2, "dblib.neon_local": patch_neon_local,
           "dblib.pgcopy": patch_pgcopy, "microbench.operations.branch": patch_branch_ops}
