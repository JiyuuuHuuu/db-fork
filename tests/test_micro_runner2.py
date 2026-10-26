"""runner2 pieces the branch-scaling study relies on, on the fake backend:
the generated CH-benCHmark source, the branch count carried on every row
(setup and main phase, sync and async), the thread-to-branch assignment
that excludes the root branch, and the latency quantiles in the summary."""

import asyncio
import itertools

import pytest
from google.protobuf import text_format

from dblib import result_collector as rc
from dblib import result_pb2 as rslt
from macrobench import task_pb2 as mtp
from macrobench.schema import CH_TABLES
from microbench import runner2
from microbench import task2_pb2 as tp
from microbench.operations.branch import BranchCreateOperation
from microbench.runner2 import (
    BackendInfo,
    BenchmarkConfig,
    BenchmarkExecutor,
    SetupPhase,
    SharedBranchManager,
    _copy_from_seed_db,
    _latency_quantiles,
    _seed_table_ref,
    seed_generated_database,
)
from tests.fake_backend import FakeSuite

MINI_SCHEMA = mtp.SchemaConfig(base=mtp.BaseSchema.CH_BENCH, scale_factor=1, items=50,
                               customers_per_district=5, orders_per_district=5,
                               suppliers=5, skip_foreign_keys=True, seed_batch_rows=50)


def _config(text: str) -> BenchmarkConfig:
    runner2.register_all_operations()
    proto = tp.TaskConfig()
    text_format.Parse(text, proto)
    return BenchmarkConfig(proto)


def _info() -> BackendInfo:
    return BackendInfo(default_branch_name="main", root_branch_name="main")


def _attach(suite, collector):
    """create_db_tools stand-in: the one fake suite, recording into the
    collector the caller passed."""
    suite.result_collector = collector
    return suite


# ---------------------------------------------------------------------------
# Generated data
# ---------------------------------------------------------------------------

def test_generated_source_seeds_every_ch_table_and_commits_base(monkeypatch):
    suite = FakeSuite(rc.ResultCollector())
    monkeypatch.setattr(runner2, "create_db_tools", lambda *a, **k: _attach(suite, a[3]))
    monkeypatch.setattr(FakeSuite, "close_connection", lambda self: None)
    gen = tp.GeneratedData(scale_factor=1, seed=3)
    stats = seed_generated_database(tp.Backend.DOLT, _info(), "d", gen, log=lambda m: None,
                                    schema=MINI_SCHEMA)
    assert stats["source"] == "generated" and stats["seed_db"] is None
    assert set(stats["rows"]) == set(CH_TABLES) and all(n > 0 for n in stats["rows"].values())
    assert [e["message"] for e in suite.log("main", limit=3, timed=False).value] == ["base"]
    rows = suite.exec(["SELECT count(*) FROM stock"], refs=["main"], timed=False)[0].rows
    assert rows[0][0] == stats["rows"]["stock"]
    # Seeding is untimed: nothing was recorded.
    assert suite.result_collector.results == []


def test_config_exposes_generated_source():
    cfg = _config('''
        run_id: "t" backend: DOLT table_name: "stock" scale_factor: 2
        database_setup { db_name: "d" generated { seed_db: "seed" } }
        operation_benchmark { operation: READ num_ops: 1 }''')
    assert cfg.source == "generated" and cfg.generated.seed_db == "seed"
    assert runner2._generated_schema(cfg.generated, cfg.scale_factor).scale_factor == 2
    with pytest.raises(ValueError):
        _config('''
            run_id: "t" backend: SEEKDB database_setup { db_name: "d" generated { seed_db: "seed" } }
            operation_benchmark { operation: READ num_ops: 1 }''')


def test_seed_table_ref_uses_the_adapters_revision_database_naming():
    assert _seed_table_ref(tp.Backend.DOLT, "seed", "stock") == '"seed/main".public.stock'
    assert _seed_table_ref(tp.Backend.DOLT_MYSQL, "seed", "stock") == "`seed/main`.`stock`"
    with pytest.raises(ValueError):
        _seed_table_ref(tp.Backend.NEON, "seed", "stock")


def test_copy_from_seed_db_visits_tables_in_dependency_order():
    statements = []

    class Db:
        def sql(self, q):
            statements.append(q)
            return [[7]] if q.startswith("SELECT") else None

    class Suite:
        def exec(self, script, refs, timed):
            assert timed is False and refs == ["main"]
            script(Db())
            return [type("R", (), {"raise_for_status": lambda self: None})()]

    counts = _copy_from_seed_db(Suite(), tp.Backend.DOLT_MYSQL, "seed", "main", log=lambda m: None)
    inserts = [q for q in statements if q.startswith("INSERT")]
    assert inserts == [f"INSERT INTO {t} SELECT * FROM `seed/main`.`{t}`" for t in CH_TABLES]
    assert counts == {t: 7 for t in CH_TABLES}


# ---------------------------------------------------------------------------
# Branch count on every row
# ---------------------------------------------------------------------------

@pytest.fixture
def seeded_suite():
    suite = FakeSuite(rc.ResultCollector())
    suite.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
                ("INSERT INTO t VALUES (?, ?)", (1, "a"))], timed=False)
    suite.commit("main", "init", timed=False)
    return suite


def test_setup_phase_records_branch_count_on_branch_and_connect_rows(monkeypatch, tmp_path, seeded_suite):
    suite = seeded_suite
    monkeypatch.setattr(runner2, "create_db_tools", lambda *a, **k: _attach(suite, a[3]))
    monkeypatch.setattr(FakeSuite, "close_connection", lambda self: None)
    monkeypatch.setattr(FakeSuite, "get_table_schema",
                        lambda self, t: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    monkeypatch.setattr(runner2, "DynamicDataGenerator", lambda ddl: None)
    monkeypatch.setattr(runner2.dbh, "get_pk_column_names", lambda conn, t: ["id"])
    cfg = _config('''
        run_id: "t" backend: DOLT table_name: "t"
        database_setup { db_name: "d" generated {} }
        operation_benchmark { operation: READ num_ops: 1
                              setup { num_branches: 4 branch_shape: FAN_OUT } }''')
    branches = SetupPhase(cfg, _info(), str(tmp_path)).execute()
    assert branches == ["main"] + [f"setup_branch_{i}" for i in range(1, 5)]

    import pyarrow.parquet as pq
    df = pq.read_table(tmp_path / "t_setup.parquet").to_pandas()
    branch_rows = df[df.op_type == rslt.OpType.BRANCH].sort_values("iteration_number")
    connect_rows = df[df.op_type == rslt.OpType.CONNECT].sort_values("iteration_number")
    assert list(branch_rows.branch_count) == [0, 1, 2, 3]
    assert list(connect_rows.branch_count) == [1, 2, 3, 4]


class _Ctx:
    """The parts of WorkerContext BranchCreateOperation uses."""

    thread_id = 0
    measure_storage = False

    def __init__(self, suite, root="main"):
        self.db_tools = suite
        self.backend_info = BackendInfo(default_branch_name=root, root_branch_name=root)
        self.branch_manager = SharedBranchManager([root])
        self._ids = itertools.count(1)

    @property
    def current_ref(self):
        return self.db_tools.current_ref

    def get_next_branch_id(self):
        return next(self._ids)

    def add_branch(self, name):
        self.branch_manager.add_branch(name)

    record_branch_count = runner2.WorkerContext.record_branch_count
    live_branch_count = runner2.WorkerContext.live_branch_count


def test_branch_create_rows_carry_the_live_branch_count(seeded_suite):
    ctx = _Ctx(seeded_suite)
    op = BranchCreateOperation()
    for _ in range(3):
        op.execute(ctx)
    rows = [r for r in seeded_suite.result_collector.results if r.op_type == rslt.OpType.BRANCH]
    assert [r.branch_count for r in rows] == [0, 1, 2]
    assert ctx.live_branch_count() == 3


def test_async_task_rows_inherit_the_threads_context():
    collector = rc.ResultCollector(run_id="t")
    collector.set_context(table_name="stock", table_schema="", initial_db_size=0, seed=5)
    collector.record_branch_count(7)

    async def emit():
        collector.record_num_keys_touched(3)
        collector.emit(rslt.OpType.READ, rslt.OpStatus.OK, latency=0.1, sql_query="SELECT 1")

    asyncio.run(emit())
    collector.emit(rslt.OpType.READ, rslt.OpStatus.OK, latency=0.1, sql_query="SELECT 2")
    task_row, thread_row = collector.results
    assert task_row.table_name == "stock" and task_row.branch_count == 7
    assert task_row.random_seed == 5 and task_row.num_keys_touched == 3
    # The task's counters do not leak into the thread's state.
    assert thread_row.branch_count == 7 and thread_row.num_keys_touched == 0


# ---------------------------------------------------------------------------
# Thread-to-branch assignment and summary metrics
# ---------------------------------------------------------------------------

def _executor(threads, setup_branches, root="main"):
    cfg = type("Cfg", (), {"num_threads": threads})()
    return BenchmarkExecutor(cfg, BackendInfo(default_branch_name=root, root_branch_name=root),
                             setup_branches, rc.ResultCollector())


def test_assignment_spreads_threads_over_setup_branches_only():
    ex = _executor(4, ["main", "setup_branch_1"])
    assert ex._assign_branches_to_threads() == {t: ["setup_branch_1"] for t in range(4)}
    assert ex.loaded_branches() == 1

    ex = _executor(2, ["main", "setup_branch_1", "setup_branch_2", "setup_branch_3"])
    assert ex._assign_branches_to_threads() == {0: ["setup_branch_1", "setup_branch_3"],
                                                1: ["setup_branch_2"]}
    assert ex.loaded_branches() == 2

    # FILE_COPY never lists the root; 1 thread and root-only lists stay on the default branch.
    ex = _executor(2, ["setup_branch_1", "setup_branch_2"], root="postgres")
    assert ex._assign_branches_to_threads() == {0: ["setup_branch_1"], 1: ["setup_branch_2"]}
    assert _executor(1, ["main", "setup_branch_1"])._assign_branches_to_threads() == {}
    assert _executor(3, ["main"])._assign_branches_to_threads() == {}
    assert _executor(3, ["main"]).loaded_branches() == 1


def test_latency_quantiles():
    assert _latency_quantiles([]) == {"n": 0}
    rows = [type("R", (), {"latency": v})() for v in (0.1, 0.2, 0.3, 0.4)]
    q = _latency_quantiles(rows)
    assert q["n"] == 4 and q["max"] == 0.4 and abs(q["mean"] - 0.25) < 1e-9
    assert 0.2 <= q["p50"] <= 0.3 and q["p95"] <= q["p99"] <= q["max"]


# ---------------------------------------------------------------------------
# Client processes (num_processes > 1)
# ---------------------------------------------------------------------------

def _timed_worker_process(shared, n_threads, out_q):
    """Spawned helper: n_threads threads pass the shared barrier, work, finish."""
    import threading as th
    import time as tm
    from microbench.runner2 import ProcessTimedPhase
    timed = ProcessTimedPhase(shared, n_threads)

    def work():
        timed.wait()
        tm.sleep(0.05)
        timed.finish()

    ts = [th.Thread(target=work) for _ in range(n_threads)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    out_q.put(timed.client_cpu_seconds())


def test_process_timed_phase_spans_processes(monkeypatch):
    import multiprocessing as mp
    import os
    monkeypatch.setenv("DBFORK_SERVER_PID", str(os.getpid()))   # sample this process as the "server"
    ctx = mp.get_context("spawn")
    times = ctx.Array("d", [0.0, 0.0, -1.0, -1.0], lock=False)
    done = ctx.Value("i", 0, lock=False)
    shared = (ctx.Barrier(4), times, done, ctx.Lock(), 4)
    q = ctx.Queue()
    ps = [ctx.Process(target=_timed_worker_process, args=(shared, 2, q)) for _ in range(2)]
    for p in ps:
        p.start()
    cpus = [q.get(timeout=60) for _ in ps]
    for p in ps:
        p.join(timeout=60)
    assert done.value == 4
    assert 0 < times[0] < times[1] and times[1] - times[0] >= 0.04
    assert times[2] >= 0 and times[3] >= times[2]
    assert all(c is not None and c >= 0 for c in cpus)


def test_merge_support_and_process_limits():
    merged = runner2._merge_support([
        {"unsupported_ops": [], "op_status_counts": {"READ": {"ok": 3}}},
        {"unsupported_ops": ["MERGE"], "op_status_counts": {"READ": {"ok": 2, "failed": 1}}},
    ])
    assert merged == {"workflow_supported": False, "unsupported_ops": ["MERGE"],
                      "op_status_counts": {"READ": {"ok": 5, "failed": 1}}}
    cfg = _config('''
        run_id: "t" backend: DOLT_MYSQL num_threads: 8 num_processes: 32
        database_setup { db_name: "d" generated {} }
        operation_benchmark { operation: READ num_ops: 1 }''')
    assert cfg.num_processes == 8          # never more processes than threads
    with pytest.raises(ValueError):
        _config('''
            run_id: "t" backend: FILE_COPY num_threads: 8 num_processes: 2
            database_setup { db_name: "d" generated {} }
            operation_benchmark { operation: READ num_ops: 1 }''')


def test_server_cpu_seconds_reads_proc(monkeypatch):
    import os
    monkeypatch.delenv("DBFORK_SERVER_PID", raising=False)
    assert runner2.server_cpu_seconds() is None
    monkeypatch.setenv("DBFORK_SERVER_PID", str(os.getpid()))
    a = runner2.server_cpu_seconds()
    sum(i * i for i in range(300_000))
    assert a is not None and runner2.server_cpu_seconds() >= a


def test_cached_meta_computes_once_under_concurrency():
    import threading as th
    import time as tm
    calls = []

    def compute():
        calls.append(1)
        tm.sleep(0.05)
        return ["a", "b"]

    key = ("test", "db", "columns", "t_once")
    out = []
    ts = [th.Thread(target=lambda: out.append(runner2._cached_meta(key, compute))) for _ in range(32)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert len(calls) == 1 and len(out) == 32 and all(o is out[0] for o in out)


def test_client_process_main_runs_workers_and_reports(monkeypatch, tmp_path):
    """One client process's entry point, run in-process on the fake backend:
    its threads pass the shared barrier, run their ops and the process
    writes its part file and reports rows, CPU and no error."""
    import multiprocessing as mp
    import pyarrow.parquet as pq

    def make_suite(*a, **k):
        s = FakeSuite(a[3])
        s.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"] +
               [("INSERT INTO t VALUES (?, ?)", (i, "x")) for i in range(1, 21)], timed=False)
        return s

    monkeypatch.setattr(runner2, "create_db_tools", make_suite)
    monkeypatch.setattr(FakeSuite, "get_table_schema",
                        lambda self, t: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    monkeypatch.setattr(runner2.dbh, "get_pk_column_names", lambda conn, t: ["id"])
    monkeypatch.setattr(runner2.dbh, "get_pk_values", lambda conn, t, cols: [(i,) for i in range(1, 21)])
    cfg = _config('''
        run_id: "mp" backend: DOLT_MYSQL table_name: "t" num_threads: 2 num_processes: 2
        database_setup { db_name: "d_mp" generated {} }
        operation_benchmark { operation: READ num_ops: 5 }''')
    ctx = mp.get_context("spawn")
    times = ctx.Array("d", [0.0, 0.0, -1.0, -1.0], lock=False)
    done = ctx.Value("i", 0, lock=False)
    shared = (ctx.Barrier(2), times, done, ctx.Lock(), 2)
    q = ctx.Queue()
    runner2._client_process_main(0, cfg._proto.SerializeToString(), _info(), [], [0, 1],
                                 {0: [], 1: []}, 7, str(tmp_path), shared, q)
    out = q.get(timeout=10)
    assert out["error"] is None, out["error"]
    assert out["part"] and out["client_cpu"] is not None and out["failed"] == []
    df = pq.read_table(out["part"]).to_pandas()
    reads = df[(df.op_name == "READ") & (df.status == "OK")]
    assert sorted(reads.thread_id.unique()) == [0, 1] and len(reads) == 10
    assert done.value == 2 and times[0] < times[1]


def test_background_load_keeps_setup_branches_busy(monkeypatch, tmp_path):
    """BRANCH_CREATE measured by one client while two background clients run
    MIXED on the two setup branches: measured stats count only the creates,
    background rows carry their own thread ids and branches, and the
    background clients stop when the measured phase ends."""
    def make_suite(*a, **k):
        s = FakeSuite(a[3])
        s.exec(["CREATE TABLE t (id INTEGER PRIMARY KEY, v VARCHAR(20))"] +
               [("INSERT INTO t VALUES (?, ?)", (i, "x")) for i in range(1, 21)], timed=False)
        for b in ("setup_branch_1", "setup_branch_2"):
            s.branch(b, "main", timed=False)
        return s

    monkeypatch.setattr(runner2, "create_db_tools", make_suite)
    monkeypatch.setattr(FakeSuite, "get_table_schema",
                        lambda self, t: "CREATE TABLE t (\n  id integer NOT NULL,\n  v varchar(20)\n);")
    monkeypatch.setattr(runner2.dbh, "get_pk_column_names", lambda conn, t: ["id"])
    monkeypatch.setattr(runner2.dbh, "get_pk_values", lambda conn, t, cols: [(i,) for i in range(1, 21)])
    monkeypatch.setattr(runner2.dbh, "get_all_columns", lambda conn, t: ["id", "v"])
    cfg = _config('''
        run_id: "bg" backend: DOLT_MYSQL table_name: "t" num_threads: 1
        background_load: true background_operation: MIXED
        database_setup { db_name: "d_bg" generated {} }
        operation_benchmark { operation: BRANCH_CREATE num_ops: 5 setup { num_branches: 2 } }''')
    collector = rc.ResultCollector(run_id="bg", output_dir=str(tmp_path))
    ex = BenchmarkExecutor(cfg, BackendInfo(default_branch_name="setup_branch_2", root_branch_name="main"),
                           ["main", "setup_branch_1", "setup_branch_2"], collector)
    m = ex.execute()
    assert m["total_ops"] == 5 and m["latency"]["ops"]["n"] == 5
    bg = m["background"]
    assert bg["operation"] == "MIXED" and bg["clients"] == 2 and bg["failed"] == 0, bg["first_error"]
    assert bg["ops_in_window"] > 0   # background clients were working while the creates were timed
    rows = collector.results
    bg_rows = [r for r in rows if r.thread_id >= 1 and r.op_type in (rslt.OpType.READ, rslt.OpType.UPDATE)]
    assert {r.thread_id for r in bg_rows} == {1, 2}
    assert {r.ref for r in bg_rows} == {"setup_branch_1", "setup_branch_2"}
    assert all(r.op_type == rslt.OpType.BRANCH for r in rows if r.thread_id == 0 and r.op_type != rslt.OpType.CONNECT)
    assert ex._bg.stopped()


# ---------------------------------------------------------------------------
# Experiment 3 additions: connect_every, parallel setup, remote CPU
# ---------------------------------------------------------------------------

def test_branch_create_connect_every_records_connects_and_returns(seeded_suite):
    ctx = _Ctx(seeded_suite)
    ctx.config = _config('''
        run_id: "t" backend: DOLT database_setup { db_name: "d" generated {} }
        operation_benchmark { operation: BRANCH_CREATE num_ops: 1
                              branch_op_config { connect_every: 2 } }''')
    ctx.connect = lambda branch, timed=True: runner2.WorkerContext.connect(ctx, branch, timed)
    type(ctx).current_branch = runner2.WorkerContext.current_branch
    seeded_suite.exec([], refs=["main"], timed=False)
    op = BranchCreateOperation()
    for _ in range(4):
        op.execute(ctx)
    rows = seeded_suite.result_collector.results
    assert sum(r.op_type == rslt.OpType.BRANCH for r in rows) == 4
    connects = [r for r in rows if r.op_type == rslt.OpType.CONNECT]
    assert [r.ref for r in connects] == ["branch_tid0_2", "branch_tid0_4"]   # the returns are untimed
    assert seeded_suite.current_ref.branch == "main"


class _StubSuite(runner2.DBToolSuite):
    """Thread-safe stand-in recording branch creates; every suite shares one list."""

    created: list = []

    def _connect_impl(self, ref):
        pass

    def _branch_impl(self, name, from_ref):
        import time
        time.sleep(0.02)   # long enough for every setup worker to take a branch
        type(self).created.append((name, from_ref.branch))

    def _storage_bytes(self):
        return 0


def test_parallel_fan_out_setup_matches_the_serial_result(monkeypatch, tmp_path):
    _StubSuite.created = []
    monkeypatch.setattr(runner2, "create_db_tools",
                        lambda *a, **k: _StubSuite(None, a[3]))
    monkeypatch.setattr(_StubSuite, "get_table_schema", lambda self, t: "CREATE TABLE t (id INT)",
                        raising=False)
    monkeypatch.setattr(runner2, "DynamicDataGenerator", lambda ddl: None)
    monkeypatch.setattr(runner2.dbh, "get_pk_column_names", lambda conn, t: ["id"])
    monkeypatch.setattr(SetupPhase, "_perform_branch_data_ops", lambda *a, **k: None)
    cfg = _config('''
        run_id: "t" backend: DOLT table_name: "t"
        database_setup { db_name: "d" generated {} }
        operation_benchmark { operation: READ num_ops: 1
                              setup { num_branches: 9 branch_shape: FAN_OUT setup_parallelism: 4 } }''')
    info = _info()
    branches = SetupPhase(cfg, info, str(tmp_path)).execute()
    assert branches == ["main"] + [f"setup_branch_{i}" for i in range(1, 10)]
    assert sorted(_StubSuite.created) == sorted((f"setup_branch_{i}", "main") for i in range(1, 10))
    assert info.default_branch_name == "setup_branch_9"

    import pyarrow.parquet as pq
    df = pq.read_table(tmp_path / "t_setup.parquet").to_pandas()
    assert (df.op_type == rslt.OpType.BRANCH).sum() == 9
    assert (df.op_type == rslt.OpType.CONNECT).sum() == 9
    assert set(df.thread_id) == {1, 2, 3, 4}


def test_remote_cpu_sampler_interpolates_the_window(monkeypatch):
    monkeypatch.setenv("DBFORK_REMOTE_CPU", "ps=http://x/cpu?proc=a;sk=http://y/cpu")
    s = runner2.RemoteCpuSampler()
    assert [t[0] for t in s.targets] == ["ps", "sk"]
    s.samples["ps"] = [{"_t": t, "proc_s": 2.0 * t, "node_busy_s": 3.0 * t, "node_iowait_s": 0.0,
                        "disk_busy_s": 0.5 * t, "ncpu": 40} for t in range(0, 11)]
    s.samples["sk"] = []
    u = s.usage(2.5, 7.5)
    assert u["ps"]["proc_cores"] == pytest.approx(2.0)
    assert u["ps"]["node_cores"] == pytest.approx(3.0)
    assert u["ps"]["disk_util"] == pytest.approx(0.5)
    assert u["sk"]["proc_cores"] is None
    monkeypatch.delenv("DBFORK_REMOTE_CPU")
    assert runner2.RemoteCpuSampler().usage(0, 1) is None


def test_range_query_bounds_select_exactly_the_range():
    import random
    from types import SimpleNamespace
    suite = FakeSuite(rc.ResultCollector())
    keys = [(a, b) for a in (1, 2, 3) for b in range(1, 41)]
    suite.exec(["CREATE TABLE t2 (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a, b))"]
               + [("INSERT INTO t2 VALUES (?, ?, 'x')", k) for k in keys], timed=False)
    ctx = SimpleNamespace(get_pk_columns=lambda t: ["a", "b"], get_pk_values=lambda t: list(keys),
                          rnd=random.Random(7), _meta_key=lambda kind, t: ("range-bounds-test", kind, t))
    crossed = 0
    for _ in range(200):
        info = runner2.WorkerContext.prepare_range_query(ctx, "t2", 25, "range read")
        rows = suite.exec([(f"SELECT a, b FROM t2 WHERE {info['where_clause']} ORDER BY a, b",
                            info["params"])], timed=False)[0].rows
        assert [tuple(r) for r in rows] == info["keys_in_range"]
        crossed += info["keys_in_range"][0][0] != info["keys_in_range"][-1][0]
    assert crossed > 0   # ranges spanning two values of the leading column were covered


def test_range_query_sorts_the_keys_once_per_process():
    import random
    from types import SimpleNamespace

    class Keys(list):
        iterations = 0

        def __iter__(self):
            type(self).iterations += 1
            return super().__iter__()

    keys = Keys([(a, b) for a in (3, 1, 2) for b in range(40, 0, -1)])
    ctx = SimpleNamespace(get_pk_columns=lambda t: ["a", "b"], get_pk_values=lambda t: keys,
                          rnd=random.Random(3), _meta_key=lambda kind, t: ("sort-once-test", kind, t))
    for _ in range(50):
        info = runner2.WorkerContext.prepare_range_query(ctx, "t3", 10, "range read")
        assert info["keys_in_range"] == sorted(info["keys_in_range"])
    assert Keys.iterations == 1


def test_prewarm_sql_runs_unrecorded_before_the_warm_up(monkeypatch):
    calls = []

    class Collector:
        recording = True

        def set_recording(self, on):
            self.recording = on

    class Ctx:
        thread_id = 1
        current_branch = "b1"
        shared_progress = None
        result_collector = Collector()

        def run_sql(self, sql, params=None, timed=True):
            calls.append(("sql", sql, timed, self.result_collector.recording))

    class Op:
        def execute(self, ctx):
            calls.append(("op", ctx.result_collector.recording))

    runner = object.__new__(runner2.OperationRunner)
    runner.context, runner.operation = Ctx(), Op()
    monkeypatch.setenv("DBFORK_PREWARM_SQL", "SET x = 1; SELECT pg_prewarm('t') ;")
    runner.execute_multiple(2, warmup_ops=1)
    assert calls[:2] == [("sql", "SET x = 1", False, False), ("sql", "SELECT pg_prewarm('t')", False, False)]
    assert calls[2:] == [("op", False), ("op", True), ("op", True)]
    calls.clear()
    monkeypatch.delenv("DBFORK_PREWARM_SQL")
    runner.execute_multiple(1)
    assert calls == [("op", True)]


def test_background_clients_run_the_prewarm_before_their_first_op(monkeypatch):
    calls = []

    class Collector:
        recording = True

        def set_recording(self, on):
            self.recording = on

    class Ctx:
        thread_id = 7
        current_branch = "setup_branch_1"
        shared_progress = None
        result_collector = Collector()

        def run_sql(self, sql, params=None, timed=True):
            calls.append(("sql", sql, timed, self.result_collector.recording))

    runner = object.__new__(runner2.OperationRunner)
    runner.context = Ctx()
    monkeypatch.setenv("DBFORK_PREWARM_SQL", "SELECT pg_prewarm('t')")
    runner.run_prewarm()
    assert calls == [("sql", "SELECT pg_prewarm('t')", False, False)]
    assert Ctx.result_collector.recording
    src = open(runner2.__file__).read()
    bg = src[src.index("runner = OperationRunner(self.config, ctx, op_type=self.config.background_operation)"):]
    assert bg.index("runner.run_prewarm()") < bg.index("runner.run_until(")
