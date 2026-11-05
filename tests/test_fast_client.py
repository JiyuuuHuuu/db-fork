"""microbench/fast_client: the patches change cost, not behaviour (same classifier results, same modified-key list,
numeric values per row, Faker-made text), and they reach the runner's spawned processes only when opted in."""
import importlib.util
import os
import random
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FC = ROOT / "microbench" / "fast_client"


def _patches():
    spec = importlib.util.spec_from_file_location("_fc_patches_test", FC / "patches.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _fresh(modname):
    sys.modules.pop(modname, None)
    return importlib.import_module(modname)


SQLS = [
    "SELECT * FROM stock WHERE s_w_id = %s AND s_i_id = %s;",
    "INSERT INTO stock (a, b) VALUES (%(a)s, %(b)s);",
    "UPDATE stock SET a = %(a)s WHERE b = %(b)s;",
    "-- lead comment\nDELETE FROM t WHERE x = 1",
    "/* c */ WITH x AS (SELECT 1) SELECT * FROM x",
    "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
    "INSERT INTO t VALUES ('a -- not a comment', 'b /* nor this */')",
    "  select 1; update t set a = 1",
    "",
    "   ",
]


def test_classifier_same_results():
    import util.sql_parse as sp
    want = [sp.get_sql_operation_keyword(s) for s in SQLS]
    sp2 = _fresh("util.sql_parse")
    _patches().patch_sql_parse(sp2)
    assert [sp2.get_sql_operation_keyword(s) for s in SQLS] == want
    _fresh("util.sql_parse")


def test_key_tracking_same_list():
    import microbench.runner2 as r2
    ops = []
    rnd = random.Random(7)
    for _ in range(3000):
        ops.append((rnd.choice(["track", "track", "untrack", "append"]), rnd.choice(["b1", "b2"]),
                    (rnd.randint(1, 60), rnd.randint(1, 3))))

    def run(cls):
        ctx = cls.__new__(cls)
        ctx._modified_keys = {}
        for op, b, k in ops:
            ctx.current_branch = b
            if op == "track":
                ctx.track_modified_key(k)
            elif op == "untrack":
                ctx.untrack_modified_key(k)
            else:  # setup's direct append (runner2 line ~1961)
                ctx._modified_keys.setdefault(b, []).append(k)
        return ctx._modified_keys

    class Orig(r2.WorkerContext):
        current_branch = None

    want = run(Orig)

    class Patched(r2.WorkerContext):
        current_branch = None

    p = _patches()
    fake = type("M", (), {"WorkerContext": Patched})
    p.patch_runner2(fake)
    assert run(Patched) == want


def test_datagen_pool():
    import microbench.datagen as dg
    ddl = ("CREATE TABLE stock (\ns_i_id integer,\ns_w_id smallint,\ns_dist_01 char(24),\ns_data varchar(50),\n"
           "s_city varchar(20),\nPRIMARY KEY (s_w_id, s_i_id)\n)")
    p = _patches()
    p.POOL_SIZE = 64
    mod = _fresh("microbench.datagen")
    p.patch_datagen(mod)
    g = mod.DynamicDataGenerator(ddl)
    rows = [g.generate_row() for _ in range(2000)]
    assert len({r["s_i_id"] for r in rows}) > 1900          # numeric: still per row
    assert len({r["s_dist_01"] for r in rows}) <= 64         # text: from the pool
    assert all(isinstance(r["s_dist_01"], str) and 0 < len(r["s_dist_01"]) <= 24 for r in rows)
    assert all(len(r["s_data"]) <= 50 and len(r["s_city"]) <= 20 for r in rows)
    _fresh("microbench.datagen")


def _child_report(env_extra):
    code = ("import multiprocessing as mp\n"
            "def f(q):\n"
            "    import microbench.runner2 as r, util.sql_parse as s\n"
            "    q.put((r.WorkerContext.track_modified_key.__module__, s._remove_sql_comments.__module__))\n"
            "if __name__ == '__main__':\n"
            "    ctx = mp.get_context('spawn'); q = ctx.Queue(); p = ctx.Process(target=f, args=(q,)); p.start()\n"
            "    print(q.get()); p.join()\n")
    env = {k: v for k, v in os.environ.items() if k not in ("BENCH_FAST_CLIENT", "PYTHONPATH")}
    env.update(env_extra)
    import tempfile
    with tempfile.TemporaryDirectory() as d:  # spawn re-imports the main module, so it must be a file
        script = Path(d) / "child_report.py"
        script.write_text(code)
        out = subprocess.run([sys.executable, str(script)], cwd=ROOT, env=env, capture_output=True, text=True,
                             timeout=120)
    assert out.returncode == 0, out.stderr
    return out.stdout.strip()


def test_spawned_children_patched_only_when_opted_in():
    pp = f"{FC}{os.pathsep}{ROOT}"
    on = _child_report({"BENCH_FAST_CLIENT": "1", "PYTHONPATH": pp})
    assert "_dbfork_fast_client_patches" in on
    off = _child_report({"PYTHONPATH": pp})
    assert "_dbfork_fast_client_patches" not in off


def test_dash_m_runner2_runs_patched():
    """python -m microbench.runner2 (run_throughput_bench.sh's form) imports the patched module and runs main()."""
    env = {k: v for k, v in os.environ.items() if k not in ("BENCH_FAST_CLIENT", "PYTHONPATH")}
    env.update({"BENCH_FAST_CLIENT": "1", "PYTHONPATH": str(FC)})
    out = subprocess.run([sys.executable, "-m", "microbench.runner2", "--help"], cwd=ROOT, env=env,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    assert "[fast_client] active" in out.stderr and "--config" in out.stdout
    env.pop("BENCH_FAST_CLIENT")
    out = subprocess.run([sys.executable, "-m", "microbench.runner2", "--help"], cwd=ROOT, env=env,
                         capture_output=True, text=True, timeout=120)
    assert out.returncode == 0 and "[fast_client]" not in out.stderr
