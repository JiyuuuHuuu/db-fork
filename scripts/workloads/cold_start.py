#!/usr/bin/env python3
"""Cold start: point-read latency on a new branch while its caches fill (experiment 5, sub-experiment D).

Per run k (1..--runs), with --pause idle seconds between runs:

Neon (experiment 5's cold_start.py): a fresh timeline of the run root (one storage-controller call, timed:
create_ms), its compute started by the first compute agent of NEON_LOCAL_AGENTS (timed: compute_start_ms; the
agent's own figure: agent_start_ms), the first psycopg2 connection (timed: first_connect_ms), then --reads point
reads back to back on that connection, each timed on the client. The compute starts with empty shared_buffers and
an empty LFC (compute opts {lfc_mb: LFC_MB, statement_log: true}, plus the shards of NEON_LOCAL_PS_OPTS); the
compute is stopped after the run (the agent keeps its log).

Doltgres ("a new branch on a warm server"): one admin connection on a fresh database copied from the seed
(SEED_DB, the runner's own helpers: BackendManager + seed_generated_database, i.e. INSERT ... SELECT from
"<seed>/main" and a "base" commit), then per run: SELECT dolt_branch('cold_<k>', 'main') (timed: create_ms), a NEW
connection to the database (timed: first_connect_ms) and SELECT dolt_checkout('cold_<k>') on it (timed:
checkout_ms; dblib/dolt.py switches branches the same way), then the same reads. Branches and the database are
dropped at the end.

Reads: SELECT * FROM stock WHERE s_w_id = %s AND s_i_id = %s with w uniform in [1, W], i in [1, 100000],
random.Random(1000 + k) (the runner's READ; keys are dense, no key scan).

    uv run python scripts/workloads/cold_start.py --topology T --backend neon|doltgres --outdir DIR \\
        [--runs COLD_RUNS] [--reads COLD_READS] [--pause COLD_PAUSE_S] [--smoke] [--dry-run]

Run it on the client node under the client placement (scripts/workloads/run_all.sh does both). Writes
DIR/<backend>/cold_start/run<k>.parquet (i, t_start, t_end, latency_s, w, i_id) and runs.json (W, opts, one entry
per run). Smoke (1 run x 3,000 reads, 5 s pause) writes to DIR/<backend>/smoke/cold_start/.
"""
import argparse
import json
import os
import random
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts" / "setup"))
os.chdir(REPO)

import topo as topo_mod  # noqa: E402

READ_SQL = "SELECT * FROM stock WHERE s_w_id = %s AND s_i_id = %s;"
CLEARED = ("DOLT_HOST", "DOLT_PORT", "DOLT_USER", "DOLT_PASSWORD", "DOLT_DATA_DIR")


# ---------------------------------------------------------------------------------------------------------------
# Shared with ceiling_check.py
# ---------------------------------------------------------------------------------------------------------------

def workload_env(topology: str, backend: str, dry: bool = False) -> dict:
    """The topology's workload variables (topo.py shell), with the backend's environment file loaded into
    os.environ after clearing inherited NEON_LOCAL_* / DBFORK_* / DOLT_* (DBFORK_SERVER_PID, set by the driver, is
    kept). Call before importing dblib (dblib/dolt.py reads DOLT_* at import)."""
    v = topo_mod.Topo(topology).shell_vars(backend)
    for k in list(os.environ):
        if k.startswith("NEON_LOCAL_") or (k.startswith("DBFORK_") and k != "DBFORK_SERVER_PID") or k in CLEARED:
            del os.environ[k]
    env_file = Path(v["ENV_FILE"])
    if env_file.exists():
        from dotenv import dotenv_values
        for k, val in dotenv_values(env_file).items():
            if val is not None:
                os.environ[k] = val
    elif dry:
        print(f"warning: no environment file {env_file}; dry run continues without it", file=sys.stderr)
    else:
        raise SystemExit(f"no environment file {env_file}: run scripts/setup/{backend}.py {topology} env-init")
    os.environ.setdefault("NEON_LOCAL_PURGE_PARALLEL", "64")
    return v


def out_root(outdir: str, backend: str, workload: str, smoke: bool) -> Path:
    base = Path(outdir).resolve() / backend
    return base / "smoke" / workload if smoke else base / workload


def neon_compute_opts(lfc_mb: int, statement_log: bool, pgdata_on_disk: bool = False) -> dict:
    o = json.loads(os.environ.get("NEON_LOCAL_PS_OPTS") or "{}")
    o.update(lfc_mb=int(lfc_mb), statement_log=statement_log)
    if pgdata_on_disk:
        o["pgdata_on_disk"] = True
    return o


def dolt_fresh_db(db_name: str, seed_db: str, W: int):
    """A new database db_name holding a copy of seed_db's tables, committed as "base" (what runner2 does before
    every run). Returns the BackendManager whose cleanup() drops it (and purges dropped databases)."""
    from microbench import task2_pb2 as tp
    from microbench.runner2 import (BackendInfo, BackendManager, BackendSetup, _database_exists,
                                    seed_generated_database)
    probe = BackendInfo(default_branch_name="main", root_branch_name="main")
    if not _database_exists(tp.Backend.DOLT, probe, seed_db):
        raise SystemExit(f"seed database {seed_db} not found: run scripts/setup/doltgres.py TOPO seed")
    setup = tp.DatabaseSetup(db_name=db_name, cleanup=True)
    setup.generated.scale_factor = W
    setup.generated.seed_db = seed_db
    mgr = BackendManager(BackendSetup(tp.Backend.DOLT, setup))
    info = mgr.setup()
    try:
        seed_generated_database(tp.Backend.DOLT, info, db_name, setup.generated, W)
    except BaseException:
        mgr.cleanup()
        raise
    return mgr


def dolt_uri(db: str) -> str:
    from dblib.dolt import DoltToolSuite
    return DoltToolSuite.get_initial_connection_uri(db)


# ---------------------------------------------------------------------------------------------------------------
# Cold start
# ---------------------------------------------------------------------------------------------------------------

def reads(conn, k: int, n: int, W: int, label: str) -> list:
    cur = conn.cursor()
    rnd = random.Random(1000 + k)
    rows = []
    for i in range(n):
        w, iid = rnd.randint(1, W), rnd.randint(1, 100000)
        t0 = time.time()
        p0 = time.perf_counter()
        cur.execute(READ_SQL, (w, iid))
        cur.fetchall()
        lat = time.perf_counter() - p0
        rows.append((i, t0, t0 + lat, lat, w, iid))
        if i % 5000 == 0:
            print(f"{label} run {k}: read {i}, {lat * 1e3:.1f} ms", flush=True)
    cur.close()
    return rows


def finish(rows, k: int, out: Path, meta: dict) -> dict:
    import pandas as pd
    df = pd.DataFrame(rows, columns=["i", "t_start", "t_end", "latency_s", "w", "i_id"])
    df.to_parquet(out / f"run{k}.parquet", index=False)
    n = len(df)
    meta.update(t_reads_start=rows[0][1], t_reads_end=rows[-1][2], reads=n,
                median_first_100_ms=float(df.latency_s.head(100).median() * 1e3))
    if n >= 10000:
        meta["median_last_5000_ms"] = float(df.latency_s.tail(5000).median() * 1e3)
    else:   # short (smoke) runs: the last 10%
        meta["median_last_10pct_ms"] = float(df.latency_s.tail(max(1, n // 10)).median() * 1e3)
    print(json.dumps(meta), flush=True)
    return meta


def neon_run(k, args, W, out):
    import psycopg2
    from dblib import neon_local as nl
    name = f"cold_{k}"
    tl = nl.timeline_id_for(name)
    t_create0 = time.time()
    nl.create_timeline(tl, nl.timeline_id_for(nl.MAIN_BRANCH))
    t_create1 = time.time()
    r = nl.ensure_compute(tl)
    t_ready = time.time()
    conn = psycopg2.connect(host=r["host"], port=r["port"], dbname=os.environ.get("NEON_LOCAL_DB", "bench"),
                            user=os.environ.get("NEON_LOCAL_PG_USER", "bench"),
                            password=os.environ.get("NEON_LOCAL_PG_PASSWORD", ""), connect_timeout=60)
    conn.autocommit = True
    t_conn = time.time()
    rows = reads(conn, k, args.reads, W, "neon")
    conn.close()
    nl.stop_compute(tl)   # the agent keeps the compute's log in its archive
    meta = {"run": k, "branch": name, "timeline": tl, "host": r["host"], "agent_start_ms": r.get("start_ms"),
            "t_create_start": t_create0, "t_create_end": t_create1, "t_compute_ready": t_ready,
            "t_connected": t_conn, "create_ms": (t_create1 - t_create0) * 1e3,
            "compute_start_ms": (t_ready - t_create1) * 1e3, "first_connect_ms": (t_conn - t_ready) * 1e3}
    return finish(rows, k, out, meta)


def dolt_run(k, args, W, out, admin, db):
    import psycopg2
    name = f"cold_{k}"
    acur = admin.cursor()
    t_create0 = time.time()
    acur.execute("SELECT dolt_branch(%s, 'main');", (name,))
    acur.fetchall()
    t_create1 = time.time()
    conn = psycopg2.connect(dolt_uri(db), connect_timeout=60)
    conn.autocommit = True
    t_conn = time.time()
    with conn.cursor() as cur:
        cur.execute("SELECT dolt_checkout(%s);", (name,))
        cur.fetchall()
    t_checkout = time.time()
    rows = reads(conn, k, args.reads, W, "doltgres")
    conn.close()
    meta = {"run": k, "branch": name, "database": db, "t_create_start": t_create0, "t_create_end": t_create1,
            "t_connected": t_conn, "t_checked_out": t_checkout, "create_ms": (t_create1 - t_create0) * 1e3,
            "first_connect_ms": (t_conn - t_create1) * 1e3, "checkout_ms": (t_checkout - t_conn) * 1e3}
    return finish(rows, k, out, meta)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topology", required=True)
    ap.add_argument("--backend", required=True, choices=["neon", "doltgres"])
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--runs", type=int, help="default: the topology's cold_runs (smoke: 1)")
    ap.add_argument("--reads", type=int, help="default: the topology's cold_reads (smoke: 3000)")
    ap.add_argument("--pause", type=float, help="idle seconds after each run (default cold_pause_s; smoke: 5)")
    ap.add_argument("--smoke", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="print the plan, connect to nothing")
    args = ap.parse_args()
    v = workload_env(args.topology, args.backend, dry=args.dry_run)
    W = int(v["W"])
    args.runs = args.runs or (1 if args.smoke else int(v["COLD_RUNS"]))
    args.reads = args.reads or (3000 if args.smoke else int(v["COLD_READS"]))
    if args.pause is None:
        args.pause = 5.0 if args.smoke else float(v["COLD_PAUSE_S"])
    out = out_root(args.outdir, args.backend, "cold_start", args.smoke)
    if args.backend == "neon":
        opts = neon_compute_opts(int(v["LFC_MB"]), statement_log=True)
        agents = [a.strip() for a in os.environ.get("NEON_LOCAL_AGENTS", "").split(",") if a.strip()]
        plan = {"compute_opts": opts, "agent": agents[0] if agents else None,
                "shared_buffers": os.environ.get("NEON_LOCAL_SHARED_BUFFERS")}
    else:
        opts = {}
        plan = {"seed_db": v["SEED_DB"], "database": "cold_start",
                "server": f"{os.environ.get('DOLT_HOST')}:{os.environ.get('DOLT_PORT')}"}
    print(f"cold start ({args.backend}): W={W} runs={args.runs} reads={args.reads} pause={args.pause}s "
          f"out={out} {json.dumps(plan)}", flush=True)
    if args.dry_run:
        return
    out.mkdir(parents=True, exist_ok=True)
    runs_json = out / "runs.json"
    if runs_json.exists():
        prev = json.loads(runs_json.read_text())
        if len(prev.get("runs", [])) >= args.runs and prev.get("reads") == args.reads:
            print(f"{runs_json} already holds {len(prev['runs'])} runs: nothing to do (new --outdir to rerun)")
            return
    metas = []
    common = {"backend": args.backend, "W": W, "reads": args.reads, "pause_s": args.pause, "topology": v["TOPO_NAME"]}
    if args.backend == "neon":
        os.environ["NEON_LOCAL_COMPUTE_OPTS"] = json.dumps(opts)
        if not plan["agent"]:
            raise SystemExit("NEON_LOCAL_AGENTS is empty")
        os.environ["NEON_LOCAL_AGENTS"] = plan["agent"]   # every compute on the first agent's node
        from dblib import neon_local as nl
        nl.setup_run("cold")
        try:
            for k in range(1, args.runs + 1):
                metas.append(neon_run(k, args, W, out))
                runs_json.write_text(json.dumps({**common, "compute_opts": opts, "agent": plan["agent"],
                                                 "runs": metas}, indent=2))
                time.sleep(args.pause)
        finally:
            nl.teardown_run("cold")
    else:
        import psycopg2
        db = "cold_start"
        mgr = dolt_fresh_db(db, v["SEED_DB"], W)
        admin = None
        try:
            admin = psycopg2.connect(dolt_uri(db))
            admin.autocommit = True
            for k in range(1, args.runs + 1):
                metas.append(dolt_run(k, args, W, out, admin, db))
                runs_json.write_text(json.dumps({**common, "seed_db": v["SEED_DB"], "database": db,
                                                 "runs": metas}, indent=2))
                time.sleep(args.pause)
        finally:
            if admin is not None:
                with admin.cursor() as cur:
                    for k in range(1, args.runs + 1):
                        try:
                            cur.execute("SELECT dolt_branch('-D', %s);", (f"cold_{k}",))
                        except Exception:
                            pass   # never created
                admin.close()
            mgr.cleanup()
    print("wrote", out)


if __name__ == "__main__":
    main()
