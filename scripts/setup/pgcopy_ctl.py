#!/usr/bin/env python3
"""pgcopy operations that talk to the node agents (experiment 13). Runs on a cluster node (the agents listen on LAN
addresses only); scripts/setup/pgcopy.py calls it there over ssh with an environment file written for that node.

  pgcopy_ctl.py --env FILE seed --scale W --shared-buffers SB --out JSON   seed instance on the root agent
  pgcopy_ctl.py --env FILE purge                                           every instance but the seed
  pgcopy_ctl.py --env FILE smoke                                           one fork per host agent, checked, deleted
  pgcopy_ctl.py --env FILE status                                          agents' /stats
  pgcopy_ctl.py --env FILE clock-check --out JSON                          agents' clock offsets (GET /time)
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
os.chdir(REPO)


def load_env(path: str) -> None:
    from dotenv import dotenv_values
    for k in list(os.environ):
        if k.startswith("PGCOPY_") or k.startswith("DBFORK_"):
            del os.environ[k]
    for k, v in dotenv_values(path).items():
        if v is not None:
            os.environ[k] = v


def seed(args) -> None:
    from dblib import pgcopy as pc
    from microbench import task2_pb2 as tp
    from microbench.runner2 import BackendInfo, seed_generated_database

    out = Path(args.out)
    if out.exists():
        raise SystemExit(f"{out} exists: the deployment is seeded (wipe first to reseed)")
    ra, name = pc.root_agent(), pc.seed_instance()
    t0 = time.time()
    r = pc.create_instance(name, None, agent=ra, init=True, start=True, parent_role=True,
                           extra={"shared_buffers": args.shared_buffers, "statement_log": False})
    print(f"seed instance {name} on {ra}: {r['host']}:{r['port']}, initdb {r['copy_ms']:.0f} ms, "
          f"start {r['start_ms']:.0f} ms", flush=True)
    pc.instance_sql(name, "CREATE DATABASE bench OWNER bench", agent=ra)
    pc.instance_sql(name, "CREATE EXTENSION IF NOT EXISTS pg_prewarm", db="bench", agent=ra)
    os.environ.update({"PGCOPY_RUN_ROOT": name, "PGCOPY_RUN_NONCE": "seed"})
    info = BackendInfo(default_branch_name="main", root_branch_name="main")
    gen = tp.GeneratedData(scale_factor=args.scale)
    stats = seed_generated_database(tp.Backend.PGCOPY, info, "bench", gen, args.scale)
    import psycopg2
    host = pc.connect_host(ra, r["host"])
    conn = psycopg2.connect(host=host, port=r["port"], dbname="bench", user="bench", password="bench")
    conn.autocommit = True
    with conn.cursor() as cur:
        t_vac = time.time()
        cur.execute("VACUUM ANALYZE")
        stats["vacuum_sec"] = round(time.time() - t_vac, 1)
        cur.execute("CHECKPOINT")
        cur.execute("SELECT c.relname, c.relkind, pg_relation_size(c.oid), c.reltuples::bigint FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' "
                    "AND c.relkind IN ('r', 'i') ORDER BY 3 DESC")
        relations = {x[0]: {"kind": x[1], "bytes": x[2], "pages": x[2] // 8192, "rows": x[3]}
                     for x in cur.fetchall()}
        cur.execute("SELECT pg_database_size('bench'), (SELECT sum(pg_database_size(oid)) FROM pg_database)")
        stats["database_bytes"], stats["cluster_bytes"] = (int(x) for x in cur.fetchone())
        cur.execute("SELECT count(*) FROM stock")
        stats["stock_rows"] = cur.fetchone()[0]
        cur.execute("SHOW server_version")
        stats["server_version"] = cur.fetchone()[0]
    conn.close()
    # A statement that hangs fails after 10 minutes instead of stopping a run (set after the load; experiment 8)
    pc.instance_sql(name, "ALTER ROLE bench SET statement_timeout = '600s'", agent=ra)
    inst = pc.get_instance(name, ra)
    stats["seconds"] = round(time.time() - t0, 1)
    doc = {"W": args.scale, "seed_instance": name, "root_agent": ra, "host": r["host"], "port": r["port"],
           "seed_shared_buffers": args.shared_buffers, "instance_bytes": inst.get("bytes"), "stats": stats,
           "relations": relations, "t": time.time()}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2))
    print(f"database {stats['database_bytes'] / 2**20:.0f} MiB, {stats['stock_rows']:,} stock rows; largest: "
          + ", ".join(f"{k} {v['bytes'] / 2**20:.0f} MiB" for k, v in list(relations.items())[:5])
          + f"; {stats['seconds']} s", flush=True)


def purge(_args) -> None:
    from dblib import pgcopy as pc
    n = pc.purge()
    print(f"purged {n} instances", flush=True)


def smoke(_args) -> None:
    """A run root from the seed, then one fork of it on every host agent (in parallel), each checked with the seed's
    stock row count and a write, then everything deleted."""
    import psycopg2
    from dblib import pgcopy as pc
    pc.purge()
    root = pc.new_run("smoke")
    pc.create_run_root(root)
    ra = pc.root_agent()
    want = int(pc.instance_sql(root, "SELECT count(*) FROM stock", db="bench", agent=ra).strip())
    agents = pc.agents()

    def one(i_agent):
        i, agent = i_agent
        name = f"smoke_{i}"
        r = pc.create_instance(name, root, parent_agent=ra, agent=agent)
        c = psycopg2.connect(host=pc.connect_host(agent, r["host"]), port=r["port"], dbname="bench", user="bench",
                             password="bench", connect_timeout=30)
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("SELECT count(*) FROM stock")
            got = cur.fetchone()[0]
            cur.execute("UPDATE stock SET s_quantity = s_quantity + 1 WHERE s_w_id = 1 AND s_i_id = 1")
        c.close()
        pc.delete_instance(name, agent)
        return {"agent": agent, "rows": got, "copy_ms": round(r["copy_ms"]), "start_ms": round(r["start_ms"]),
                "mb": round(r["bytes"] / 2**20), "ok": got == want}

    with ThreadPoolExecutor(len(agents)) as ex:
        res = list(ex.map(one, enumerate(agents)))
    for x in res:
        print(json.dumps(x), flush=True)
    left = {}
    for a in pc.all_agents():
        st, body = pc.http("GET", f"{a}/instances")
        left[a] = [x["name"] for x in body] if st == 200 else f"HTTP {st}"
    pc.purge()
    bad = [x for x in res if not x["ok"]]
    print(f"smoke: {len(res) - len(bad)}/{len(res)} forks ok (stock rows {want}); instances before the final purge: "
          + json.dumps({a.split('//')[1]: v for a, v in left.items() if v}), flush=True)
    if bad:
        raise SystemExit(f"smoke failed: {bad}")


def status(_args) -> None:
    from dblib import pgcopy as pc
    for a in pc.all_agents():
        st, body = pc.http("GET", f"{a}/stats", timeout=30)
        if st != 200:
            print(f"{a}: HTTP {st} {body}")
            continue
        print(f"{a}: {body['instances']} instances ({body['running']} running), data "
              f"{body['data_bytes'] / 2**30:.2f} GiB, wal {body['wal_bytes'] / 2**30:.2f} GiB, mem avail "
              f"{body['mem_available_mb'] / 1024:.1f} GiB, disk free {body['disk_free_gb']:.0f} GB", flush=True)


def clock_check(args) -> None:
    """Offset of every agent's clock from this node's, by the midpoint of a GET /time (median of 5)."""
    from dblib import pgcopy as pc
    out = {}
    for a in pc.all_agents():
        offs = []
        for _ in range(5):
            t0 = time.time()
            st, body = pc.http("GET", f"{a}/time", timeout=10)
            t1 = time.time()
            if st == 200:
                offs.append(body["t"] - (t0 + t1) / 2)
        offs.sort()
        out[a] = {"offset_ms": round(offs[len(offs) // 2] * 1e3, 3) if offs else None}
        print(f"{a}: {out[a]['offset_ms']} ms", flush=True)
    if args.out:
        Path(args.out).write_text(json.dumps(out, indent=2))


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("seed")
    p.add_argument("--scale", type=int, required=True)
    p.add_argument("--shared-buffers", default="1GB")
    p.add_argument("--out", required=True)
    for c in ("purge", "smoke", "status"):
        sub.add_parser(c)
    p = sub.add_parser("clock-check")
    p.add_argument("--out")
    a = ap.parse_args(argv)
    load_env(a.env)
    {"seed": seed, "purge": purge, "smoke": smoke, "status": status, "clock-check": clock_check}[a.cmd](a)


if __name__ == "__main__":
    main()
