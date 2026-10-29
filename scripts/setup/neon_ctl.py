#!/usr/bin/env python3
"""Seed and operate a self-hosted Neon deployment through dblib/neon_local.py (experiment 8's and 11's neon_ctl.py,
parameterized by an environment file instead of an experiment's paths). Runs on the node that should talk to the
cluster: setup/neon.py runs `seed` and `shard-split` on the seeding node (near the storage) and the rest on the
client node, with the environment file it wrote for that node's view (topo.py write_env).

    uv run python scripts/setup/neon_ctl.py --env FILE <command>

    seed --scale W --shared-buffers SB --out JSON [--pageserver-node 1]
                      create the tenant (placement Attached:0 on pageserver node 1, scheduling Pause) and the
                      root timeline (Postgres 17); load CH-benCHmark W into database "bench" (owner "bench")
                      through a seeding compute (shared_buffers SB, LFC and statement log off); extensions neon
                      and pg_prewarm; VACUUM ANALYZE; relation sizes; bench's statement_timeout 600 s; wait for
                      the pageserver to ingest it; checkpoint and forced image compaction; write WAL past the
                      images. Writes JSON (tenant_id, seed_timeline_id, W, relations, ...).
    compact-seed      checkpoint + image compaction of the seed timeline (the last step of seed)
    shard-split --shards K --stripe-pages P --out JSON
                      split the tenant, shard k to pageserver node k+1, pause scheduling again; one-way
    locate            the tenant's shards and their pageservers
    purge             stop every compute, delete every timeline except the seed
    status            timelines and each agent's /stats
    metrics FILE [--targets label=url,...]
                      /metrics of the services into one file (default: NEON_LOCAL_* URLs)
    clock-check --agents label=url,... [--out JSON]
                      clock offset of every agent against this machine (GET /time, best of 10)
    smoke             branch, compute start, read, write and delete on one branch
"""
import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
os.chdir(REPO)

nl = None   # dblib.neon_local, imported after the environment file is loaded


def load_env(path: str) -> None:
    """KEY=VALUE lines (topo.env_text: values with JSON or spaces in single quotes) into os.environ."""
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"":
            v = v[1:-1]
        os.environ[k.strip()] = v


def admin_sql(timeline_id: str, sql: str, db: str = "postgres") -> str:
    """SQL as cloud_admin on the timeline's running compute, in database ``db``."""
    st, resp = nl.http("POST", f"{nl.agent_for(timeline_id)}/computes/{timeline_id}/sql", {"sql": sql, "db": db},
                       timeout=7200)
    if st != 200:
        raise RuntimeError(f"admin SQL on {timeline_id} ({db}): HTTP {st} {resp}")
    return resp["stdout"]


def storcon(method, path, body=None, timeout=60):
    st, resp = nl.http(method, f"{os.environ['NEON_LOCAL_STORCON_URL']}{path}", body, timeout=timeout)
    if st not in (200, 201, 202):
        raise RuntimeError(f"{method} {path}: HTTP {st} {resp}")
    return resp


def locate(tenant=None) -> dict:
    return storcon("GET", f"/debug/v1/tenant/{tenant or nl.tenant()}/locate")


def describe(tenant=None) -> dict:
    return storcon("GET", f"/control/v1/tenant/{tenant or nl.tenant()}")


def pause_tenant(tenant) -> None:
    storcon("PUT", f"/control/v1/tenant/{tenant}/policy", {"scheduling": "Pause"})


def lsn_int(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def last_record_lsn(timeline_id: str) -> int:
    st, body = nl.http("GET", f"{os.environ['NEON_LOCAL_PAGESERVER_URL']}/v1/tenant/{nl.tenant()}"
                              f"/timeline/{timeline_id}")
    if st != 200:
        raise RuntimeError(f"timeline detail: HTTP {st} {body}")
    return lsn_int(body["last_record_lsn"])


def compact_images(timeline_id: str) -> dict:
    """Checkpoint and compact with forced image-layer creation (pageserver testing API), so reads find page
    images instead of replaying every WAL record since the bulk load (a bulk load alone leaves only delta
    layers). Returns the layer counts after."""
    base = f"{os.environ['NEON_LOCAL_PAGESERVER_URL']}/v1/tenant/{nl.tenant()}/timeline/{timeline_id}"
    for path in ("/checkpoint", "/compact?force_image_layer_creation=true&force_l0_compaction=true"
                                "&force_repartition=true&wait_until_uploaded=true"):
        st, body = nl.http("PUT", base + path, timeout=3600)
        if st != 200:
            raise RuntimeError(f"PUT {path}: HTTP {st} {body}")
    st, body = nl.http("GET", base + "/layer")
    kinds = {}
    for layer in body["historic_layers"]:
        kinds[layer["kind"]] = kinds.get(layer["kind"], 0) + 1
    return kinds


def bump_past_images(timeline_id: str) -> str:
    """Image layers are written at last_record_lsn + 1, and a branch made at last_record_lsn reads below them
    (falling back to replaying deltas). Write a little WAL (compute start, CHECKPOINT, shutdown) so later
    branches start above the images. Returns the new last_record_lsn."""
    ps = os.environ["NEON_LOCAL_PAGESERVER_URL"]
    st, body = nl.http("GET", f"{ps}/v1/tenant/{nl.tenant()}/timeline/{timeline_id}/layer")
    image_lsn = max(lsn_int(l["lsn_start"]) for l in body["historic_layers"] if l["kind"] == "Image")
    nl.ensure_compute(timeline_id)
    nl.compute_sql(timeline_id, "CHECKPOINT")
    nl.stop_compute(timeline_id)
    deadline = time.time() + 300
    while last_record_lsn(timeline_id) <= image_lsn:
        if time.time() > deadline:
            raise RuntimeError("seed timeline did not advance past its image layers")
        time.sleep(1)
    st, body = nl.http("GET", f"{ps}/v1/tenant/{nl.tenant()}/timeline/{timeline_id}")
    return body["last_record_lsn"]


def compact_seed(_args) -> None:
    t0 = time.time()
    kinds = compact_images(nl.seed_timeline())
    lsn = bump_past_images(nl.seed_timeline())
    print(f"seed timeline layers after image compaction: {kinds}; last_record_lsn now {lsn} "
          f"({time.time() - t0:.0f}s)")


def seed(args) -> None:
    from microbench import task2_pb2 as tp
    from microbench.runner2 import BackendInfo, seed_generated_database

    out_path = Path(args.out)
    if out_path.exists():
        raise SystemExit(f"{out_path} exists: the deployment is seeded (wipe first to reseed)")
    storcon_url = os.environ["NEON_LOCAL_STORCON_URL"]
    tenant = uuid.uuid4().hex
    timeline = uuid.uuid4().hex
    st, body = nl.http("POST", f"{storcon_url}/v1/tenant",
                       {"new_tenant_id": tenant, "placement_policy": {"Attached": 0}})
    if st not in (200, 201):
        raise RuntimeError(f"tenant create: HTTP {st} {body}")
    # The tenant must sit on pageserver node --pageserver-node (the others start paused, so it does); a Pause
    # policy keeps the storage controller from moving it to the emptiest pageserver later
    where = {s["node_id"] for s in body.get("shards", [])}
    if where and where != {args.pageserver_node}:
        raise RuntimeError(f"tenant placed on {where}, wanted node {args.pageserver_node}")
    pause_tenant(tenant)
    st, body = nl.http("POST", f"{storcon_url}/v1/tenant/{tenant}/timeline",
                       {"new_timeline_id": timeline, "pg_version": 17}, timeout=300)
    if st not in (200, 201):
        raise RuntimeError(f"root timeline create: HTTP {st} {body}")
    # The seeding compute: statement log off (the bulk INSERTs would write gigabytes of log), no LFC
    os.environ.update({"NEON_LOCAL_TENANT_ID": tenant, "NEON_LOCAL_SEED_TIMELINE_ID": timeline,
                       "NEON_LOCAL_RUN_ROOT": timeline, "NEON_LOCAL_RUN_NONCE": "seed",
                       "NEON_LOCAL_COMPUTE_OPTS": json.dumps({"statement_log": False, "lfc_mb": 0})})
    print(f"tenant {tenant}, seed timeline {timeline}", flush=True)
    t0 = time.time()
    r = nl.ensure_compute(timeline, shared_buffers=args.shared_buffers)
    print(f"seed compute {r['host']}:{r['port']} up in {r['start_ms']:.0f} ms", flush=True)
    pw = os.environ["NEON_LOCAL_PG_PASSWORD"]
    nl.compute_sql(timeline, f"CREATE ROLE bench LOGIN PASSWORD '{pw}'")
    nl.compute_sql(timeline, "CREATE DATABASE bench OWNER bench")
    # neon: perf counters and LFC stats the agent samples (in "postgres"); pg_prewarm: the untimed cache fill
    # before each measured run (in "bench"). Branches inherit both.
    admin_sql(timeline, "CREATE EXTENSION IF NOT EXISTS neon")
    admin_sql(timeline, "CREATE EXTENSION IF NOT EXISTS pg_prewarm", db="bench")
    info = BackendInfo(default_branch_name="main", root_branch_name="main")
    gen = tp.GeneratedData(scale_factor=args.scale)
    stats = seed_generated_database(tp.Backend.NEON_LOCAL, info, "bench", gen, args.scale)
    import psycopg2
    conn = psycopg2.connect(host=r["host"], port=r["port"], dbname="bench", user="bench", password=pw)
    conn.autocommit = True
    with conn.cursor() as cur:
        t_vac = time.time()
        cur.execute("VACUUM ANALYZE")
        stats["vacuum_sec"] = round(time.time() - t_vac, 1)
        cur.execute("SELECT c.relname, c.relkind, pg_relation_size(c.oid), c.reltuples::bigint FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' "
                    "AND c.relkind IN ('r', 'i') ORDER BY 3 DESC")
        relations = {x[0]: {"kind": x[1], "bytes": x[2], "pages": x[2] // 8192, "rows": x[3]}
                     for x in cur.fetchall()}
        cur.execute("SELECT pg_database_size('bench')")
        stats["database_bytes"] = cur.fetchone()[0]
        cur.execute("SELECT pg_current_wal_flush_lsn()::text")
        flush = lsn_int(cur.fetchone()[0])
    conn.close()
    # A statement that hangs fails after 10 minutes instead of stopping a run (set after the load, which must
    # not be cut short; experiment 8)
    admin_sql(timeline, "ALTER ROLE bench SET statement_timeout = '600s'")
    print(f"database {stats['database_bytes'] / 2**30:.2f} GiB; largest: "
          + ", ".join(f"{k} {v['bytes'] / 2**20:.0f} MiB" for k, v in list(relations.items())[:5]), flush=True)
    while last_record_lsn(timeline) < flush:
        time.sleep(1)
    nl.stop_compute(timeline)
    stats["layers"] = compact_images(timeline)
    stats["last_record_lsn"] = bump_past_images(timeline)
    out = {"tenant_id": tenant, "seed_timeline_id": timeline, "W": args.scale,
           "pageserver_node": args.pageserver_node, "seed_shared_buffers": args.shared_buffers,
           "relations": relations, "seed": stats, "seconds": round(time.time() - t0, 1)}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


def cmd_locate(_args) -> None:
    print(json.dumps(locate(), indent=2))
    print(json.dumps([{k: s.get(k) for k in ("tenant_shard_id", "node_attached", "node_secondary", "is_reconciling",
                                              "is_pending_compute_notification", "scheduling_policy")}
                      for s in describe()["shards"]], indent=2))


def shard_split(args) -> None:
    """Experiment 5/8: split, shard k to node k+1, wait until every shard is attached there, pause again."""
    t0 = time.time()
    tenant = nl.tenant()
    out = {"tenant_id": tenant, "shards": args.shards, "stripe_pages": args.stripe_pages, "before": locate()}
    for i in range(2, args.shards + 1):        # the other pageservers take part from now on
        storcon("PUT", f"/control/v1/node/{i}/config", {"node_id": i, "scheduling": "Active"})
    storcon("PUT", f"/control/v1/tenant/{tenant}/policy", {"scheduling": "Active"})
    out["split"] = storcon("PUT", f"/control/v1/tenant/{tenant}/shard_split",
                           {"new_shard_count": args.shards, "new_stripe_size": args.stripe_pages}, timeout=1800)
    print("split:", out["split"], f"({time.time() - t0:.0f}s)", flush=True)
    for sh in sorted(out["split"]["new_shards"]):
        want = int(sh.split("-")[1][:2], 16) + 1          # shard k on node k + 1
        cur = {s["shard_id"]: s["node_id"] for s in locate()["shards"]}.get(sh)
        if cur != want:
            t1 = time.time()
            storcon("PUT", f"/control/v1/tenant/{sh}/migrate",
                    {"node_id": want, "migration_config": {"override_scheduler": True}}, timeout=1800)
            print(f"migrate {sh}: node {cur} -> {want} requested ({time.time() - t1:.1f}s)", flush=True)
    deadline = time.time() + 1800
    while True:
        d = describe()
        if all(not s.get("is_reconciling")
               and s.get("node_attached") == int(s["tenant_shard_id"].split("-")[1][:2], 16) + 1
               for s in d["shards"]):
            break
        if time.time() > deadline:
            raise RuntimeError(f"shards not placed: {d['shards']}")
        time.sleep(2)
    pause_tenant(tenant)
    out["after"] = locate()
    out["describe"] = describe()
    out["seconds"] = round(time.time() - t0, 1)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2))
    print(json.dumps(out["after"], indent=2))
    print(f"done in {out['seconds']}s -> {args.out}")


def purge(_args) -> None:
    nl.purge()
    status(_args)


def status(_args) -> None:
    tls = nl.list_timelines()
    print(f"tenant {nl.tenant()}: {len(tls)} timelines")
    for a in nl._list("NEON_LOCAL_AGENTS"):
        st, body = nl.http("GET", f"{a}/stats", timeout=30)
        print(f"  {a}: {body}")


def pairs(spec: str) -> list:
    return [tuple(x.split("=", 1)) for x in spec.split(",") if x.strip()]


def metrics(args) -> None:
    if args.targets:
        parts = pairs(args.targets)
    else:
        parts = [("pageserver", os.environ["NEON_LOCAL_PAGESERVER_URL"])]
        parts += [(f"safekeeper{i + 1}", u) for i, u in enumerate(nl._list("NEON_LOCAL_SAFEKEEPER_URLS"))]
        parts += [("storcon", os.environ["NEON_LOCAL_STORCON_URL"])]
    with open(args.file, "w") as f:
        for name, url in parts:
            try:
                st, text = nl.http("GET", f"{url.rstrip('/')}/metrics", timeout=30)
                text = text if isinstance(text, str) else json.dumps(text)
            except Exception as e:  # noqa: BLE001
                text = f"# error: {e}\n"
            f.write(f"### {name} {time.time():.3f}\n{text}")


def clock_check(args) -> None:
    """Offset of every agent's clock against this machine's (agent - local), from the GET /time sample with the
    smallest round trip out of 10 on one kept-alive connection; the error bound is half that round trip."""
    import http.client
    out = {"host": os.uname().nodename, "t": time.time(), "agents": {}}
    for label, url in pairs(args.agents):
        unix = nl.split_unix_url(url)
        if unix:
            conn = nl.UnixHTTPConnection(unix[0], timeout=10)
        else:
            u = url.split("://", 1)[1].rstrip("/")
            host, port = u.rsplit(":", 1)
            conn = http.client.HTTPConnection(host, int(port), timeout=10)
        best = None
        for i in range(11):
            t1 = time.time()
            conn.request("GET", "/time")
            ta = json.loads(conn.getresponse().read())["t"]
            t2 = time.time()
            if i and (best is None or t2 - t1 < best[0]):
                best = (t2 - t1, ta - (t1 + t2) / 2)
        conn.close()
        out["agents"][label] = {"offset_s": best[1], "rtt_s": best[0]}
        print(f"{label}: offset {best[1] * 1e3:+.2f} ms (rtt {best[0] * 1e3:.2f} ms)")
    if args.out:
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        old = json.loads(path.read_text()) if path.exists() else []
        path.write_text(json.dumps((old if isinstance(old, list) else [old]) + [out], indent=2))


def smoke(_args) -> None:
    from dblib import result_collector as rc
    nl.setup_run("smoke")
    s = nl.NeonLocalToolSuite(rc.ResultCollector(), "main")
    print("root stock rows:", s.exec(["SELECT count(*) FROM stock"], refs=["main"])[0].rows)
    t0 = time.time()
    print("branch:", s.branch("b1", from_ref="main"), f"({(time.time() - t0) * 1e3:.1f} ms)")
    t0 = time.time()
    res = s.exec(["UPDATE stock SET s_quantity = s_quantity + 1 WHERE s_w_id = 1 AND s_i_id = 1",
                  "SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = 1"], refs=["b1"])
    print(f"b1 first exec (compute start) {time.time() - t0:.2f}s:", res[0].status, res[0].rows,
          "compute start ms:", s.last_compute_start_ms)
    t0 = time.time()
    for i in range(1, 201):
        s.exec([f"SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = {i}"], refs=["b1"])
    print(f"200 point reads on b1: {(time.time() - t0) / 200 * 1e3:.3f} ms each (exec overhead included)")
    print("main unchanged:", s.exec(["SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = 1"],
                                    refs=["main"])[0].rows)
    print("delete:", s.delete("b1"))
    s.close_connection()
    nl.teardown_run("smoke")
    status(_args)


def main(argv=None):
    global nl
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, help="environment file (KEY=VALUE) written by setup/neon.py")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("seed")
    p.add_argument("--scale", type=int, required=True)
    p.add_argument("--shared-buffers", required=True)
    p.add_argument("--pageserver-node", type=int, default=1)
    p.add_argument("--out", required=True)
    sub.add_parser("compact-seed")
    p = sub.add_parser("shard-split")
    p.add_argument("--shards", type=int, default=2)
    p.add_argument("--stripe-pages", type=int, default=256)
    p.add_argument("--out", required=True)
    sub.add_parser("locate")
    sub.add_parser("purge")
    sub.add_parser("status")
    p = sub.add_parser("metrics")
    p.add_argument("file")
    p.add_argument("--targets", default="", help="label=url,... (service base URLs)")
    p = sub.add_parser("clock-check")
    p.add_argument("--agents", required=True, help="label=url,...")
    p.add_argument("--out", default="")
    sub.add_parser("smoke")
    a = ap.parse_args(argv)
    load_env(a.env)
    from dblib import neon_local
    nl = neon_local
    {"seed": seed, "compact-seed": compact_seed, "shard-split": shard_split, "locate": cmd_locate, "purge": purge,
     "status": status, "metrics": metrics, "clock-check": clock_check, "smoke": smoke}[a.cmd](a)


if __name__ == "__main__":
    main()
