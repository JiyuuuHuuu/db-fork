# Experiment 5: Neon across two zones with a compute cache: low load, scaling, branch creation, cold start

## Context

Experiment 4 (`experiment/4-neon-cross-zone-low-load/`) measured Neon's low-load latency with the storage layer in
Wisconsin and the compute plus client in Utah (38.9 ms round trip), W=60 and a 448 MB LFC holding 15% of the
stock heap. The user wants four follow-ups on the same placement, as sub-experiments of one new experiment, with a
smaller dataset and a compute cache that becomes the default for later experiments:

1. **Low load again** with the new dataset/cache: latency bars, breakdown, plus a figure that separates cache-hit
   from cache-miss latency.
2. **Scaling with data ops** (experiment 3's throughput-vs-latency lines, max-throughput ring, every point labelled
   `N=`), in three variants: (a) no LFC (almost every page from the pageserver), (b) the default LFC, (c) the default
   LFC with **two pageservers**, branches partitioned over them.
3. **Branch creation** with N busy branches, latency graph only, default LFC.
4. **Cold start**: point-read latency time series after a branch is created, while its LFC fills.

Decisions taken with the user (this session):
- Dataset **W=4** (stock heap 133 MiB + key 8.6 MiB), compute **shared_buffers 8 MB**, **LFC 36 MB in 8 kB chunks
  (25% of stock)**: prewarmed with the whole stock key plus the first 20% of the heap, so about 25% of point reads
  hit the compute (20% LFC + ~6% shared_buffers). The LFC must be small in absolute terms because at N branches
  there are N computes on seven 15 GB Utah nodes, and seeing the two-pageserver knee needs N≈1024.
- Sweep N = 1, 4, 16, 64, 128, 256, 512, 1024; the Python runner on the 8-core r320 client saturates near 8-12k
  ops/s, so experiment 3's minimal asyncio client re-measures N ≥ 256 (drawn as × points on the same chart);
  runner points with ≥ 10 of the client's 16 threads busy are hollow.
- Two pageservers by **shard split** (Neon's data partitioning: the one tenant is split into 2 shards with a
  2 MiB stripe before variant (c), shard 1 moved to the second pageserver), chosen over two tenants with
  round-robin branches: same effect on the knee, no multi-tenant logic in the benchmark, and it is Neon's own
  mechanism.
- My calls (user did not object): second pageserver on **a5** (unused /dev/sdc SSD; a7's SSD is the root disk);
  "very small LFC" = **LFC off**; exp 3's one-zone bars and exp 4's two-zone bars stay as references in the
  low-load figure.

Physics to state in the report: with one request in flight per branch, a branch issues about one GetPage per WAN
round trip whether or not it has a cache (hit ≈ 1 ms, miss ≈ 41 ms), so the cache raises the throughput at the
knee by 1/(1−hit) but does not move the knee's N. The one-pageserver knee is expected near N≈500 where the
pageserver's 1 GbE WAN uplink carries ~13k GetPage/s (a2 sustained ~890 Mbit/s to Utah); two pageservers double it.
Both are expectations to be checked by probes (bandwidth, memory per compute) before fixing the top N.

## Facts that shape the design (verified)

- Neon places per **tenant shard**; every timeline of a tenant lives on the tenant's shards, secondaries cannot
  serve reads, and existing tenants are never rebalanced onto a new node. Shard split: `PUT
  /control/v1/tenant/{id}/shard_split {"new_shard_count": 2, "new_stripe_size": 256}` (stripe in pages; the
  default 2048 = 16 MiB would give the 133 MiB stock heap only ~8 stripes); children start on the parent's node,
  so shard 1 (`{tenant}-0102`) is moved with `PUT /control/v1/tenant/{shard}/migrate {"node_id": 2,
  "migration_config": {"override_scheduler": true}}` and the tenant is pinned with `PUT
  /control/v1/tenant/{id}/policy {"scheduling": "Pause"}`; `GET /debug/v1/tenant/{id}/locate` lists shards with
  node and pg address. A page maps to a shard by hash(relation) + block/stripe mod shard count
  (`pgxn/neon/libpagestore.c:390-406`).
- The compute learns the shards from its spec: `pageserver_connection_info {shard_count, stripe_size, shards:
  {"0002": {pageservers: [{id, libpq_url, grpc_url}]}, "0102": …}}` plus the legacy `pageserver_connstring`
  (comma list, one URL per shard) and `shard_stripe_size` (`libs/compute_api/src/spec.rs:121-128, 237-252`);
  `neon.pageserver_connstring` is a comma list and `neon.stripe_size` an int, both reloadable.
- `dblib/neon_local.py` `ensure_compute` :138-146 sends `{timeline_id, tenant_id, shared_buffers?}` and nothing
  about pageservers or the LFC; `delete_timeline` :113-130 polls `NEON_LOCAL_PAGESERVER_URL` with the bare tenant
  id (on a sharded tenant that path is a 404 at once, which just skips the wait; harmless).
- The exp 4 agent (`neon_node_agent.py`) already accepts per-compute `shared_buffers` and `lfc_mb` in the POST body
  (:253-254); the pageserver is agent-wide (`--pageserver`, `render_config` :106-136 writes one shard `"0000"`).
- The CH generator is deterministic (`macrobench/datagen/ch.py:95`, seed 42).
- `DBFORK_PREWARM_SQL` runs only for measured sync workers (`runner2.py:2187-2201`), not for background clients.
  Warm-up rows are not recorded. The key scan runs per process on the first branch that needs it; with
  `--processes 1` and setup updates it runs on `main`.
- LFC GUCs: `neon.max_file_cache_size` is postmaster-only, chunk size 1..128 pages, `neon_get_lfc_stats()` keys
  `file_cache_hits/misses/used/used_pages/writes/limit/evicted_pages` (sampled every second by the agent).
- Cluster now: a0-a3 storage layer active, a4 seed agent, b1 agent (tmpfs 6G, no computes), a6 agent (one-zone
  leftovers, unused), a5 has a db-fork checkout and a blank /dev/sdc, a7 spare, b2-b7 untouched (15 GB, 16
  threads). Pageserver a2 has 417 GB free; one tenant (W=60) with only its seed timeline.
- Exp 3's `ceiling_check.py` reads only warehouse 1 (`s_w_id = 1`) and inserts with `s_w_id = 2, 3, …`; at W=4 it
  must pick the warehouse at random and insert with `s_w_id > W`, or its working set is a quarter of the table.

## Layout: `experiment/5-neon-cross-zone-scaling/`

```
README.md  plan.md  REPRODUCE.md  report.md  figures/  logs/  code/shared_code.patch
config/compute_config_template.json   from exp 4; shared_buffers 8MB (agent still rewrites it)
config/compute_hook_stub.py            unchanged
scripts/cluster.env                    exp 4's + a5, b2-b7; PAGESERVER_NODES="a2 a5"; W=4 SB=8MB LFC_MB=36
                                       LFC_CHUNK=1 PREWARM_HEAP_BLOCKS=<20% of stock pages from seed.json> TMPFS_SIZE=12G
scripts/setup_neon_cluster.sh          from exp 4: several pageserver nodes (ids 1,2), binaries to b2-b7 over the
                                       Utah LAN from b1 (tar over python http.server), tmpfs 12G, node 2 registered
                                       with scheduling "Pause" until variant (c)
scripts/neon_node_agent.py             from exp 4 + per-compute `pageservers` (list, one per shard), `stripe_size`
                                       and `statement_log` in the POST body (rendered into pageserver_connection_info,
                                       pageserver_connstring and shard_stripe_size),
                                       NIC tx/rx bytes (control NIC) and mem/tmpfs in every sample, and the
                                       per-compute psql sampling capped per tick (--sample-max-computes 8,
                                       round-robin): at 146 computes per 8-core node one psql per compute per
                                       second is not affordable (services are still sampled every tick)
scripts/neon_ctl.py                    from exp 4 + `seed --pageserver-node 1` (pin + Pause), `shard-split`
                                       (split, migrate shard 1 to node 2, Pause, locate check), `env-init
                                       --shards` (writes NEON_LOCAL_COMPUTE_OPTS from locate), `node-scheduling`,
                                       metrics of all pageservers
scripts/probe_wan.sh                   iperf3 a2/a5 -> b1..b7, many streams, both directions -> logs/
scripts/probe_memory.py                100 computes on b2 (+500 reads each) -> MB per compute (RSS, tmpfs) -> logs/
scripts/run_low_load.sh                exp 4's (sub-experiment A) -> data/low_load/
scripts/run_scaling.sh                 sub-experiment B: --variant nolfc|lfc25|lfc25_2ps --ns ... -> data/scaling/<variant>/
scripts/ceiling_check.py               exp 3's, adapted (random warehouse, inserts at s_w_id > W, prewarm, 16 procs) -> data/scaling/<variant>/ceiling_check.csv
scripts/run_branch_create.sh           sub-experiment C -> data/branch_create/
scripts/cold_start.py                  sub-experiment D -> data/cold_start/run<k>/
scripts/pull_data.sh, sync_client.sh   from exp 4 (archive -> data/archive/<node>/)
scripts/breakdown.py                   from exp 4, parametrised (data dir, pageserver nodes)
scripts/load_runs.py                   summary loader (exp 3's load() for per-op dirs, variants, client_limited at >= 10 cores)
scripts/plot_low_load.py               low_load.png, low_load_breakdown.png, low_load_hit_miss.png
scripts/plot_scaling.py                throughput_latency.png (per variant), data_ops.png, scaling_bottleneck.png
scripts/plot_branch_create.py          branch_create.png
scripts/plot_cold_start.py             cold_start.png
scripts/summarize.py                   from exp 4
data/seed/seed.json, shard_split.json  the W=4 seed; the split's response and locate output
data/low_load/   data/scaling/{nolfc,lfc25,lfc25_2ps}/{data_ops/<OP>,ceiling_check.csv,metrics,logs}
data/branch_create/   data/cold_start/run<k>/   data/archive/<node>/   data/superseded/
```

Node roles: a0 control + sk1; a1, a3 sk2/3; a2 pageserver 1 (node 1); **a5 pageserver 2 (node 2)**; a4 seed compute;
a6, a7 unused; b0 client; **b1-b7 compute nodes** (computes on 12G tmpfs). All compute-to-storage traffic on public
addresses as in exp 4.

## Shared code changes (recorded in `code/shared_code.patch`, listed in the README)

1. `dblib/neon_local.py` `ensure_compute` (:138-146): merge `NEON_LOCAL_COMPUTE_OPTS` (a JSON object, e.g.
   `{"lfc_mb": 36, "statement_log": false, "pageservers": ["128.105.145.168:64000", "128.105.145.64:64000"],
   "stripe_size": 256}`) into the POST body; keys absent when unset, so the agent's flags still apply and nothing
   else in the backend changes (one tenant, one storcon, as today). The agent owns the meaning of the keys. Scaling
   runs turn statement logging off (1024 computes × 3,000 statements per run is ~400 MB of log per run); low-load
   and cold-start keep it on for the breakdown.
2. `microbench/runner2.py`: also run `DBFORK_PREWARM_SQL` for background clients (after the untimed first op,
   before `self._bg.started()`, ~line 3070), so the busy branches of sub-experiment C have the same cache state as
   measured ones.
3. Tests: `tests/test_neon_local.py` (new, fake `neon_local.http`): the compute body carries the opts when the env
   var is set and nothing extra when it is not; a `test_micro_runner2.py` case for change 2 in the house style of
   `test_prewarm_sql_runs_unrecorded_before_the_warm_up`.
4. Exp 3's `ceiling_check.py` is copied into exp 5 (not edited in place).

Agent (experiment script): `render_config` takes `pageservers` (list) and `stripe_size`; with one entry it renders
today's single shard `"0000"`; with K entries it renders shards `"{i:02x}{K:02x}"`, `shard_count=K`, `stripe_size`,
the legacy `pageserver_connstring` as the comma list and `shard_stripe_size`. Checked on a smoke compute by
`SHOW neon.pageserver_connstring` and by GetPage counters rising on both pageservers.

## Procedure (coordinator; `X=experiment/5-neon-cross-zone-scaling`)

### 0. Code and smoke tests (~1 h)
- Create the directory, copy and adapt the exp 4 scripts, implement the shared changes, `uv run pytest tests/ -q`.
- Write `plan.md` (this plan) and a README skeleton.

### 1. Cluster (~1.5 h, mostly binary copies)
- `setup_neon_cluster.sh`: agents on b2-b7 (tmpfs 12G, shmmni), pageserver 2 on a5 (`/mnt/ssd/ps`, id 2, AZ `lan`,
  same toml), register node 2 in storcon with scheduling `Pause`; b1's tmpfs remounted at 12G. `--status` checks
  every unit, both pageservers Active/Pause, every agent `/time`.
- `probe_wan.sh` (10 min): iperf3 from a2 and a5 to each of b1-b7 at once with 16 streams per pair, and the reverse;
  ping. Pass: a2 ≥ ~800 Mbit/s aggregate; if a5 is far below a2, switch pageserver 2 to a6/a7 (whichever probes
  best) before going on. Records the expected GetPage/s ceiling per pageserver.
- Seed on a4 over the LAN: `neon_ctl.py seed --scale 4 --pageserver-node 1` (~5 min incl. image compaction): creates
  the tenant, pins it to node 1 (`migrate` if needed, scheduling `Pause`; the old W=60 tenant is paused too),
  seeds, compacts to image layers, writes `data/seed/seed.json`. Pass: stock rows 400,000, Image layers > 0,
  last_record_lsn above the images, `locate` shows one shard on node 1. Node 2 stays `Pause`d until variant (c).
- `env-init --from-seed-json` on b0 (`NEON_LOCAL_COMPUTE_OPTS` with the single pageserver, `DBFORK_REMOTE_CPU` for
  both pageservers, the safekeepers and the seven compute nodes), clock check (< 10 ms), `neon_ctl.py smoke`.
- `probe_memory.py` (10 min): 100 computes on b2 at the default cache, 500 random reads each, then read the
  agent's `/stats` and tmpfs use -> MB per compute. Decide the top N per variant:
  `N_max = 7 × floor((mem_available − 2 GB) / per_compute)`, rounded down to the sweep list. Expected: 1024 for all
  variants if a compute costs ≤ 90 MB.

### 2. Sub-experiment A: low load (~1 h)
`run_low_load.sh` as exp 4 (READ 40k, RANGE_READ 10k, INSERT 6k, UPDATE 6k, +10% warm-up, prewarm before each; 100
creates, every 5th connected, one MIXED background client), output `data/low_load/`. Smoke first (`--smoke --ops READ`).
Pass: progress.log DONE ×5, no failures, LFC hit ratio ≈ 0.25 ± 0.05 in the READ window.

### 3. Sub-experiment B: scaling (~12 h unattended, run as three detached variant drivers in sequence)
`run_scaling.sh --variant V` for V in `nolfc` (LFC_MB=0), `lfc25` (36 MB), `lfc25_2ps` (36 MB, tenant split over two
pageservers), one variant after the other. Before `lfc25_2ps`: `neon_ctl.py purge`; `neon_ctl.py shard-split --shards 2 --stripe-pages 256`
(sets node 2 Active, splits, migrates shard `{tenant}-0102` to node 2 with `override_scheduler`, pauses the tenant,
saves the split response and `locate` to `data/seed/shard_split.json`); `env-init --shards` rewrites
`NEON_LOCAL_COMPUTE_OPTS` with both pageservers and `stripe_size` 256; smoke (`SHOW neon.pageserver_connstring`
lists both, GetPage counters rise on a2 and a5). The split is one-way, so this variant runs last; (a) and (b) never
touch a5. Branch creates in (c) touch both shards (two pageserver calls), noted in the report.
- per N in the sweep and op in READ, RANGE_READ, INSERT, UPDATE: `run_throughput_bench.sh neon_local
  --sweep-proportional --threads-per-branch 1 --branch-list N --processes min(N,16) --operations OP --num-ops k
  --mode sync --warmup-fraction 0.1 --table-name stock --updates-per-branch 100 --seed-db bench --generated-scale 4
  --setup-parallelism 32 --output-dir data/scaling/V/data_ops/N/OP`, with `DBFORK_PREWARM_SQL` set for the LFC
  variants and `NEON_LOCAL_COMPUTE_OPTS` per variant (`lfc_mb`, `statement_log: false`, pageservers).
- ops per client k: `max(min_op, total_op / N)` with totals 12,000 / 2,500 / 6,000 / 6,000 and minimums 3,000 / 600 /
  2,500 / 2,500 (every window ≥ 2 min at WAN pace; N=1 ≤ 8 min).
- metrics snapshots of both pageservers, safekeepers and storcon before/after each run; `neon_ctl.py purge` on
  failure; `RUN_TIMEOUT` 3600 s.
- after the sweep: `ceiling_check.py --variant V --ns 256,512,1024 --kinds point,range,insert,update --seconds 30
  --procs 16` (creates max N branches once, prewarms them for the LFC variants, measures the first N per point).
- Pass per run: 0 failed ops; `client_cpu_cores` recorded; for (c) both pageservers show GetPage traffic within
  ±10% of each other; no storcon "is rate limited"; no pageserver "dio vec read failed" (count and report if any).

### 4. Sub-experiment C: branch creation (~1.5 h)
`run_branch_create.sh --ns 1,4,16,64,128,256,512`: exp 3's creation phase (`--sweep-branches --threads 1
--branch-list N --processes min(N+1,16) --operations BRANCH_CREATE --branch-create-ops 100
--branch-create-connect-every 5 --background-op MIXED`, prewarm on, one tenant) -> `data/branch_create/`. Pass: 100
creates per N, 20 compute starts, background in-flight share recorded.

### 5. Sub-experiment D: cold start (~1 h)
`cold_start.py --runs 3 --reads 40000`: per run, purge, create a branch of the root, time the first connection
(compute start), then 40,000 uniformly random point reads (`s_w_id` 1..4, `s_i_id` 1..100000: dense keys, so no key
scan) on one psycopg2 connection, recording per-read latency and timestamps to parquet; the agent's 1 s samples
give LFC hits/misses/used and the start metrics. Pass: hit ratio rises from 0 toward ~0.25 and the rolling median
falls from ~41 ms toward the low-load value.

### 6. Collect, analyse, plot (~1 h)
`pull_data.sh`; `breakdown.py --data data/low_load`; `summarize.py`; the four plot scripts (load the `dataviz` skill
first); record `git rev-parse HEAD` and `code/shared_code.patch` (`git apply --check`).

Figures:
- `low_load.png`: median per op, bars exp 3 (one zone, LAN, no cache), exp 4 (two zones, W=60, 15% LFC), exp 5
  (two zones, W=4, 25% LFC); compute start hatched; RTT line.
- `low_load_breakdown.png`: exp 4's stacked components for exp 5 (and exp 4 two-zone beside it).
- `low_load_hit_miss.png`: READ, RANGE_READ, UPDATE split per statement into "served from the compute" vs "fetched
  from the pageserver" (threshold RTT/2 = 20 ms for READ; UPDATE: 1 vs 2 round trips; RANGE_READ: by round-trip
  count 0..8), median/p95 per class and the class share, with the LFC counter hit ratio as the check.
- `throughput_latency.png`: 2×2 per op, one line per variant walking N, runner points (hollow when client-limited),
  × points from the minimal client, every point labelled `N=…`, each line's max throughput ringed and annotated.
- `data_ops.png`: median/p95 and throughput vs N per variant; `scaling_bottleneck.png`: pageserver(s) WAN tx
  Mbit/s, GetPage/s, SSD reads/s, CPU; safekeeper SSD writes/s; client CPU; compute-node memory, vs N per variant.
- `branch_create.png`: median (and p95) create latency and median compute start vs N, with exp 3's one-zone Neon
  line as a light reference.
- `cold_start.png`: per-read latency vs time since the branch was created (log y, rolling median, compute start
  span marked) and below it the LFC hit ratio and fill vs time; three runs overlaid; the time to steady state annotated.

### 7. Write-up
`report.md` (verdict, one section per sub-experiment, figures, tables, caveats, data quality), `REPRODUCE.md` with
the executed commands and measured durations, README (question, verdict, layout, shared changes, superseded data),
`.agent/cloudlab.md` experiment 5 layout, memory note. Purge timelines; leave services running.

## Risks and fallbacks
- **Memory per compute** larger than ~90 MB: top N of the LFC variants drops to 768 or 512 (decided by the probe);
  the report states the reached N. WAL segments on the tmpfs (16 MB each) are the likely cost; `wal_init_zero=off`
  in the template is the first lever.
- **a5's WAN path** much slower than a2's: pick the best probed node among a5/a6/a7 for pageserver 2; the report
  shows both pageservers' uplink throughput.
- **Client**: expected limit 8-12k ops/s; the minimal client covers N ≥ 256 (its p50 at N=256 should match the
  runner's where both are unlimited).
- **Shard split**: if shard 1 does not move to node 2 (412 "worse-scoring node"), `override_scheduler` is already
  set; verify with `locate` before any variant (c) run. After the split, pageserver-side paths use shard ids
  (`{tenant}-0002`, `-0102`); `neon_ctl.py` metrics/layer calls iterate over `locate`. The `--tenant-rate-limit
  1000` storcon setting stays.
- **Sharded range reads** may get a little faster (pages of one range can be in flight on two connections);
  reported as a property of the variant, not hidden.
- **Time**: ~18 h machine time in total; B is split into three detached drivers so a failure costs one variant.

## Verification
- `uv run pytest tests/ -q` green with the new tests.
- Smoke before (a) and again after the split (a compute lists both pageservers and both serve GetPage),
  `probe_memory`, `probe_wan` logged.
- Per sub-experiment pass checks above; breakdown components sum to the client mean; statement counts match the
  compute logs; LFC hit ratio ≈ 0.25 (lfc25) and ≈ 0.06 (nolfc, shared_buffers only) at N=1.
- Figures regenerate from `data/` with the plot scripts alone.
