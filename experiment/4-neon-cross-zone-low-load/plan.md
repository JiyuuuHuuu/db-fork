# Experiment 4: Neon low-load latency with compute and storage in different zones

## Context

Experiment 3 measured Neon's low-load latency (`experiment/3-neon-branch-scaling/figures/low_load.png`: N = 1 busy
branch, one client; median of point READ, RANGE_READ 100 rows, INSERT, UPDATE, branch create, plus a hatched
compute-start bar) with every Neon service on one 10 GbE LAN. The new CloudLab cluster (`.agent/cloudlab.md`,
probed 2026-10-22) spans Wisconsin (a0-a7, c220g2, SSD) and Utah (b0-b7, r320, 16 GB RAM, HDDs only) with a
stable 38.9 ms round trip between them. The user wants to re-measure the Neon bars with the storage layer in one
zone and the client plus computes in the other, so compute-to-storage traffic behaves like cross-cloud traffic,
with the compute's local file cache (LFC) enabled, a dataset larger than the compute caches, and a latency
breakdown for every bar.

Decisions already taken with the user:
- Storage zone = Wisconsin (only zone with SSDs; every data path must be on SSD). Client and computes = Utah.
- Utah has no SSD, so compute directories (pgdata + LFC file) live on a **tmpfs** mount.
- Cache design: about **15% of point reads served from the compute cache** once warm.
- The new figure shows experiment 3's single-zone Neon bars as the reference (no Dolt rerun).
- Simple analysis, Markdown report, figures embedded; experiment layout per `.agent/CLAUDE.md`.

## Placement

| Node | Zone | Role |
|---|---|---|
| a2 (`c220g2-030631`) | Wisconsin | pageserver (best WAN path to Utah; pageserver->compute is the slow direction) |
| a0 (`c220g2-011312`) | Wisconsin | control: storage controller + its Postgres, broker, compute-hook stub, safekeeper 1 |
| a1, a3 | Wisconsin | safekeepers 2 and 3 |
| a4 | Wisconsin | agent with `--computes`: hosts the **seeding** compute only (LAN to storage) |
| a5, a6, a7 | Wisconsin | spare SSD nodes, reserved for more pageservers later (a7's SSD is its root disk; a6 has 125 GB) |
| b0 (`apt107`) | Utah | benchmark client (`~/db-fork`, uv venv, `.env`) |
| b1 (`apt116`) | Utah | compute node for this experiment (`NEON_LOCAL_AGENTS` = b1 only) |
| b2-b7 | Utah | spare compute nodes |

Disks: on every a-node `/dev/sdc` -> XFS at `/mnt/ssd`; binaries, systemd wrappers, pageserver dir, pageserver
"remote storage" (local_fs), safekeeper dirs and their remote storage, the storcon database and a4's compute root
all go under `/mnt/ssd` (nothing on `/mydata`, which does not exist on the new nodes). On b1: `tmpfs` 6 GB at
`/mnt/tmpfs` (computes in `/mnt/tmpfs/computes`), binaries and log archive on the root disk.

Addressing: intra-zone service traffic on LAN IPs (storcon <-> pageserver <-> safekeepers <-> broker <-> hook,
all in Wisconsin; client <-> compute agent in Utah). Cross-zone on public IPs: compute `pageserver_connstring`
-> a2 public:64000, `neon.safekeepers` -> a0/a1/a3 public:5454, client `NEON_LOCAL_STORCON_URL` -> a0
public:1234, `NEON_LOCAL_PAGESERVER_URL` -> a2 public:9898, safekeeper HTTP -> public:7676. Storcon runs with
`--tenant-rate-limit 1000` from the start (experiment 3's low-load branch-create bar came from that config).

## Parameters (in `scripts/cluster.env`)

| | Value | Reason |
|---|---|---|
| W (warehouses) | 60 | stock table ~2 GB heap + ~165 MB PK leaves; whole DB ~6 GB; fits the 447 GB SSDs easily |
| shared_buffers | 128MB | small compute, as a 0.25 CU cloud compute |
| LFC | 512MB | the run's key scan pins the PK leaf pages in the LFC, so heap hit ratio ~ (512-165)/2000 ~ 15-17% |
| effective_io_concurrency | 2 | experiment 3's value, kept for comparability (noted as a knob in the report) |
| seed compute shared_buffers | 8GB | on a4, LFC off |
| tmpfs | 6G | pgdata + LFC on b1 (15 GB RAM) |

## Files to create: `experiment/4-neon-cross-zone-low-load/`

```
README.md  plan.md  REPRODUCE.md  report.md  figures/  logs/  data/neon_local/  data/superseded/  code/
config/compute_config_template.json   from exp 3; postgresql_conf gains LFC, logging, query-time GUCs
config/compute_hook_stub.py            copied unchanged
scripts/cluster.env                    two-zone layout, explicit HOST/LAN/PUB per node, parameters above
scripts/setup_neon_cluster.sh          from exp 3 (see below)
scripts/neon_node_agent.py             from exp 3 (see below)
scripts/neon_ctl.py                    from exp 3 (see below)
scripts/run_low_load.sh                from run_neon_busy_branches.sh, N = 1 only
scripts/clock_check.py                 client vs agent clock offsets -> data/neon_local/clock_offsets.json
scripts/pull_data.sh                   rsync results from b0 and the agents' archives from b1/a0/a1/a2/a3
scripts/breakdown.py                   per-bar latency breakdown -> figures/breakdown.csv + markdown table
scripts/plot_low_load.py               low_load.png and low_load_breakdown.png
scripts/summarize.py, ps_metrics_delta.py, sync_client.sh   copied (sync_client -> b0)
scripts/compute_start_probe.py         copied, constants turned into args (optional)
```

### `setup_neon_cluster.sh` (adapt exp 3's, keep it idempotent with `--skip-binaries`, `--restart`, `--status`)
- Helpers `host/lan/pub/zone n`; per-zone `NEON_BIN`/`NEON_SVC` (`/mnt/ssd/neon-bin`, `/mnt/ssd/neon-svc` on a*,
  `~/neon/bin`, `~/neon/svc` on b*). Drop every `/mydata` reference and the Dolt/Doltgres stop step.
- a*: mkfs.xfs `/dev/sdc` if blank, mount `/mnt/ssd` noatime. b*: tmpfs mount (+ fstab), `kernel.shmmni=32768`.
  `RemoveIPC=no` in logind everywhere.
- Binaries from `/mydata/jiyu/neon` (commit 4187e2da7, copied not rebuilt): pageserver -> a2; broker, storcon,
  storcon_cli -> a0; safekeeper -> a0,a1,a3; compute_ctl + `pg_install/v17` -> a4, b1 (rsync -z over the WAN).
- Agents (systemd `neon-agent`, port 9000): a4 `--computes --host lan(a4) --pageserver lan(a2):64000
  --safekeepers LAN list --shared-buffers 8GB --lfc-mb 0 --compute-root /mnt/ssd/computes`; b1 `--computes --host
  lan(b1) --pageserver pub(a2):64000 --safekeepers pub list --shared-buffers 128MB --lfc-mb 512 --compute-root
  /mnt/tmpfs/computes`; every agent `--archive-dir ~/neon-archive --sample-interval 1 --services
  label=http://127.0.0.1:<port>/metrics,...` (a2 pageserver; a0 storcon + sk1; a1 sk2; a3 sk3).
- Client bootstrap on b0 (and a4 for the seed client): exp 1's recipe
  (`experiment/1-dolt-branch-scaling/scripts/setup_cloudlab_dolt.sh:127-132`): uv, Python 3.13, rsync repo,
  `uv sync --group dev`, `build_protos.py`.
- `--status`: units per node, storcon node list, `curl pub(a2):9898/v1/status` from b0 (proves the WAN path).

### `neon_node_agent.py` (stdlib only; keep `/cpu` unchanged so `RemoteCpuSampler` still works)
1. New args `--lfc-mb`, `--eic`, `--archive-dir`, `--sample-interval`, `--services`. `render_config()` also
   substitutes `neon.max_file_cache_size`, `neon.file_cache_size_limit`, `neon.file_cache_path='<dir>/file.cache'`,
   `effective_io_concurrency` (`--lfc-mb 0` disables the LFC).
2. Start compute_ctl with `INSTANCE_ID=<timeline>` in its env so `GET /metrics.json` on the external port is
   unauthenticated (`compute_tools/src/http/middleware/authorize.rs:66-78`). After the first successful `select 1`
   (untimed, after `start_ms` is taken): fetch metrics.json, run `CREATE EXTENSION IF NOT EXISTS neon` in `postgres`
   (needed because `skip_pg_catalog_updates: true` skips it; counters are in shared memory so one DB suffices),
   append `{t0, t_ready, timeline, start_ms, metrics}` to `<archive>/starts.jsonl`, return `compute_metrics`.
3. Archive `<dir>/compute.log` (compute_ctl relays Postgres stderr as `PG:` lines) to
   `<archive>/pg/<timeline>_<t>.log` before any `rmtree` in `stop_compute()` / `delete_all()`.
4. Sampler thread, every 1 s: per running compute one `psql -d postgres -XAt` returning `neon_perf_counters`
   (non-bucket rows), `neon_get_lfc_stats()`, `backpressure_throttling_time()`; per `--services` URL a GET of
   `/metrics` filtered by a regex whitelist (`pageserver_smgr_query_seconds_global_(sum|count){...get_page_at_lsn}`,
   `pageserver_wal_redo_seconds_*`, `pageserver_layers_per_read_global_*`, `safekeeper_(flush|write)_wal_seconds_*`,
   `storage_controller_http_request_latency_*{...timeline}`, `storage_controller_pageserver_request_latency_*`).
   One JSON line per tick in `<archive>/samples.jsonl`. Plus `GET /time` for the clock check.

### `compute_config_template.json` postgresql_conf
Keep exp 3's lines (`fsync=off`, `synchronous_standby_names=walproposer`, `wal_sender_timeout=5s`, ...); set
`shared_buffers=128MB`, `effective_io_concurrency=2`, `neon.max_file_cache_size=512MB`,
`neon.file_cache_size_limit=512MB`, `neon.file_cache_path='file.cache'`, `neon.monitor_query_exec_time=on`
(otherwise `query_time_seconds_*` stays empty; `pgxn/neon/neon.c:551`), `log_min_duration_statement=0`,
`log_line_prefix='%m [%p] %u@%d '`, `log_statement=none`.

### `neon_ctl.py`
- `env-init` writes the public URLs above, `NEON_LOCAL_AGENTS=http://lan(b1):9000`,
  `NEON_LOCAL_SHARED_BUFFERS=128MB`, `DBFORK_REMOTE_CPU` for pageserver (pub a2), sk1-3 (pub a0/a1/a3, proc
  `safekeeper`, disk sdc) and compute1 (lan b1, proc `postgres,compute_ctl`). `--zone a` writes the LAN variant
  with agent a4 for seeding; `--from-seed-json` copies tenant/timeline ids into b0's `.env`.
- `seed --scale 60 --shared-buffers 8GB` runs on a4 (unchanged logic: load, VACUUM ANALYZE, wait for ingest,
  `compact_images`, `bump_past_images`; verified it only uses `NEON_LOCAL_*` URLs and the agent).
- `metrics` also snapshots storcon `/metrics`.

### `run_low_load.sh`
Exp 3's wrapper with `NS=1`, `--processes 1`, ops 40000 READ / 10000 RANGE_READ / 6000 INSERT / 6000 UPDATE
(smoke: 10% with a floor of 300), `COMMON` unchanged (`--mode sync --warmup-fraction 0.1 --table-name stock
--updates-per-branch 100 --seed-db bench --setup-parallelism 32`), branch create `--sweep-branches --branch-list 1
--processes 2 --operations BRANCH_CREATE --branch-create-ops 100 --branch-create-connect-every 5 --background-op
MIXED`, `RUN_TIMEOUT=7200`, `neon_ctl.py metrics` before/after each run, epoch timestamps in `progress.log`.

### `breakdown.py` (coordinator, after `pull_data.sh`)
Window = `[min start_time, max end_time]` of the measured OK statement rows (warm-up rows are not recorded);
counters interpolated at the edges with `RemoteCpuSampler._at` (import from `microbench.runner2`) on the agents'
`samples.jsonl` with the measured clock offsets applied; deltas divided by op count. Stacks, in ms per op (means),
with the median total annotated and the 38.9 ms RTT as a reference line:
- **READ, RANGE_READ**: client - server (LAN + libpq/python; server = per-statement `duration:` lines of
  `bench@bench` in the archived Postgres log, count must match op count); server - query_time (parse/plan/protocol);
  query_time - getpage_wait - lfc_read_wait (executor CPU); LFC read wait; GetPage wait split into pageserver
  processing (`smgr_query_seconds` delta) and WAN remainder. Report GetPage requests/op, LFC hit ratio, prefetch share.
- **INSERT, UPDATE**: same plus commit wait = server - query_time (this fork has no quorum-commit counter), split
  into safekeeper fsync (`flush_wal` mean x flushes/op, quorum of 3) and WAN remainder; backpressure time if any.
- **Branch create**: client BRANCH latency; storcon-side (`storage_controller_http_request_latency` for the
  timeline POST); pageserver-side (`storage_controller_pageserver_request_latency`); WAN = remainder.
- **Compute start**: CONNECT latency (20 samples) = client->agent + agent `start_ms`; `start_ms - total_startup_ms`
  (launch + readiness polling); `total_startup_ms` = sync_sk_check + sync_safekeepers + basebackup +
  start_postgres + config (+ load_ext), from `starts.jsonl`.

### `plot_low_load.py`
Reuse `load`/`fig_low_load` from exp 3's `plot_neon_vs_dolt.py` with the exp 3 run dirs plus the new
`data/neon_local` labelled "Neon cross-zone": `low_load.png` (log y, exp 3 Neon vs cross-zone Neon, both
compute-start bars hatched) and `low_load_breakdown.png` (one panel per bar, linear y, stacked components from
`breakdown.csv`). Load the `dataviz` skill before writing it.

## Shared code change (one, recorded in `code/shared_code.patch` and the README)
`microbench/runner2.py` `prepare_range_query` (~line 1823) sorts the whole key set on every range read; at 6M
keys that is seconds per op (untimed, but it would make the RANGE_READ run take a day). Cache the sorted list with
`_cached_meta(self._meta_key("sorted_pks", table_name), ...)`; add a test in `tests/test_micro_runner2.py`.
Results are unchanged. Nothing else in the shared tree needs to change (`NEON_LOCAL_SHARED_BUFFERS` passthrough
exists in `dblib/neon_local.py:140`; LFC is an agent default).

## Command sequence (coordinator; `X=experiment/4-neon-cross-zone-low-load`)
1. Create the directory and files; write `plan.md` (this plan) and a README skeleton. Apply the shared change,
   run `uv run pytest tests/ -q`.
2. Network probe from b0: ping and iperf3 to pub(a2) both directions -> `logs/network_probe.out` (RTT 38.8-39.0 ms,
   no loss).
3. `$X/scripts/setup_neon_cluster.sh` (~20 min, pg_install to b1 over the WAN dominates). Pass: `--status` all
   units active, pageserver Active in storcon, b0 reaches a2:9898, every agent `/cpu` and `/time` answer,
   mounts right, `shmmni` 32768 on b1, storcon journal has no "is rate limited".
4. `clock_check.py` from b0: |offset| < 5 ms for every agent, else note and apply.
5. Seed on a4 (~1 h at W=60): `neon_ctl.py env-init --zone a` then `seed --scale 60 --shared-buffers 8GB`. Pass:
   `seed.json` row counts (stock = 6,000,000), image layers present and `last_record_lsn` above them, `du -sh
   /mnt/ssd/ps` ~ 6-9 GB, a4 has no running compute afterwards. Copy `seed.json` to b0's `.env`.
6. Smoke on b0 (`neon_ctl.py smoke`, ~3 min): compute starts on b1 (`start_ms` ~0.5-1.5 s, `basebackup_ms`,
   `sync_safekeepers_ms` reported), `neon_get_lfc_stats()` shows `file_cache_used` > 0 after a read,
   `SHOW neon.max_file_cache_size` = 512MB, archived log has `duration:` lines, `samples.jsonl` grows 1 line/s.
   Also `EXPLAIN SELECT s_w_id, s_i_id FROM stock` must be an index-only scan (the runner's key scan; a seq scan
   of 2 GB at 2 requests in flight would take over an hour per run; fix = `VACUUM` so the visibility map is set).
7. Smoke run `run_low_load.sh --smoke` (~15 min) -> `data/superseded/smoke_neon_local`; `breakdown.py` on it
   must produce all six stacks with non-negative components and server-duration count = op count.
8. Full run detached on b0 (~1.5-2 h: READ ~30 min, RANGE_READ ~35 min, INSERT and UPDATE ~15 min each, branch
   create ~5 min; each run also pays a ~5 min key scan). Pass: five DONE, 0 failed ops, p50 READ ~40 ms, branch
   create ~45-50 ms, no "is rate limited", no pageserver "dio vec read failed".
9. `pull_data.sh`, `breakdown.py`, `plot_low_load.py`, `summarize.py`; record `git rev-parse HEAD` and the patch.
10. `report.md` (verdict, the two figures, breakdown table, LFC hit ratio, caveats), `REPRODUCE.md` from the
    executed commands with measured durations, README (question, verdict, layout, shared change). Update
    `.agent/cloudlab.md` with the experiment 4 layout. Purge timelines; leave services running until accepted.

## WAN-specific pitfalls to check
- Key scan per run (`util/db_helpers.py get_pk_values`): ~20k PK leaf pages at <= 2 requests in flight ~ 5 min
  per run, and it fills the LFC with the leaves (so a cold point read is one round trip). Check the gap between
  "Setup complete" and "Executing warm-up" in the run log.
- Range reads: Neon's PG17 prefetches index leaf and heap pages but only `effective_io_concurrency`=2 deep ->
  ~3-4 sequential round trips cold (~120-160 ms). Kept for comparability; report it.
- Bimodal reads (LFC hit ~0.5 ms vs miss ~40 ms): report mean and p50, and the hit ratio.
- Read-after-write on an evicted page waits for the WAL to reach the pageserver via the safekeepers (WAN): long
  `getpage_wait` tails in UPDATE.
- Timeouts (walproposer connect 10 s, safekeeper heartbeat 5 s, `wal_sender_timeout` 5 s, agent start 300 s) are
  all far above 39 ms; no change. Storcon<->pageserver and pageserver<->safekeeper stay on the LAN.
- b1 memory: tmpfs 6 GB + shared_buffers; watch the agent's `GET /stats` `mem_available_mb`.
- The agent's sampler statements appear in the log as `cloud_admin@postgres`; the parser filters on `bench@bench`.
- WAN packet loss (single streams lose): look at p99/max and the GetPage histogram buckets; keep before/after
  ping/iperf3 probes.

## Verification
- `uv run pytest tests/ -q` passes (plus the new sorted-PK cache test).
- Agent checks on b1 (hand-started compute): `compute_metrics` present, `neon` extension created, LFC GUCs,
  `file.cache` on tmpfs, archived log after stop.
- Breakdown consistency per bar: components sum to the client mean within 2%; GetPage requests/op ~ 1 for READ;
  branch-create WAN remainder ~ 39 ms; compute-start WAN parts ~ k x 39 ms; LFC hit ratio ~ 15%.
- `low_load.png` reproduces exp 3's Neon bars unchanged next to the new ones; breakdown totals match `summary.csv`.
