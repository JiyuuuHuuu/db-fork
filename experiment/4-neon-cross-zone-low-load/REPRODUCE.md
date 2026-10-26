# Reproducing experiment 4

Everything is driven from a coordinator machine that holds the Neon build and this repo, and can ssh to every
node with `~/.ssh/dassl_rsa`. The nodes cannot ssh to each other. `X=experiment/4-neon-cross-zone-low-load`.

## 1. Machines

CloudLab experiment `branchDB` (project rasl-PG0), two groups without routers between them (`.agent/cloudlab.md`):

| Node | Hostname | LAN | Public | Role |
|---|---|---|---|---|
| a0 | c220g2-011312.wisc.cloudlab.us | 10.10.1.10 | 128.105.145.147 | storage controller + its Postgres, broker, hook stub, safekeeper 1 |
| a1 | c220g2-011127.wisc.cloudlab.us | 10.10.1.11 | 128.105.145.130 | safekeeper 2 |
| a2 | c220g2-030631.wisc.cloudlab.us | 10.10.1.12 | 128.105.145.168 | pageserver |
| a3 | c220g2-011014.wisc.cloudlab.us | 10.10.1.13 | 128.105.145.88 | safekeeper 3 |
| a4 | c220g2-011023.wisc.cloudlab.us | 10.10.1.14 | 128.105.145.97 | seeding compute (and seeding client) |
| b0 | apt107.apt.emulab.net | 10.10.2.10 | 128.110.96.107 | benchmark client |
| b1 | apt116.apt.emulab.net | 10.10.2.11 | 128.110.96.116 | compute node |
| a5 | c220g2-010822.wisc.cloudlab.us | 10.10.1.15 | 128.105.145.64 | one-zone setup: benchmark client |
| a6 | c220g2-011302.wisc.cloudlab.us | 10.10.1.16 | 128.105.145.137 | one-zone setup: compute node (125 GiB RAM) |

- Wisconsin (a*): c220g2, 2x Xeon E5-2660 v3 (40 threads), 157 GiB RAM, 447 GB Intel S3500 SSD at `/dev/sdc`
  (XFS at `/mnt/ssd`, holds all Neon data), 10 GbE LAN.
- Utah (b*): r320, 1x Xeon E5-2450 (16 threads), 15 GiB RAM, HDDs only; computes on a 6 GB tmpfs.
- Round trip between zones 38.8-38.9 ms, no loss; within Wisconsin 0.15 ms, within Utah 0.22 ms
  (`logs/network_probe.out`). Cross-zone traffic uses the public addresses (1 GbE control NICs).
- All nodes Ubuntu 22.04, kernel 5.15.0-187, NTP synchronized.

## 2. Software and code

- Neon fork at `/mydata/jiyu/neon` on the coordinator, commit `4187e2da7` (branch `dev`), release build with
  `--features=testing`, Postgres 17.5 from `pg_install/v17`. Binaries are copied, not rebuilt.
- This repo at commit `6bb15c7` plus `code/shared_code.patch` (`git apply --check` passes on 6bb15c7): the sorted
  key cache for range reads and the `DBFORK_PREWARM_SQL` hook in `microbench/runner2.py`.
- Client and seeding node: uv, Python 3.13, `uv sync --group dev`; the generated `*_pb2.py` are copied from the
  coordinator (no protoc on the nodes).

## 3. Parameters (`scripts/cluster.env`)

| Parameter | Value |
|---|---|
| Dataset | CH-benCHmark, 60 warehouses (30.05 M rows, database 5.11 GiB; stock heap 1996 MiB, stock_pkey 128.5 MiB) |
| Compute | shared_buffers 128 MB, LFC 448 MB in 8 kB chunks (`neon.file_cache_chunk_size=1`), effective_io_concurrency 2, fsync off |
| Cache fill before each data-op run | `pg_prewarm('stock_pkey')` and the first 38,400 stock heap pages (15.0%) |
| Statement logging | `log_min_duration_statement=0`, `neon.monitor_query_exec_time=on` on the measured computes |
| Storage controller | `--tenant-rate-limit 1000` (experiment 3's low-load branch-create configuration) |
| Workload | N = 1 busy branch, 1 client, synchronous; 40,000 READ, 10,000 RANGE_READ (100 rows), 6,000 INSERT, 6,000 UPDATE, +10% untimed warm-up each; 100 branch creates, every 5th connected (compute start), with one MIXED background client |

## 4. Commands, in order

```bash
# 4.1 network probe (2 min): ping and iperf3 from b0 and b1 to a2 -> logs/network_probe.out (see the file for the commands)

# 4.2 deploy (about 20 min; copying pg_install to Utah dominates). Idempotent; --restart wipes all Neon state.
$X/scripts/setup_neon_cluster.sh                 # logs/setup_cluster*.out
$X/scripts/setup_neon_cluster.sh --status        # every expected unit active, pageserver Active, b0 reaches a2:9898

# 4.3 env files and clock check
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py env-init && uv run python $X/scripts/neon_ctl.py clock-check'
ssh a4 'cd db-fork && uv run python $X/scripts/neon_ctl.py env-init --zone a'

# 4.4 seed on a4 over the Wisconsin LAN (54 min at W=60: the Python generator is the bottleneck)
ssh a4 'cd db-fork && PYTHONUNBUFFERED=1 setsid nohup uv run python $X/scripts/neon_ctl.py seed --scale 60 --shared-buffers 8GB > $X/logs/seed.out 2>&1 < /dev/null &'
#   pass: seed.json rows (stock 6,000,000), layers with Image > 0, last_record_lsn above the images
scp a4:db-fork/$X/data/neon_local/seed.json $X/data/neon_local/
$X/scripts/sync_client.sh b0
ssh b0 'cd db-fork && cat > $X/data/neon_local/seed.json' < $X/data/neon_local/seed.json
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py env-init --from-seed-json $X/data/neon_local/seed.json'

# 4.5 checks (about 15 min): smoke (branch, compute start on b1, read, write, delete), prewarm speed, first connection
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py smoke'          # 11 min: count(*) is a 16k-page index-only scan at one page per round trip
ssh b0 'cd db-fork && uv run python $X/scripts/prewarm_probe.py --blocks 2000'      # about 330 pages/s
ssh b0 'cd db-fork && uv run python $X/scripts/prewarm_probe.py --connect-probe'    # logs/connect_probe.out

# 4.6 smoke run (25 min) -> data/superseded/smoke_neon_local
ssh b0 'cd db-fork && setsid nohup $X/scripts/run_low_load.sh --smoke --ops READ > $X/logs/driver_smoke.out 2>&1 < /dev/null &'

# 4.7 full run (2 h 3 min: READ 35 min, RANGE_READ 42 min, INSERT 14 min, UPDATE 16 min, branch create 15 min;
#     each data-op run spends 7-11 min on the runner's key scan and 3 min on the prewarm) -> data/neon_local
ssh b0 'cd db-fork && setsid nohup $X/scripts/run_low_load.sh > $X/logs/driver.out 2>&1 < /dev/null &'
#   pass: progress.log has DONE for data_READ_n1, data_RANGE_READ_n1, data_INSERT_n1, data_UPDATE_n1, create_n1

# 4.8 collect, analyse, plot (on the coordinator)
$X/scripts/pull_data.sh
uv run python $X/scripts/breakdown.py           # figures/breakdown.{csv,json,md}; prints consistency checks
uv run python $X/scripts/plot_low_load.py       # figures/low_load.png, figures/low_load_breakdown.png
```

## 4b. The one-zone setup (about 30 min)

Same storage layer and seed; client a5 (c220g2-010822, 10.10.1.15 / 128.105.145.64) and compute node a6
(c220g2-011302, 10.10.1.16 / 128.105.145.137; computes on its SSD). Run after 4.1-4.4.

```bash
DBFORK_SETUP=onezone $X/scripts/setup_neon_cluster.sh     # 10 min; formats a6's SSD, copies compute binaries, sets up a5
#   (rewrites a0-a3's agent units and restarts those agents; services and data are untouched)
# network probe: ping/iperf3 a6 -> a2 on the public and LAN addresses, a5 -> a6 and a5 -> a0 -> logs/network_probe_onezone.out
ssh a5 'cd db-fork && cat > $X/data/neon_local/seed.json' < $X/data/neon_local/seed.json
ssh a5 'cd db-fork && DBFORK_SETUP=onezone uv run python $X/scripts/neon_ctl.py env-init --from-seed-json $X/data/neon_local/seed.json'
ssh a5 'cd db-fork && uv run python $X/scripts/neon_ctl.py clock-check'                       # within 0.2 ms
ssh a5 'cd db-fork && $X/scripts/run_low_load.sh --smoke --ops READ'                          # 2 min
ssh a5 'cd db-fork && setsid nohup $X/scripts/run_low_load.sh > $X/logs/driver_onezone.out 2>&1 < /dev/null &'   # 8 min
DBFORK_SETUP=onezone $X/scripts/pull_data.sh
uv run python $X/scripts/breakdown.py --data $X/data/neon_local_onezone --compute-node a6 \
    --net-label "public network" --tag onezone
uv run python $X/scripts/plot_low_load.py        # both setups (reads figures/breakdown.json and breakdown_onezone.json)
```

`env-init` writes `DBFORK_SETUP=onezone` into a5's `.env`, so `run_low_load.sh` and `neon_ctl.py` there pick up
`scripts/cluster_onezone.env` (client, compute node, `data/neon_local_onezone`).

Pass checks for the breakdown: per bar the components sum to the client mean, the number of logged
statements equals the number of client statements, the LFC hit ratio and GetPage waits per point read match the
cache design (about 0.85 GetPage waits per point read).

## 5. Cleanup

```bash
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py purge'     # computes and timelines except the seed
# to stop the cluster: on every agent node  sudo systemctl stop neon-agent neon-pageserver neon-safekeeper neon-storcon neon-hook neon-storcon-pg neon-broker
```

## 6. Pitfalls

- `ssh host cmd args` re-splits arguments on spaces; the setup script passes the agent command line base64-encoded.
- The agent must set `disable_nagle_algorithm`: on kept-alive HTTP connections every reply otherwise waited 40 ms for
  the client's delayed ACK (clock check showed 120 ms round trips over a 39 ms path).
- Seeding compute: statement logging off (`--no-statement-log`), otherwise the bulk INSERTs write gigabytes of log
  (the first seed attempt was aborted for this; `data/superseded/seed1_statement_log`).
- Neon's LFC defaults to 1 MB chunks; with uniform random reads over a table 4x the cache each chunk holds a few
  pages and the hit ratio collapses. This experiment uses 8 kB chunks so the cache holds whole pages.
- Each run's compute starts with an empty LFC; the runner's key scan runs on the root branch's compute, not on the
  measured branch. `DBFORK_PREWARM_SQL` fills the measured compute's cache before the warm-up.
- Index-only scans get no prefetch on Neon: the runner's key scan (all 6 M stock keys, 16,448 index pages) takes
  about 7-11 min per run over the WAN, and `count(*)` on stock as long. `pg_prewarm` (read stream) gets about 13 pages
  per round trip.
- The data-op runs write one parquet per output directory and its name has no operation in it, so each operation
  gets its own directory (`data_ops/<OP>/`).
- `neon_perf_counters` is readable by cloud_admin only; read it through the agent (`POST /computes/<tl>/sql`).
- A fresh compute's first connection to a database reads that database's catalog pages over the WAN (about 50
  round trips, 1.9-2.4 s); the agent's readiness check pays it for `postgres`, the client's first connection for `bench`.
