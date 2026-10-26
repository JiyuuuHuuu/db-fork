# Reproducing experiment 5

Everything is driven from a coordinator machine that holds the Neon build and this repo, and can ssh to every
node with `~/.ssh/dassl_rsa`. The nodes cannot ssh to each other. `X=experiment/5-neon-cross-zone-scaling`.

## 1. Machines

CloudLab experiment `branchDB` (project rasl-PG0), two groups without routers between them (`.agent/cloudlab.md`):

| Node | Hostname | LAN | Public | Role |
|---|---|---|---|---|
| a0 | c220g2-011312.wisc.cloudlab.us | 10.10.1.10 | 128.105.145.147 | storage controller + its Postgres, broker, hook stub, safekeeper 1 |
| a1 | c220g2-011127.wisc.cloudlab.us | 10.10.1.11 | 128.105.145.130 | safekeeper 2 |
| a2 | c220g2-030631.wisc.cloudlab.us | 10.10.1.12 | 128.105.145.168 | pageserver 1 (node id 1); exports the remote storage over NFS |
| a3 | c220g2-011014.wisc.cloudlab.us | 10.10.1.13 | 128.105.145.88 | safekeeper 3 |
| a4 | c220g2-011023.wisc.cloudlab.us | 10.10.1.14 | 128.105.145.97 | seeding compute (and seeding client) |
| a5 | c220g2-010822.wisc.cloudlab.us | 10.10.1.15 | 128.105.145.64 | pageserver 2 (node id 2; shard 1 of the split tenant) |
| b0 | apt107.apt.emulab.net | 10.10.2.10 | 128.110.96.107 | benchmark client |
| b1-b7 | apt116, apt095, apt118, apt101, apt109, apt115, apt112 .apt.emulab.net | 10.10.2.11-17 | 128.110.96.116, .95, .118, .101, .109, .115, .112 | compute nodes |

- Wisconsin (a*): c220g2, 2x Xeon E5-2660 v3 (40 threads), 157 GiB RAM, 447 GB Intel S3500 SSD at `/dev/sdc`
  (XFS at `/mnt/ssd`, holds all Neon data), 10 GbE LAN, 1 GbE control NIC (the WAN path).
- Utah (b*): r320, 1x Xeon E5-2450 (16 threads), 15 GiB RAM, HDDs only. LFC files on a 12 GB tmpfs
  (`/mnt/tmpfs/computes`); data directories on the tmpfs (low load, cold start) or on the root HDD
  (`~/neon-computes`, the runs with hundreds of computes per node).
- Round trip between zones 38.8-38.9 ms; each pageserver sends about 915 Mbit/s to the compute nodes, and the two
  pageservers' paths are independent (`logs/probe_wan.out`).
- Memory per compute (`logs/probe_memory*.out`): 102 MB with the data directory on the tmpfs, 83 MB with it on disk
  (default cache, after the LFC filled). With 15 GB per node that is about 156 computes per node, so N=1024 (about
  146 per node) fits only with the data directories on disk.

## 2. Software and code

- Neon fork at `/mydata/jiyu/neon` on the coordinator, commit `4187e2da7` (branch `dev`), release build with
  `--features=testing`, Postgres 17.5 from `pg_install/v17`. Binaries are copied, not rebuilt; the Utah nodes other
  than b1 fetch them from b1 over the Utah LAN.
- This repo at commit `f004d9f` plus `code/shared_code.patch` (`git apply --check` passes on f004d9f).
- Client and seeding node: uv, Python 3.13, `uv sync --group dev`; generated `*_pb2.py` copied from the coordinator.

## 3. Parameters (`scripts/cluster.env`, `config/compute_config_template.json`)

| Parameter | Value |
|---|---|
| Dataset | CH-benCHmark, W=4 (2.1 M rows, database 0.36 GiB; stock 400,000 rows, heap 133 MiB = 17,032 pages, stock_pkey 8.6 MiB = 1,099 pages) |
| Compute | shared_buffers 8 MB, LFC 36 MB = 4,608 pages (25.4% of stock) in 8 kB chunks, effective_io_concurrency 2, max_connections 25, fsync off, wal_init_zero off |
| Cache fill (prewarm) | stock heap pages 0-3,399 (20%), then `pg_prewarm('stock_pkey')`, untimed, before every client's warm-up (also the background clients); `maintenance_io_concurrency = 128` |
| Statement logging | on for low load and cold start (breakdown), off for scaling and branch creation |
| Storage controller | `--tenant-rate-limit 1000` |
| Two pageservers | tenant split into 2 shards, stripe 256 pages (2 MiB); shard 0 on a2, shard 1 on a5; remote storage of both = a2's `/mnt/ssd/ps-remote` (NFS on a5) |
| A, low load | N=1, 1 client: 40,000 READ, 10,000 RANGE_READ (100 rows), 6,000 INSERT, 6,000 UPDATE, +10% warm-up; 100 branch creates, every 5th connected, one MIXED background client; computes on b1 |
| B, scaling | N = 1, 4, 16, 64, 128, 256, 512, 1024 (two pageservers: 1, 64, 256, 512, 1024, no inserts); one client per branch, synchronous; per client max(total/N, minimum) ops with totals 12,000 / 2,500 / 6,000 / 6,000 and minimums 3,000 / 600 / 2,500 / 2,500 (READ / RANGE_READ / INSERT / UPDATE), capped at 1,000,000 reads and 640,000 writes per run, +10% warm-up; client processes min(N, 16); computes on b1-b7, data directories on disk; bench role `statement_timeout = 600s` |
| B, minimal client | `ceiling_check.py`, N = 128, 256, 512, 1024, 30 s per point, 16 processes |
| C, branch creation | N = 1, 4, 16, 64, 128, 256, 512 busy branches (MIXED), 100 creates, every 5th connected |
| D, cold start | 3 runs; each: create a branch, start its compute (first connection), 30,000 uniformly random point reads on one connection; computes on b1 |

## 4. Commands, in order

```bash
# 4.1 deploy (about 25 min the first time; idempotent; --skip-binaries afterwards; --restart wipes all Neon state)
$X/scripts/setup_neon_cluster.sh                       # logs/setup_cluster*.out
$X/scripts/setup_remote_storage_nfs.sh                 # logs/setup_remote_storage_nfs.out
$X/scripts/setup_neon_cluster.sh --status

# 4.2 probes (5 min, 15 min)
$X/scripts/probe_wan.sh > $X/logs/probe_wan.out

# 4.3 seed on a4 over the Wisconsin LAN (4 min)
ssh a4 'cd db-fork && uv run python $X/scripts/neon_ctl.py env-init --zone a && uv run python $X/scripts/neon_ctl.py seed --scale 4 --pageserver-node 1'
#   pass: seed.json stock rows 400,000, layers Image > 0; the tenant is on node 1 with scheduling Pause
scp a4:db-fork/$X/data/seed/seed.json $X/data/seed/
$X/scripts/sync_client.sh b0
ssh b0 'cd db-fork && cat > $X/data/seed/seed.json' < $X/data/seed/seed.json
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py env-init --from-seed-json $X/data/seed/seed.json && uv run python $X/scripts/neon_ctl.py clock-check && uv run python $X/scripts/neon_ctl.py smoke'
ssh b0 'cd db-fork && uv run python $X/scripts/probe_memory.py --node b2 --computes 100 --reads 200 --pgdata-on-disk'

# 4.4 smoke runs (about 30 min) -> data/superseded/smoke_*
$X/scripts/launch.sh exp5-smoke-low $X/logs/driver_smoke_low_load.out $X/scripts/run_low_load.sh --smoke --ops READ
$X/scripts/launch.sh exp5-smoke-b $X/logs/driver_smoke_b.out bash -c "$X/scripts/run_scaling.sh --variant nolfc --smoke --ns 4,64 --ops READ,INSERT && $X/scripts/run_scaling.sh --variant lfc25 --smoke --ns 64 --ops READ,UPDATE && $X/scripts/run_branch_create.sh --smoke --ns 4"

# 4.5 the measured runs, in order, as one transient systemd unit on b0 (logs/run_all.log; a rerun skips DONE steps)
$X/scripts/launch.sh exp5-all $X/logs/run_all_driver.out $X/scripts/run_all.sh
#   low_load 84 min, cold_start 56 min, branch_create 47 min,
#   scaling_nolfc about 5.3 h of runs + ceiling_nolfc 17 min, scaling_lfc25 4.1 h + ceiling_lfc25 23 min,
#   split 2 min, scaling_lfc25_2ps 2.3 h (N = 1, 64, 256, 512, 1024; no inserts) + ceiling_lfc25_2ps 26 min
#   pass: every progress.log line DONE (a FAILED run is retried once at the end of its sweep); summarize.py: fail=0
#
# The split step as it should run (it was finished by hand; see logs/notes.md): the shard split is one-way, so
# every one-pageserver run must be done first.
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py purge && uv run python $X/scripts/neon_ctl.py shard-split --shards 2'
#   92 s; data/seed/shard_split.json: shard ...-0002 on node 1 (a2), ...-0102 on node 2 (a5), stripe 256
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py env-init --from-seed-json $X/data/seed/seed.json'
#   pass: .env holds NEON_LOCAL_PS_OPTS='{"pageservers": [...a2..., ...a5...], "stripe_size": 256}' (single quotes)
ssh b0 'cd db-fork && uv run python $X/scripts/lfc_contents_probe.py --node b2 --reads 2000 --heap-first --out lfc_probe_2ps_smoke.json'
#   pass: both pageservers' get_page_at_lsn counters rise by about the same amount
$X/scripts/launch.sh exp5-2ps $X/logs/run_all_driver_2ps.out $X/scripts/run_all.sh scaling_lfc25_2ps

# 4.6 collect, analyse, plot (on the coordinator; pull_data.sh gzips the agents' samples and recompresses large
#     result files, about 8 GB in all)
$X/scripts/pull_data.sh
uv run python $X/scripts/breakdown.py --offsets $X/data          # figures/breakdown.{csv,json,md}
uv run python $X/scripts/plot_low_load.py                         # low_load.png, low_load_breakdown.png, low_load_hit_miss.png
uv run python $X/scripts/plot_scaling.py                          # throughput_latency.png, data_ops.png, scaling_summary.*
uv run python $X/scripts/resource_use.py                          # scaling_bottleneck.png, resource_use.csv
uv run python $X/scripts/plot_branch_create.py                    # branch_create.png
uv run python $X/scripts/plot_cold_start.py                       # cold_start.png
# The lines above make the first report's figures (now in superseded/report_lfc_w4/figures/). The current report
# (regenerated 25 Oct 2026) uses experiment 8's cache runs: after experiment 8's own analysis (its REPRODUCE.md),
# and with superseded/report_lfc_w4/figures/resource_use.csv holding this experiment's no-LFC rows:
uv run python $X/scripts/plot_with_exp8.py                        # every figure in figures/
```

Check after the pull: every run has all N client threads and no failed operations (the report's data-quality
section lists the query); the only non-OK rows are the client's occasional duplicate-key inserts.

## 5. Cleanup

```bash
ssh b0 'cd db-fork && uv run python $X/scripts/neon_ctl.py purge'     # computes and timelines except the seed
# the split is one-way: rerunning the one-pageserver variants needs a new seed (4.3)
```

## 6. Pitfalls

- Shards move between pageservers only through remote storage: without the shared directory a migrated shard has
  no layers. `setup_remote_storage_nfs.sh` must run before the split.
- Every pageserver but the first registers with scheduling `Pause`, and the tenant is pinned with a `Pause`
  scheduling policy: otherwise the storage controller places new tenants on the emptiest pageserver.
- Background processes started over ssh (`setsid nohup ... &`) kept the session open; `launch.sh` uses
  `systemd-run` instead, and the binary fan-out uses a transient unit for its HTTP server.
- `pkill -f <pattern>` over ssh matches the remote shell's own command line; anchor the pattern.
- A compute's data directory holds a fully allocated 16 MB WAL segment even with `wal_init_zero=off`; with the LFC
  file that made 102 MB per compute on the tmpfs. Data directories go on disk for the many-compute runs.
- `scripts/bottleneck.py` would shadow pandas' optional `bottleneck` import: the resource script is `resource_use.py`.
- The venv has no `tabulate`; Markdown tables come from `load_runs.to_md`.
- **The client node needs `RemoveIPC=no` too.** Otherwise each ssh logout removes the runner's POSIX semaphores
  and its client processes fail to start (`SemLock._rebuild ... FileNotFoundError`). The deploy script sets it.
- **Client memory.** The runner keeps every result row in memory until a run ends; on the 15 GB r320 client,
  1,024 clients x 2,500 writes swapped for an hour. `run_scaling.sh` caps a run at 1,000,000 reads, 640,000 writes.
- **Prewarm order.** Load the heap prefix first and the primary key last; the reverse evicts index leaves first.
- **Compute placement.** A hash of the timeline id put up to 40% more computes on one node than on another; the
  shared change maps numbered branches round-robin to the agents.
- **Deleting written-to timelines is slow** (seconds each); `NEON_LOCAL_PURGE_PARALLEL=64` in common.sh.
- **Compute-notification stub.** The single-threaded stub of experiments 3-4 hung on one connection, every
  notification then timed out and the tenant's reconciliation never finished; a shard split needs a successful
  notification. `config/compute_hook_stub.py` is threaded with a timeout.
- **.env and JSON.** A value with double quotes must be written in single quotes (`neon_ctl.set_env` does).
- **Coordinator disk.** The coordinator's root filesystem is shared and was full twice; keep the samples gzipped and
  run `scripts/recompress.py` on the result files.
- **Memory on the compute nodes at N = 1,024 with the LFC:** 1.1-1.5 GB left of 15 GB, 0.8 GB swapped; it held, but
  a larger LFC or N would not.
- Index-only scans get no prefetch on Neon: the runner's key scan over 400,000 stock keys (1,099 index pages) takes
  about 45 s over the WAN, once per client process.
