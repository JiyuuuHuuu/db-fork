# Reproducing experiment 10

Self-hosted Neon on one machine, every inter-service connection on a Unix domain socket, NUMA node 2 for Neon and
node 3 for the benchmark client. Everything below runs as user `jiyu` without sudo, except one setting that must
be on before any run (lingering, step 0).

## Machine

| | |
|---|---|
| Host | `dassl-serv-01` (local server, not CloudLab) |
| CPU | 4x Intel Xeon Gold 6418H, 96 cores, no SMT, 4 NUMA nodes of 24 cores; core ids are interleaved: node k holds k, k+4, k+8, ... |
| Memory | 251 GiB, about 64 GB per node |
| Neon | NUMA node 2: CPUs 2,6,...,94, memory of node 2 (`numactl --cpunodebind=2 --membind=2`) |
| Client | NUMA node 3: CPUs 3,7,...,95, memory of node 3 (`numactl --cpunodebind=3 --membind=3`) |
| Disk | `/mydata`: NVMe `nvme2n1`, ext4, about 300 GB free (shared with other users); multipath NVMe, so I/O counters live on `nvme2c2n1` |
| OS | Ubuntu 22.04, kernel 5.15.0-131, `ulimit -n` 1,048,576, `kernel.shmmni` 4096 |
| Other load | three root-owned Hadoop DataNode JVMs (about 6% of a core each, affinity 0-95); per-group CPU accounting separates them ("other (node 2)") |

## Software

- Neon fork `/mydata/jiyu/neon`, branch `local-uds`, base commit `4187e2da7` (branch `dev`) plus the uncommitted
  changes in [code/neon_fork.patch](code/neon_fork.patch) (also listed in `code/neon_fork_commit.txt`). The patch
  holds the Unix-socket support (see README "Neon fork changes") and one older, unrelated uncommitted change
  (`pagebench/src/util/request_stats.rs` reports p50).
- Build (about 5 minutes for a rebuild, 30 for a clean one; protoc 25.3 in `~/.local/protoc`):
  ```bash
  cd /mydata/jiyu/neon && git checkout local-uds && git apply <db-fork>/experiment/10-neon-local-uds/code/neon_fork.patch  # if not applied
  export PROTOC=$HOME/.local/protoc/bin/protoc PATH=$HOME/.local/protoc/bin:$PATH
  BUILD_TYPE=release CARGO_BUILD_FLAGS="--features=testing" make -j48 -s
  cargo test --release -p utils listen_addr && cargo test --release -p postgres_connection -p uds_http
  ```
- The binaries actually run are a snapshot in `/mydata/jiyu/neon-exp10/bin` (`neon_services.sh install`); their
  hashes are in [code/bin_sha256.txt](code/bin_sha256.txt), and `neon_services.sh status` re-checks them.
- This repo: base commit in [code/base_commit.txt](code/base_commit.txt) plus
  [code/shared_code.patch](code/shared_code.patch) (`git diff` of shared code, untracked test included; checked with
  `git apply --check`). The experiment's own shared change is `dblib/neon_local.py` (HTTP over Unix sockets) and
  its tests in `tests/test_neon_local.py`; the rest of the patch is experiment 5's uncommitted code, which this
  experiment also uses (compute options, prewarm in background clients).
- Python 3.13 venv via `uv`; Postgres 17.5 (Neon's), Neon's `pgbench` and `psql` from the snapshot.

## Parameters

| | |
|---|---|
| Services | 1 pageserver, 1 safekeeper (WAL backup to `local_path`), storage broker, storage controller + its Postgres, compute-notification stub, compute agent |
| Transport | `uds` (default): every listener a Unix socket in `/mydata/jiyu/neon-exp10/run`; `tcp`: the same on 127.0.0.1 |
| Pageserver | `max_file_descriptors = 65536`, `page_cache_size = 131072` (1 GiB; experiment 5: 8 GiB, see README), disk-usage eviction off, remote storage `local_path` |
| Dataset | CH-benCHmark W=4 generated into database `bench` (seed timeline; stock heap 133 MiB, `stock_pkey` 8.6 MiB, database 369 MiB) |
| Compute | Postgres 17 + neon extension via `compute_ctl`; `shared_buffers = 43MB` (30% of stock + stock_pkey); local file cache off; `effective_io_concurrency = 2`; `max_connections = 25`; `fsync = off` (durability is the safekeeper's) |
| Prewarm | `pg_prewarm('stock', 'buffer', 'main', 0, 4129)` then `pg_prewarm('stock_pkey')`, untimed, on every client's branch before its warm-up |
| A low load | one branch, one client: READ 40,000, RANGE_READ 10,000, INSERT 6,000, UPDATE 6,000; branch create 100 (every 5th connected) with one MIXED busy branch; statement logging on |
| B scaling | N = 1, 4, 16, 64, 128, 256, 512; ops per client: experiment 5's formula (totals 12,000 / 2,500 / 6,000 / 6,000, minimums 3,000 / 600 / 2,500 / 2,500, caps 1,000,000 reads / 640,000 writes); one client per branch, one statement in flight; up to 24 client processes; TCP arm: READ at N = 1, 16, 64, 256 |
| C branch create | N = 1, 4, 16, 64, 128, 256, 512 MIXED busy branches (80% point reads / 20% updates), 100 creates, every 5th connected |
| Ceiling | `ceiling_pgbench.py`: one pgbench process per branch (`-c 1 -j 1`), 30 s per point, same N as B, both transports; branches with computes are added so that exactly N exist at each point; before each measurement it waits until disk writes stay under 20 MB/s for 5 s; point, range and insert once, update three times (median reported) |
| Common runner flags | `--mode sync --warmup-fraction 0.1 --table-name stock --updates-per-branch 100 --seed-db bench --generated-scale 4 --setup-parallelism 64` |

## Commands, in order

```bash
cd ~/db-fork
X=experiment/10-neon-local-uds/scripts

# 0. once: keep the user's IPC objects when the last login session ends (sudo; Postgres shared memory and the
#    runner's semaphores would otherwise be deleted by logind)
sudo loginctl enable-linger jiyu

# 1. binaries (after building the fork) and services on Unix sockets (about 30 s)
$X/neon_services.sh install
$X/neon_services.sh start --transport uds
$X/neon_services.sh status        # every pid on node 2, "TCP listeners ... none", snapshot MATCH

# 2. seed the uds state tree (about 2 minutes), then a smoke test
numactl --cpunodebind=3 --membind=3 uv run python $X/neon_ctl.py seed
numactl --cpunodebind=3 --membind=3 uv run python $X/neon_ctl.py smoke

# 3. the TCP state tree needs its own seed (the storage controller keeps node addresses)
$X/neon_services.sh stop && $X/neon_services.sh start --transport tcp
numactl --cpunodebind=3 --membind=3 uv run python $X/neon_ctl.py seed
$X/neon_services.sh stop && $X/neon_services.sh start --transport uds && uv run python $X/neon_ctl.py env-init

# 4. smoke runs (each a few minutes; output in data/superseded/smoke_*)
numactl --cpunodebind=3 --membind=3 $X/run_low_load.sh --transport uds --smoke --ops READ,INSERT
numactl --cpunodebind=3 --membind=3 $X/run_scaling.sh --transport uds --smoke --ns 4,64 --ops READ,INSERT
numactl --cpunodebind=3 --membind=3 $X/run_branch_create.sh --transport uds --smoke --ns 4
numactl --cpunodebind=3 --membind=3 uv run python $X/ceiling_pgbench.py --smoke --ns 1,16,64 --kinds point,insert --seconds 10

# 5. everything (resumable; about 3 hours; switches transports itself and ends on uds)
setsid nohup numactl --cpunodebind=3 --membind=3 $X/run_all.sh > experiment/10-neon-local-uds/logs/run_all.log 2>&1 < /dev/null &

# 6. transport microbenchmarks and the NVMe probe, services idle (about 10 minutes). Run WITHOUT a numactl binding:
#    the scripts pin every process themselves (ipc_microbench.sh refuses to run when bound)
$X/ipc_microbench.sh
$X/disk_probe.sh

# 7. tables and figures
uv run python $X/analyze.py
```

`run_all.sh` steps and their measured durations are in `logs/run_all.log`. What was actually run, in order:
`run_all.sh` (25-26 Oct; its ceiling steps used the first version of `ceiling_pgbench.py`, now in
`data/superseded/`), `run_followup.sh` (ceiling with the disk-settle wait, update repetitions, IPC microbenchmarks;
its ceiling data is also superseded), `disk_probe.sh`, and `run_ceiling.sh` (the reported ceiling, both transports,
about 36 minutes each). With the current `ceiling_pgbench.py`, `run_all.sh` alone produces the reported data set.

## Pass checks

- Every line of every `progress.log` reads `DONE ... ok`: no failed operations, no failed background operations,
  the intended number of client processes, CPU labels present, node-2 available memory (free + file-backed page
  cache) above 4 GB. `CHECK-FAIL` keeps the data and names the problem.
- `neon_services.sh status`: Neon's pids have node 2's CPUs only, no TCP listener of a Neon process in uds mode.
- The shared-code md5 lines at the start and end of each driver match.
- `ceiling_pgbench.csv`: `failed` is 0 in every row.

## Cleanup

```bash
uv run python experiment/10-neon-local-uds/scripts/neon_ctl.py purge     # computes and branches
experiment/10-neon-local-uds/scripts/neon_services.sh stop
rm -rf /mydata/jiyu/neon-exp10                                          # everything outside the repo (a few GB)
```

## Known pitfalls

- **Lingering.** Without `loginctl enable-linger`, logind (RemoveIPC=yes) deletes the user's System V and POSIX
  shared memory when the last session ends; Postgres then fails and the runner's semaphores vanish.
- **Unix socket paths.** `sun_path` holds 108 bytes; the socket directory is kept short (`/mydata/jiyu/neon-exp10/run`,
  27 bytes). Paths must not contain `:` or `,` (the walproposer splits `neon.safekeepers` on them).
- **HTTP endpoints on Unix sockets** are written `http+unix://<hex of the socket path>/<path>` (`http::Uri` rejects
  percent escapes in the host); libpq endpoints use the socket directory (`host=<dir> port=<n>`, or
  `postgres://user@%2F<dir>:<n>`).
- **Storage controller database over a Unix socket** needed a fork change: its rustls connector rejects the empty
  server name of a Unix socket before sslmode is read.
- **NVMe multipath.** `/proc/diskstats` counts I/O on `nvme2c2n1`, not `nvme2n1`.
- **The benchmark runner is CPU-bound** at about 0.3 ms of client CPU per statement; with 24 client cores it limits
  writes near 20k/s. The pgbench ceiling exists for this reason.
- **Ceiling with too many computes.** Keeping all 512 computes running while measuring small N depresses write
  throughput through memory pressure (14.9k vs 19.4k updates/s at N = 128); measure each N with exactly N computes.
- **Writeback after starting many computes** disturbs the next measurement for tens of seconds; wait for the disk
  to settle (the ceiling script does).
- **Memory check.** Use free plus file-backed page cache (`mem_Available` in the procs CSV), not `MemFree`, which
  sits at the kernel watermark whenever the page cache is full.
- **Memory.** Each compute holds about 64 MB of shared memory (43 MB shared buffers) and about 17 MB private; 512
  computes fill node 2 to its watermark (page cache reclaimed). N = 1,024 does not fit and was not run.
- **The pageserver page cache is preallocated.** 8 GiB would sit on node 2 unused (GetPage data reads use direct
  I/O); 1 GiB gave the same throughput (`data/superseded/smoke_ceiling_uds_pc1g`).
- **Transport switch.** The storage controller rejects a node re-registering with other addresses, so each transport
  has its own state tree (`/mydata/jiyu/neon-exp10/{uds,tcp}`) and seed; run `neon_ctl.py env-init` after a switch.
- **Never write the repo's `.env`**: other experiments use it. This experiment's environment is
  `/mydata/jiyu/neon-exp10/env.exp10`.
