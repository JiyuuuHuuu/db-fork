# Reproducing experiment 3

Everything needed to rerun this experiment: machines, Neon build and deployment, code version, parameters, the
commands in order with durations, pass checks, collection and plotting, cleanup and pitfalls. Commands run from the
repo root on the coordinator (a machine with the Neon build and ssh access to the nodes) unless stated otherwise.
`X=experiment/3-neon-branch-scaling`.

## 1. What you need

| Item | Value used on 19 Oct 2026 |
|---|---|
| Machines | 8 CloudLab **c220g2** nodes (Wisconsin), Ubuntu 22.04, glibc 2.35; experiment `branchdb`, project `rasl-PG0`, user `JiyuHu23`. Host names and roles in `scripts/cluster.env`. |
| Node hardware | 2x Xeon E5-2660 v3 (20 hardware threads per socket, 40 per node), 157 GB RAM, unused 447 GB Intel S3500 SSD at `/dev/sdc`, root and `/mydata` on HDD, 10 GbE private LAN `10.10.1.0/24` (round trip 0.12-0.15 ms). Specs: `.agent/cloudlab.md`. |
| Coordinator | x86_64 Ubuntu 22.04 with a Neon release build in `/mydata/jiyu/neon` and ssh key `~/.ssh/dassl_rsa` for the nodes. Nodes cannot ssh to each other; everything goes through the coordinator. |
| Neon | Fork at `/mydata/jiyu/neon`, commit `4187e2da7` (branch `dev`), `BUILD_TYPE=release CARGO_BUILD_FLAGS=--features=testing make`; Postgres 17.5 from its `pg_install/v17`. No rebuild on the nodes: the binaries are copied. |
| db-fork code | Base commit `24ab525` plus [`code/shared_code.patch`](code/shared_code.patch) (experiments 2 and 3's shared-code changes, uncommitted at the time). |

```bash
git checkout 24ab525 && git apply $X/code/shared_code.patch
uv sync --group dev && uv run python build_protos.py
uv run pytest tests/test_micro_runner2.py tests/test_db_api.py -q   # all pass except the pre-existing
                                                                      # test_db_api.py::test_branch_commit_log_diff_reset_delete
```

## 2. Topology

| Node | Role |
|---|---|
| node0 (10.10.1.1) | benchmark client: `~/db-fork`, runner2 with up to 40 processes |
| node1, node6, node7 | compute nodes: `neon-agent` starts one Neon compute (compute_ctl + Postgres 17) per branch, data in `/mnt/ssd/computes` |
| node2 | pageserver (`/mnt/ssd/ps` on the SSD; "remote storage" `local_fs` at `/mydata/ps-remote` on the HDD) |
| node3 | safekeeper 1, storage_broker, storage_controller (`--dev`) with its own Postgres, compute-hook stub |
| node4, node5 | safekeepers 2 and 3 |

Every node except node0 runs `neon-agent` (`scripts/neon_node_agent.py`, port 9000), which reports CPU and SSD
counters to the client and, on compute nodes, starts and stops computes.

## 3. Deploy the cluster (about 15 minutes, mostly copying binaries)

```bash
$X/scripts/setup_neon_cluster.sh              # idempotent; --skip-binaries to only redo configs/units
$X/scripts/setup_neon_cluster.sh --status     # every unit active where it should be, pageserver Active
```

What it does:

1. Copies `pageserver`, `safekeeper`, `storage_controller`, `storage_broker`, `storcon_cli`, `compute_ctl` and
   `pg_install/v17` to `/mydata/neon-bin` on the nodes that need them.
2. Formats `/dev/sdc` as XFS if blank and mounts it at `/mnt/ssd`; sets `RemoveIPC=no` in `/etc/systemd/logind.conf`.
3. Writes systemd units and their wrapper scripts in `/mydata/neon-svc`: `neon-broker`, `neon-storcon-pg` (with
   `shared_preload_libraries=neon_rmgr`), `neon-hook`, `neon-storcon`, `neon-safekeeper` (ids 1-3, AZs az1-az3,
   `--remote-storage={local_path="/mydata/sk-remote"}`), `neon-pageserver` (`pageserver.toml`: default tenant config,
   `page_cache_size = 1048576`, `max_file_descriptors = 65536`, default I/O mode = direct I/O), `neon-agent`.
4. Starts them in order and registers the pageserver with the storage controller (`POST /control/v1/node`).
5. Stops Dolt on node0 and Doltgres on node1 (left from experiments 1-2), copies the repo to node0.

`--restart` wipes every Neon data directory (tenant, timelines, safekeeper WAL, storcon database) first.

Compute configuration (`config/compute_config_template.json`, rendered by the agent): `shared_buffers=1MB`, no local
file cache, `max_connections=100`, `fsync=off` (the compute's local files are scratch), `synchronous_standby_names=walproposer`
(commit waits for 2 of 3 safekeepers), `max_replication_write_lag=15MB`, `safekeepers_generation: null`,
ports `20000+slot` (pg), `22000+2*slot` (http). Role `bench` / database `bench` are created in the seed timeline and
inherited by every branch; clients log in over the LAN with a password (compute_ctl appends `host all all all md5`).

## 4. Environment and seed (about 2 minutes)

On node0:

```bash
cd ~/db-fork
uv run python $X/scripts/neon_ctl.py env-init   # NEON_LOCAL_* and DBFORK_REMOTE_CPU into .env
uv run python $X/scripts/neon_ctl.py seed       # tenant + seed timeline, CH-benCHmark W=1 (608k rows) into "bench"
uv run python $X/scripts/neon_ctl.py smoke      # branch, compute start, read, write, delete; ends with 1 timeline
```

`seed` loads through a compute with 2 GB of shared_buffers (58 s), runs `VACUUM ANALYZE`, waits until the pageserver
has ingested the WAL, stops the compute, compacts the seed timeline into image layers, writes a little more WAL so
the timeline's last record lies above the images, and writes the tenant and seed timeline ids into `.env` and
`data/neon_local/seed.json`. The last two steps are also `neon_ctl.py compact-seed` (the original run applied them
to an already loaded seed this way). Without them every read replays the page's history since the bulk load (see
pitfalls).

## 5. Smoke test (about 4 minutes)

```bash
ssh <node0> "cd db-fork && (setsid nohup $X/scripts/run_neon_busy_branches.sh --smoke < /dev/null > $X/logs/driver_smoke.out 2>&1 &)"
```

N = 1, 4, 64 with READ and UPDATE, branch creation with 10 creates; output in `data/superseded/smoke_neon_local`.
Pass: every `progress.log` line `DONE`; `scripts/summarize.py <dir>` shows `fail=0`, `bgfail=0`, and CPU columns for
the pageserver, three safekeepers and three compute nodes.

## 6. Full run

```bash
ssh <node0> "cd db-fork && (setsid nohup $X/scripts/run_neon_busy_branches.sh < /dev/null > $X/logs/driver.out 2>&1 &)"
ssh <node0> "tail -3 db-fork/$X/data/neon_local/progress.log"
```

### Parameters (all set by `scripts/run_neon_busy_branches.sh`; same as experiment 2 unless noted)

| Parameter | Value |
|---|---|
| N | 1, 2, 4, ..., 1024; a fresh run root (timeline branched from the seed) per run |
| Branches | FAN_OUT from the run root; each gets 100 untimed single-row UPDATEs on `stock` right after creation |
| Setup | 32 branches set up at a time (`--setup-parallelism 32`; experiment 2 set up one at a time) |
| Data-op runs | N clients, client *i* on branch *i*, one request in flight (sync); READ, RANGE_READ (100 rows), INSERT, UPDATE |
| Ops per client | READ max(3000, ⌈40000/N⌉), RANGE_READ max(600, ⌈10000/N⌉), INSERT/UPDATE max(2500, ⌈6000/N⌉), 10% extra untimed warm-up. Experiment 2 used minimums of 50/10/10; with them Neon's windows from N = 16 up lasted under a second and measured client start-up skew. The N = 1, 2, 4 points ran with the old minimums (identical counts for READ and RANGE_READ; writes 6,000/N per client, windows ≥ 6 s). |
| Branch-creation runs | 1 measured client creates 100 branches from its busy branch; every 5th new branch is also connected to (timed CONNECT row = compute start + first connection), then the client returns untimed; N background clients run MIXED (80% point READ / 20% point UPDATE) |
| Client processes | min(N, 40) for data ops, min(N + 1, 40) for branch creation (node0's 40 hardware threads; experiment 2 had 20) |
| Timed branch create | `POST /v1/tenant/<t>/timeline` on the storage controller with the parent as ancestor |
| Remote CPU/SSD | `DBFORK_REMOTE_CPU`: the client samples every agent once a second and interpolates at the window's ends |
| Per-run timeout | 5,400 s (`timeout -s INT`, then a purge) |

Each run is one call of `scripts/run_throughput_bench.sh neon_local ...` logged in `data/neon_local/logs/<run>.log`;
pageserver and safekeeper `/metrics` are saved before and after each run in `data/neon_local/metrics/`.

The original run was launched in two parts: the full script (all N; its N ≥ 8 data-op points were then moved to
`data/superseded/short_windows`), then, with the new minimums,

```bash
scripts/run_neon_busy_branches.sh --phases data_ops --ns 8,16,32,64,128,256,512,1024
scripts/run_neon_busy_branches.sh --phases branch_create --ns 512,1024
```

Three runs were repeated into `data/neon_local_reruns` (absolute `--outroot`; a relative one resolves against the
repo root), after freeing the compute nodes' shared-memory ids:

```bash
X=$HOME/db-fork/experiment/3-neon-branch-scaling; R=$X/data/neon_local_reruns
$X/scripts/run_neon_busy_branches.sh --outroot $R --phases data_ops --ns 32 --ops READ
$X/scripts/run_neon_busy_branches.sh --outroot $R --phases data_ops --ns 64 --ops UPDATE
$X/scripts/run_neon_busy_branches.sh --outroot $R --phases branch_create --ns 1024
```

The whole original run took about 2.5 hours (data operations about 2 hours, mostly N = 512 and 1,024).

### Branch creation without the storage controller's rate limit (about 20 minutes)

The storage controller limits each tenant to 10 API requests/s by default, which delayed the first runs' creates
from N = 128 on. The plotted branch-creation series reran with the limit lifted:

```bash
ssh <node3> "sed -i 's#--control-plane-url#--tenant-rate-limit 1000 --control-plane-url#' /mydata/neon-svc/neon-storcon.sh \
    && sudo systemctl restart neon-storcon"     # the original wrapper is kept as neon-storcon.sh.default
X=$HOME/db-fork/experiment/3-neon-branch-scaling
$X/scripts/run_neon_busy_branches.sh --outroot $X/data/neon_local_nolimit --phases branch_create
```

Check: `journalctl -u neon-storcon | grep -c "is rate limited"` stays 0 during the run.

### Minimal-client ceiling check (about 10 minutes)

```bash
ssh <node0> "cd db-fork && uv run python $X/scripts/ceiling_check.py"   # -> data/neon_local/ceiling_check.csv
```

N = 16, 64, 256 busy branches (each with a compute), point reads, range reads, inserts and updates for 20 s each
from a minimal asyncio client on 40 processes; remote CPU and SSD from the agents.

## 7. Collect and plot

```bash
rsync -az <node0>:db-fork/$X/data/ $X/data/
rsync -az <node0>:db-fork/$X/logs/ $X/logs/
uv run python $X/scripts/plot_neon_vs_dolt.py      # figures/*.png, figures/summary.csv|md
python3 $X/scripts/summarize.py $X/data/neon_local  # one line per run
uv run python $X/scripts/branch_activity.py         # figures/activity.csv (requests in flight)
python3 $X/scripts/internal_latency.py              # figures/internal_latency.csv (GetPage, WAL flush)
```

The plot script reads experiment 2's `data/dolt_mysql` and `data/dolt` for the Dolt and Doltgres series.

## 8. Clean up

```bash
ssh <node0> "cd db-fork && uv run python $X/scripts/neon_ctl.py purge"   # leaves only the seed timeline
for n in 1 2 3 4 5 6 7; do ssh <node$n> 'sudo systemctl disable --now neon-agent neon-pageserver neon-safekeeper neon-storcon neon-hook neon-storcon-pg neon-broker 2>/dev/null'; done
```

Restart Dolt on node0 / Doltgres on node1 with `sudo systemctl enable --now dolt|doltgres` if experiments 1-2 are rerun.

## Known pitfalls

- **logind `RemoveIPC`**: by default systemd-logind deletes a user's POSIX shared memory when their last ssh session
  ends, which crashes every Postgres run as that user (storcon's database, every compute). The setup script sets
  `RemoveIPC=no`.
- **Neon's Postgres needs `neon_rmgr` to replay its own WAL**: the storage controller's database failed recovery
  after a crash until `shared_preload_libraries=neon_rmgr` was added.
- **Timeline DELETE through the storage controller returns 200 even when the timeline is already gone**; poll the
  pageserver's `GET /v1/tenant/<t>/timeline/<id>` for 404 instead (done in `dblib/neon_local.py`).
- Arguments with spaces passed through `ssh host bash -s -- args` are re-split by the remote shell.
- `pkill -f <pattern>` over ssh can match the calling shell; use `[x]yz` patterns and keep them out of the same command line.
- The pageserver reads layer files with direct I/O by default (`virtual_file_io_mode` defaults to `DirectRw`), so
  GetPage requests hit the SSD; SSD "% busy" (io_ticks) is near 100% at low load and is not a saturation signal.
- **A bulk-loaded timeline has no image layers.** Neon creates images only after several delta layers pile up over a
  key range; one load of this size leaves a single delta layer, so every GetPage replays all WAL records of the page
  since `initdb` (about 131 per page here) in the single tenant's WAL-redo. Forcing image creation
  (`PUT .../compact?force_image_layer_creation=true`) is not enough on its own: the images are written at
  `last_record_lsn + 1`, and a branch made at `last_record_lsn` reads below them. Write some WAL after compacting.
- **SysV shared-memory ids run out.** Each Postgres keeps a small SysV segment; a compute killed before it shuts
  down leaves it behind, and after a few thousand computes `kernel.shmmni` (4,096) is exhausted and no compute starts
  ("could not create shared memory segment: No space left on device"). The setup script raises the limit to 32,768
  and the agent removes unattached segments when it stops computes. To clean by hand:
  `ipcs -m | awk '$3=="JiyuHu23" && $6==0 {print $2}' | xargs -n1 ipcrm -m`.
- **Pageserver read errors.** Under concurrent writes this build occasionally fails a page read from a timeline's
  in-memory layer (`dio vec read failed` in `journalctl -u neon-pageserver`). The statement fails; if it is a
  client's start-up key scan, that client fails its whole run. Count them per run with
  `grep -c FAILED`/`failed_ops` and rerun such points.
- **The benchmark client limits fast operations.** Range reads, inserts and the background load of branch-creation
  runs hit the Python client's limit from 32-128 branches on; compare with `ceiling_check.py` and
  `branch_activity.py` before reading those points.
