# Plan: Experiment 3, Neon branch scaling vs Dolt (busy branches)

## Context

Experiment 2 (`experiment/2-dolt-busy-branches/`) measured Dolt 2.4.2 and Doltgres 1.4.0 with N = 1..1,024 existing
branches, every branch busy: data operations (READ, RANGE_READ, INSERT, UPDATE; one client per branch, one request in
flight) and branch creation (100 creates while N clients run 80/20 reads/updates). Reads scaled until server CPU was
full (~40k reads/s), writes serialized at ~680/s, branch creation collapsed to ~2 s at N = 1,024.

The user wants the same hypothesis tested on self-hosted Neon on the same CloudLab `c220g2` cluster:

> Neon's branch ops and data ops latency will generally be higher than Dolt because Neon is distributed, but it will
> probably be more scalable, and eventually still be bottlenecked by the safekeeper and pageserver as the workload
> increases. Compare Neon and Dolt latency under low workload, then verify Neon's scalability bottleneck as N increases.

Decisions made with the user (19 Oct 2026):

| Question | Decision |
|---|---|
| Storage CPU budget | Follow the Neon experiments' layout: pageserver on its own node, **3 safekeepers** on 3 nodes (one each, as in `/mydata/jiyu/neon/experiments/5-safekeeperless-wal`). Whole nodes, no socket pinning. |
| Compute cache | Small: `shared_buffers=1MB` (what every earlier Neon experiment and `neon_local` use), local file cache off, so reads reach the pageserver. |
| Timed BRANCH_CREATE | The storage-controller timeline create (metadata op, comparable to Dolt's `DOLT_BRANCH`). Compute start until first successful connection recorded separately, plotted as a second series. |
| Dolt baseline | Experiment 2's numbers as they are (loopback client); the report states the LAN difference. No Dolt reruns. |
| Dolt nodes | May be removed if disk is tight. Disk is not tight (2.1 TB `/mydata` per node), so Dolt/Doltgres are only **stopped** (`systemctl disable --now dolt|doltgres`), not removed; node0 and node1 are reused. |

Client, computes and storage are on separate nodes (the user's preference for Neon).

## Facts established during exploration

- All 8 `c220g2` nodes of CloudLab experiment `branchdb` are up: node0..node7, private LAN `10.10.1.1..8` on
  `enp6s0f1`, `/etc/hosts` resolves `nodeX`. node0/node1 run idle Dolt/Doltgres. node2..node7 are untouched (`/dev/sdc`
  447 GB SSD unformatted, `/mydata` 2.1 TB). Ubuntu 22.04, glibc 2.35, 40 hardware threads, 157 GB RAM, no `sar`.
- This machine holds an x86_64 **release** build of Neon (`/mydata/jiyu/neon/target/release`: pageserver 408 MB,
  safekeeper, storage_controller, storage_broker, compute_ctl; built 2026-09-29 from commit `4187e2da7` on `dev`,
  features `testing`) and `pg_install/v17` (postgres, `neon.so`, `neon_walredo.so`, ...). Same glibc, ICU 70 and
  OpenSSL 3 as the nodes: **rsync, no rebuild**. Experiment 5's safekeeper-less WAL code is opt-in; a normal deployment
  is unaffected.
- The only bring-up code is `/mydata/jiyu/neon/experiments/4-branch-shape-scaling-cluster/scripts/{deploy.py,lib.py}`
  (hardcoded to dead c220g5 hosts) and experiment 5's `exp5.py` (multi-safekeeper launch, per-compute ports). Reusable
  from there: service command lines, `config/pageserver.toml.tmpl`, `config/compute_config_template.json`,
  `config/compute_hook_stub.py`, and the gotchas (register the pageserver with storcon via `POST /control/v1/node`;
  `safekeepers_generation: null`; `LD_LIBRARY_PATH` for compute_ctl; `--remote-storage` TOML quoting; storcon says
  `Active` before the pageserver answers `/v1/status`).
- compute_ctl appends `host all all all md5` to `pg_hba.conf`, and the template sets `skip_pg_catalog_updates`, so the
  benchmark role is created **once in the seed root** (inherited by every branch through the catalog) and the client
  node logs in over the LAN with a password. Earlier Neon experiments only connected from localhost.
- No earlier Neon experiment ran more than 16 computes per node or measured per-compute memory. Experiment 5: pgbench
  write transaction p50 2.25 ms with one SSD safekeeper and one compute.
- db-fork: the `neon` backend is the Neon **cloud** API; `--seed-db` / `_copy_from_seed_db` are Dolt-only;
  `db_setup/ch-w1.sql` does not exist (CH data comes from `macrobench/datagen/ch.py::seed_ch`); operation SQL is
  dialect-neutral pyformat, so READ/RANGE_READ/INSERT/UPDATE/MIXED run unchanged on Postgres. `_run_verb` times only
  `_branch_impl`; `exec()` records a timed CONNECT row around `_connect_impl` on a switch; `SetupPhase` does
  `branch → connect → 100 updates` serially per branch (`microbench/runner2.py:1021-1049`); the measured BRANCH_CREATE
  never connects to the new branch; `WorkerContext.__enter__` connects every worker and every background client.

## Topology (8 nodes, CloudLab experiment `branchdb`)

| Node | LAN IP | Role | Data |
|---|---|---|---|
| node2 | 10.10.1.3 | **pageserver** (device under test), http 9898 / pg 64000 | `/mnt/ssd/ps` (sdc, XFS); local_fs remote storage `/mydata/ps-remote` |
| node3 | 10.10.1.4 | **safekeeper 1** + storage_broker 50051 + storage_controller 1234 + its Postgres 1235 + compute-hook stub 9999 | `/mnt/ssd/sk` |
| node4 | 10.10.1.5 | **safekeeper 2** | `/mnt/ssd/sk` |
| node5 | 10.10.1.6 | **safekeeper 3** | `/mnt/ssd/sk` |
| node1, node6, node7 | 10.10.1.2/7/8 | **compute nodes**: compute agent :9000, up to ~342 computes each at N = 1,024 | `/mydata/computes/<name>/pgdata` |
| node0 | 10.10.1.1 | **benchmark client**: `~/db-fork`, runner2 with up to 40 processes | – |

- Pageserver: experiment 4's `pageserver.toml` template **without** the GC-disabling `[tenant_config]` block (defaults;
  the workload writes), `page_cache_size = 1048576`, `max_file_descriptors = 65536`, `LimitNOFILE` raised in the unit.
- Safekeepers: `--id 1..3 --availability-zone az1..az3 --listen-pg 0.0.0.0:5454 --advertise-pg <ip>:5454
  --listen-http 0.0.0.0:7676 --broker-endpoint http://10.10.1.4:50051 --remote-storage {local_path=...}`.
- storcon: `--dev --listen 0.0.0.0:1234 --database-url ... --control-plane-url http://10.10.1.4:9999`, **without**
  `--timelines-onto-safekeepers` (as in all earlier Neon experiments): computes list the three safekeepers in their
  spec, the walproposer creates the timeline on them at first connect, commits wait for a quorum of 2 of 3 fsyncs.
  Consequence stated in the report: Neon's timed "branch create" is the pageserver-side metadata create only.
- All services as **systemd units** (`neon-broker`, `neon-storcon-pg`, `neon-hook-stub`, `neon-storcon`,
  `neon-safekeeper`, `neon-pageserver`, `neon-compute-agent`) for restarts, `MainPID` and `journalctl`.
- Computes: Postgres 17, spec from the template with `shared_buffers=1MB`, no LFC, `max_connections=100`,
  `synchronous_commit` default, `listen_addresses='0.0.0.0'`, fixed per-compute ports (pg `20000+i`, http
  `30000+2i`/`30001+2i`; below the ephemeral range 32768+), `suspend_timeout_seconds=-1`. The seed compute (used once)
  gets `shared_buffers=2GB`.

## Work

### Phase A: experiment directory and cluster bring-up (`experiment/3-neon-branch-scaling/`)

Layout per `.agent/CLAUDE.md`: `README.md`, `plan.md` (this plan), `REPRODUCE.md`, `report.md`, `figures/`,
`data/neon_local/{data_ops,branch_create}`, `data/superseded/`, `logs/`, `scripts/`, `code/`, plus `config/`
(pageserver.toml template, compute spec template, hook stub, unit files). Scripts resolve the repo root from their own
location and write to the experiment's `data/` by default.

1. `scripts/cluster.env`: roles, IPs, ports, paths, tenant id and seed root timeline id (filled by `seed`); sourced by
   the shell scripts, parsed by the Python ones.
2. `scripts/setup_neon_cluster.sh` (idempotent; modelled on `experiment/1-dolt-branch-scaling/scripts/setup_cloudlab_dolt.sh`):
   per node `apt-get install xfsprogs sysstat numactl`, format/mount `/dev/sdc` at `/mnt/ssd` only if blank, `chown
   /mydata`, rsync the five Neon binaries and `pg_install/v17` from `/mydata/jiyu/neon` to `/mydata/neon-bin/` (first
   copy ≈ 1 GB per node), write configs and units, start in order broker → storcon Postgres (`initdb`, `createdb
   storage_controller`) → hook stub → storcon → safekeepers → pageserver → `POST /control/v1/node` and wait for
   `Active` plus pageserver `/v1/status`; on node0 stop Dolt, rsync this repo to `~/db-fork` (excluding
   `experiment/*/data`, `.venv`, `.git`), `uv sync --group dev`, `build_protos.py`, write `.env`
   (`NEON_LOCAL_*`, `DBFORK_REMOTE_CPU`, ssh `ControlMaster` config for the CPU sampling); on node1 stop Doltgres.
   `--status` prints every unit, `/v1/status`, timeline count, computes per agent.
3. `scripts/neon_compute_agent.py` (stdlib only; systemd unit on each compute node, `LimitNOFILE`/`LimitNPROC`
   raised): `POST /computes {name, timeline_id, shared_buffers?}` → allocate the port triple from the name's slot,
   render `config.json`, launch `compute_ctl` detached with `LD_LIBRARY_PATH`, wait for `pg_isready` and `SELECT 1` as
   `bench`, return `{host, port, start_ms}` (concurrent starts, bounded to 32 at a time); `GET /computes` (name →
   timeline_id, host, port); `DELETE /computes/{name}` (anchored `pkill -f '[c]ompute-id <name> '`, remove pgdata);
   `DELETE /computes` (all); `POST /computes/{name}/admin-sql` (runs SQL as `cloud_admin` via local psql; used only by
   `seed`); `GET /stats` (count, RSS sum, node CPU).
4. `scripts/neon_ctl.py`: `status`, `seed` (create tenant and root timeline through storcon; start compute `seed` on it
   with 2 GB shared_buffers; `CREATE ROLE bench LOGIN PASSWORD ...; CREATE DATABASE bench OWNER bench` via admin-sql;
   call db-fork's `seed_generated_database(NEON_LOCAL, ...)` with CH W=1, FKs skipped; stop the compute; write ids
   into `cluster.env`), `purge` (stop every compute, delete every timeline except the seed root, deepest first, polling
   each DELETE until 404), `timelines`, `metrics-snapshot` (pageserver and safekeeper `/metrics` to a file).
5. `scripts/run_neon_busy_branches.sh`: experiment 2's driver for backend `neon_local`: same N grid 1..1,024, same ops
   per client (`READ max(50, 40000/N)`, `RANGE_READ max(10, 10000/N)`, writes `max(10, 6000/N)`), same flags
   (`--mode sync --warmup-fraction 0.1 --table-name stock --updates-per-branch 100 --seed-db bench`),
   `--processes min(N, 40)`, `--setup-parallelism 32`; branch creation with `--background-op MIXED
   --branch-create-ops 100 --branch-create-connect-every 5`. Every run is wrapped in `timeout`; `neon_ctl.py purge`
   runs before each run; `/metrics` snapshots before and after each run; `sar -u -d 1` on storage and compute nodes
   for the whole study into `logs/sar_<node>.txt`. `--smoke` = N 1,4,64, READ+UPDATE, 10 creates, into
   `data/superseded/smoke_neon_local`.
6. `scripts/plot_neon_vs_dolt.py`: loads experiment 2's `data/dolt_mysql` and `data/dolt` with its loader logic
   (`plot_busy_branches.py`) plus `data/neon_local`; writes `figures/data_ops.png` (p50/p95 latency and total
   throughput vs N, three systems), `figures/branch_create.png` (timeline-create latency and creates/s vs N for all
   three, Neon compute start from the CONNECT rows as a second series; Neon creates/s computed as creates / Σ BRANCH
   latency because the window also holds the 20 compute starts), `figures/low_load.png` (N = 1 and 4 per-op median
   latency bars), `figures/neon_bottleneck.png` (pageserver, safekeeper, compute-node and client CPU cores vs N per
   operation from `remote_cpu`, safekeeper disk utilisation from `sar`), `figures/summary.csv|md`.
7. `scripts/branch_activity.py`: experiment 2's Little's-law check, pointed at this experiment's data.

### Phase B: shared code (listed in the experiment README, saved as `code/shared_code.patch` against `24ab525`)

1. **`dblib/neon_local.py`**: `NeonLocalToolSuite(DBToolSuite)` (no `PgMergeMixin`; merge/diff stay unsupported),
   `SUPPORTS_COMMIT_REFS=False`, `SCHEMA_DIALECT="postgres"`. Must not read env or touch the network at import time
   (runner2 imports every adapter; tests import runner2 without a cluster).
   - Env: `NEON_LOCAL_STORCON_URL`, `NEON_LOCAL_PAGESERVER_URL`, `NEON_LOCAL_TENANT_ID`,
     `NEON_LOCAL_ROOT_TIMELINE_ID`, `NEON_LOCAL_AGENTS`, `NEON_LOCAL_PG_USER/PASSWORD`, `NEON_LOCAL_DB` (`bench`).
   - **Deterministic timeline ids**: `timeline_id = md5(f"{db_name}/{branch}")` (32 hex = one timeline id); `main`
     maps to `md5(db_name)` and compute name `db_name`. Any process resolves a name without a registry; a compute is
     found by `GET /computes` on every agent (cached per process, refreshed on a miss); new computes go to the agent
     with the fewest computes.
   - `_branch_impl(name, from_ref)`: only `POST {storcon}/v1/tenant/{tid}/timeline {new_timeline_id,
     ancestor_timeline_id}` (the BRANCH row). The compute starts **lazily in `_connect_impl`** on first connection, so
     the CONNECT row carries compute start + first connection (setup parquet gives both series for every setup
     branch; `connect_every` gives them for measured creates).
   - **Lazy connection** (review finding 21): `__init__` sets `_current_ref = Ref(default_branch)` with `conn=None`;
     override `_connect` to force `_connect_impl` when `conn` is None/closed, and `get_current_connection`/`_execute`
     to connect on demand. Each worker then opens exactly one connection, to its own branch's compute.
   - `_connect_impl(ref)`: name → timeline id → compute (start if absent) → `psycopg2.connect(host, port,
     dbname=bench, user=bench, password=...)`, autocommit.
   - `_commit_impl`: `SELECT pg_current_wal_flush_lsn()::text` as the commit id (only the seed path commits).
     `_delete_impl`: agent `DELETE /computes/{name}`, storcon timeline DELETE. `_storage_bytes` returns 0.
   - Module helpers: `setup_run(db_name)` (purge leftovers of a previous run with the same name via the pageserver
     timeline list, create the run-root timeline from the seed root, start its compute), `teardown_run(db_name)`
     (stop all computes on all agents; delete every non-seed timeline deepest-first, 16 in parallel, poll until 404),
     `purge_all()`.
2. **`microbench/runner2.py`**: `NEON_LOCAL` in `create_db_tools`; `BackendManager.setup()` branch
   (`default_branch_name="main"`, empty `default_uri`, call `neon_local.setup_run(db_name)` instead of
   `_create_database`); `cleanup()` calls `teardown_run` (runs in `main()`'s `finally`, so crashes clean up);
   `_validate` allows `seed_db` for NEON_LOCAL; `seed_generated_database` returns early for NEON_LOCAL with `seed_db`
   set (data is inherited from the seed root; no schema re-apply); `ensure_seed_database`/`_seed_table_ref` unchanged.
   New `SetupConfig.setup_parallelism` (default 1 = current serial behaviour): `SetupPhase` creates branches and runs
   their untimed data ops on a thread pool of that size with one suite per thread, rows still go to `_setup.parquet`
   (otherwise N = 1,024 setup costs ~1 h per run with a ~2 s compute start per branch). New
   `BranchOpConfig.connect_every`: `BranchCreateOperation.execute` connects to every k-th created branch
   (`context.connect(name)`, timed CONNECT row) and switches back to the creator's branch untimed; default 0 leaves
   Dolt runs unchanged. **Remote CPU**: `DBFORK_REMOTE_CPU="pageserver=user@host:pid,sk1=...,node6=user@host:0"`
   sampled at window start/end (process utime+stime and node busy jiffies; `pid 0` = node only) by parallel ssh over
   pre-opened `ControlMaster` sockets, in both `TimedPhase` and `ProcessTimedPhase` (extend the shared `times` array
   only when the variable is set so existing tests keep passing), written to the summary as
   `remote_cpu: {label: {proc_cores, node_cores}}`; `server_cpu_cores` falls back to the pageserver's.
3. **Enum and scripts**: `NEON_LOCAL = 13` in `microbench/task2.proto` and `macrobench/task.proto` (parity), then
   `build_protos.py`; `scripts/run_throughput_bench.sh`: backend regex (line 268), `--seed-db` check (292),
   `--setup-parallelism`, `--branch-create-connect-every`; `run_single_thread_bench.sh` regex for completeness.
   `macrobench/runner.py` maps use `.get`, no change needed for a micro run.
4. **Tests**: `tests/test_micro_runner2.py` additions on the fake backend (`connect_every` records CONNECT rows and
   keeps forking from the original branch; `setup_parallelism > 1` produces the same branches/rows as serial;
   `remote_cpu_seconds` parsing with a stubbed ssh); unit test for the agent's config rendering and port allocation;
   `tests/test_neon_local_live.py` (self-skips unless `NEON_LOCAL_STORCON_URL` answers): branch, connect, read, write,
   delete round trip. Expected pass set unchanged except the pre-existing `test_db_api.py::test_branch_commit_log_diff_reset_delete`.

### Phase C: run

1. `setup_neon_cluster.sh` (≈15-20 min), `neon_ctl.py seed` (608k rows through one compute; minutes).
2. Live test on node0, then `--smoke` (N = 1, 4, 64). Pass: every `progress.log` line `DONE`, `failed_ops == 0`,
   `num_processes == min(N, 40)`, `background.failed == 0`, `GET /computes` shows N + 1 computes during a run and 0
   after, timeline count back to 1 after purge, N = 1 latencies plausible (point read ≈ 1-4 ms, write ≈ 2-5 ms with
   quorum fsync), memory per compute from `/stats` at N = 64 (expected 30-60 MB, so 1,024 fit on three nodes).
   Capacity check at N = 256 before scheduling 512 and 1,024 (review S1): pageserver/safekeeper connection counts,
   compute-node RSS, start storms.
3. Full sweep detached on node0 (`setsid nohup ... run_neon_busy_branches.sh neon_local`), monitored from here;
   data ops and branch creation for N = 1..1,024. Expected 3-5 h with parallel setup. If Neon stops answering at some
   N (earlier Neon experiments saw starvation at 256 timelines under saturating load), the `timeout` ends the run, the
   grid stops there, and the failure mode is reported as a result.
4. If `client_cpu_cores` approaches 40 while storage CPUs are low on read points, add an asyncpg version of
   experiment 2's `read_ceiling_check.py` (hollow markers as in experiment 2).
5. Pull data and logs back (`rsync`), plot, write `report.md`, `README.md`, `REPRODUCE.md`; save
   `code/shared_code.patch` (`git diff 24ab525` with untracked files, `git apply --check`); update `.agent/cloudlab.md`
   (new node roles) and the memory file.

## Report structure (`report.md`, Markdown with embedded figures)

1. Verdict on the hypothesis: low-load latency Neon vs Dolt; how Neon scales with N per operation; at which N and in
   which component (pageserver CPU, safekeeper CPU/disk, compute nodes, client) Neon saturates.
2. Setup: topology table, versions (Neon `4187e2da7`, Postgres 17; Dolt 2.4.2, Doltgres 1.4.0), compute config,
   differences from experiment 2 (LAN hop, 3 fsyncing safekeepers, whole-node storage services, a compute per branch,
   timeline create excludes safekeepers).
3. Low workload (N = 1, 4): table and `low_load.png`.
4. Data operations vs N (`data_ops.png`; table at N = 1 / 16 / 1,024).
5. Branch creation vs N (`branch_create.png`: timeline create vs Dolt; compute start; background ops/s).
6. Where Neon's time goes as N grows (`neon_bottleneck.png`; pageserver/safekeeper `/metrics` deltas if informative).
7. Limits: client, one data size and table, single pageserver shard, local_fs remote storage, no GC pressure, storcon
   not managing safekeeper timelines.

## Verification

- `uv run pytest tests/test_micro_runner2.py tests/test_db_api.py -q` locally and on node0.
- `uv run pytest tests/test_neon_local_live.py` on node0 against the cluster.
- Smoke criteria above; full run: 55 `DONE` lines, `failed == 0` in `summary.csv`, `background.failed == 0`,
  `branch_activity.py` ≈ N requests in flight for writes and background load at N = 1,024, `remote_cpu` present in
  every summary.
- Figures regenerate with one command; report numbers come from `figures/summary.csv`.

## Risks and fallbacks

- **1,024 computes**: memory fine on paper; process/fd limits raised in the agent unit; fixed ports avoid the
  ephemeral-port exhaustion experiment 4 hit; starts bounded to 32 concurrent per node; capacity checked at 256 first.
- **Timeline deletion** is asynchronous and refuses parents with children: deepest-first, polled, parallel, purge before
  every run.
- **Client limit** on reads at high N: hollow markers and the asyncpg check if needed.
- **Warm pageserver**: branches share the root's layers, so pageserver reads are warm after warm-up while compute
  caches are cold by design; stated in the report.
