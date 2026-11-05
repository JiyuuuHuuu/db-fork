# Experiment 13: shared-nothing branching by copying Postgres data directories to other nodes (`pgcopy`)

## Context

The repo already benchmarks "branch = copy of the database files" in one form: `dblib/file_copy.py` (PostgreSQL 18,
`CREATE DATABASE ... STRATEGY = FILE_COPY` with reflink on one XFS volume). Experiment 9 looked at Xata, whose
branches are ZFS snapshot clones plus a new Postgres pod on the same node (smoke test only, never benchmarked). Neither
scales a branch to *another* node.

This experiment adds the naive shared-nothing baseline: a branch is a fresh PostgreSQL instance on some cluster node
whose data directory was produced by copying the parent's data directory over the network (`pg_basebackup` from the
parent's live instance). It puts a floor under the systems compared so far (Doltgres on 10 cores, Neon local on 10
cores, Neon cross-zone TCP/RDMA in experiment 12): what branching costs when it is just "copy the files to a node and
start Postgres", and how the same four workloads behave on it.

Decisions taken with the user (5 Nov 2026):
- **Fork = copy + start, ready to connect.** `branch()` returns when the new instance accepts connections; the copy and
  the start phases are recorded separately (`PGCOPY_START_ON_BRANCH=0` gives the Neon-like reading: copy only, start
  on first connect, for a later variant). Cold start = fork + first connect + reads, as for Neon.
- **Clients are co-located** with their branch's instance (loopback), like experiment 7's local Doltgres: only the fork
  crosses the network. Branches on one node share it; every branch is forked from the run root on a dedicated seed
  node, so every fork is a network copy.
- **Per-instance cache**: `shared_buffers = 40MB` (≈ Neon's 36 MB LFC + 1 MB shared_buffers at W=3). The OS page cache
  is shared by co-located instances and is warm right after a copy; the report says so.
- **Unpinned CPUs** on the host nodes (client CPU from the runner's rusage, the node's `postgres` CPU from the CPU agent).
- **Busy branches spread over all host nodes** for scaling_data, scaling_branch and the ceiling check (multi-node
  driver); low contention and cold start run on one host. Note: in a shared-nothing system busy branches on other
  nodes cannot slow a create on node 1, so the spread curve is expected flat; that is the finding. A cheap single-node
  variant (`--single-node`, N ≤ 128 on one host) shows the contended case and is run as a secondary series.
- **Cluster**: the user re-instantiates CloudLab as one LAN (no zones, no netem) and runs the coordinator on a node
  outside the cluster. All 16 cluster nodes are available: 1 seed/root node + 15 hosts (the host list is a topology
  parameter; cut it to 7 to match experiment 12's per-node compute density if wanted).
- Postgres 17 (PGDG packages), the same major version as Neon's computes.
- After approval, export this plan as `experiment/13-pgcopy-nodes/plan.md`.

Out of scope: Neon-only phases of exp 12 (2 pageservers / 2 safekeeper sets, RDMA), commit refs, merge/diff/rebase
(unsupported, as for `neon_local`). Optional follow-ups, not in the main run: a larger seed (`seed --scale 30`,
~3 GB) for create/cold-start only, to give the copy-size axis; a copy-with-fsync variant (no `--no-sync`).

## Validity of the experiment (checked 5 Nov on the live cluster)

| Concern | Finding |
|---|---|
| Disk | Seed `bench` DB = 296 MB at W=3 (`experiment/12-.../logs/w3_seed.log`); a copied instance ≈ 0.3 GB + 1–2 WAL segments (16 MB) ≈ 0.35 GB. N = 1024 → ≈ 360 GB total; over 15 hosts ≈ 24 GB/host, over 7 ≈ 52 GB/host; `/mnt/ssd` has ≈ 350 GB free on every node. Run roots on the seed node: one per runner process per point (≈ 0.35 GB each). The agent refuses a copy below `--disk-guard-gb 20`. |
| Memory | ≤ 150 instances/host (7 hosts, N=1024): with 40 MB buffers and `max_connections=20`, ≈ 60–80 MB PSS per instance ≈ 9–12 GB of 62 GB; page cache holds the rest. `mem_guard_gb` stays 4. |
| IPC / limits | ≈ 7 processes per instance → ≈ 1,000 per host. One SysV segment per postmaster → `kernel.shmmni=32768`; POSIX DSM segments in `/dev/shm` are swept at agent start and on purge (exp 12 left 7,600 stale ones); `RemoveIPC=no`, `LimitNOFILE=1048576` (all already in `neon.py prepare` / `nodes.systemd_unit`, reused). PG17 on Linux uses POSIX unnamed semaphores. `huge_pages=off`, `dynamic_shared_memory_type=posix`, parallel query off. Ports `24000+slot` (Neon's agent uses 20000+). |
| Network | One LAN at 25 GbE (measured 13–15 Gbit/s single stream; 0.07–0.12 ms ping). A 0.3 GB copy ≈ 0.15–0.4 s on the wire; a timed fork should land at a few hundred ms, the same order as a Neon compute start (207–356 ms in exp 12). **No netem**: a fresh instantiation has none; if the current nodes are kept, run `.agent/network-probe-2026-11-02-xl170/scripts/az_netem.sh remove` and capture `status` into `logs/` before any run. Single zone → every address is a LAN address. |
| Seed-node load | Every fork reads from a run root on the seed node. `--checkpoint=fast` = one immediate checkpoint per request, coalesced by the checkpointer (a batch of 64 setup forks costs ~2 checkpoints on an idle 40 MB-buffer parent). Parents need `max_wal_senders=96`, `max_connections=160`, `wal_keep_size=512MB`, role `bench` with `REPLICATION`, and a `host replication` pg_hba line. Per host, concurrent copies are throttled to 16 (SATA SSD write bound: 64 × 0.3 GB would take ≈ 40 s). Timed forks happen one at a time per creator. |
| Copy consistency | `pg_basebackup` is a consistent online copy (backup_label; the child recovers at start as a primary, no recovery.signal). The run root is never written by the workloads (branches get the `updates_per_branch` divergence). |
| Density vs exp 12 | Exp 12 put ≤ 146 computes on each of 7 nodes and 20 client processes on a separate node. Here client processes share the host. The report shows node CPU split into `postgres` and client. |
| Data at risk | Re-instantiating CloudLab wipes `/mnt/ssd` on every node: a0 holds `neon-src` (40 GB), `neon-rdma` (23 GB), `exp12` (19 GB), `cargo`/`rustup` (3 GB); b0 holds `exp12` (18 GB). **Copy `exp12` (both nodes) and the two Neon source trees off-cluster before re-instantiating.** |

## Design

### 1. Node agent: `scripts/setup/pgcopy_node_agent.py` (new)
Stdlib HTTP server, one per seed/host node (systemd unit `pgcopy-agent`), modelled on
`scripts/setup/neon_node_agent.py` — reuse its owner/waiter `Event` pattern in `ensure_compute`, `free_slot`,
`START_SEM`, `disk_free_gb`, `kill_by_cwd`, `clean_roots`, `remove_orphan_shm`, `keep_log`/`archive_line`,
`stats()`, the router/`Handler`/argparse.

- `POST /instances {name, parent: {host, port} | null, init: bool, start: bool (default true), shared_buffers?,
  statement_log?, parent_role: bool}`:
  - allocate slot → port `--pg-port-base + slot`, dir `<instance_root>/<name>`;
  - `init: true` (the seed only): `initdb -D dir -U bench --pwfile --auth=scram-sha-256`;
  - else, under a per-node copy semaphore (16): `pg_basebackup -h HOST -p PORT -U bench -D dir -Fp -X stream
    --no-slot --checkpoint=fast --no-sync --no-manifest` (timed → `copy_ms`; `bytes` = size of dir after);
  - **overwrite** `postgresql.auto.conf` entirely (the copy inherits the parent's: port, `listen_addresses='*'`,
    `unix_socket_directories='<dir>'`, `shared_buffers`, `huge_pages=off`, `dynamic_shared_memory_type=posix`,
    `max_parallel_workers_per_gather=0`, `log_min_duration_statement=0` iff statement_log, `logging_collector=off`,
    `statement_timeout=600s`; parents (`parent_role`: seed and run roots) get `max_wal_senders=96`,
    `max_connections=160`, `wal_keep_size=512MB`; host instances `max_wal_senders=8`, `max_connections=20`) and
    `pg_hba.conf` (`local all all trust`, `host all bench 127.0.0.1/32 scram`, `host all bench 10.0.0.0/8 scram`,
    `host replication bench 10.0.0.0/8 scram`);
  - if `start`: `Popen([postgres, -D, dir], start_new_session=True)` and poll `psql -c 'select 1'` every 20 ms
    (the Neon agent's loop, same resolution; not `pg_ctl -w`, which polls at 100 ms) → `start_ms`;
  - returns `{name, host, port, copy_ms, start_ms, bytes, started}`; the entry keeps `{state, event, copy_ms,
    start_ms, bytes}` so a late retry (HTTP timeout) returns the recorded timings with `started: false`.
- `POST /instances/<name>/start` (idempotent; for `PGCOPY_START_ON_BRANCH=0`), `GET /instances`,
  `GET /instances/<name>`, `DELETE /instances/<name>` (SIGTERM postmaster, `kill_by_cwd`, rmtree), `DELETE
  /instances` (all but `--protect seed`, parallel, then DSM sweep), `POST /instances/<name>/sql {sql, db}`,
  `GET /stats` (`instances`, `data_bytes`, `wal_bytes` from a cached scandir refreshed ≤ 1/s), `GET /time`.
- Args: `--listen`, `--host`, `--pg-bin /usr/lib/postgresql/17/bin`, `--instance-root /mnt/ssd/pgcopy/instances`,
  `--archive-dir`, `--protect seed`, `--shared-buffers 40MB`, `--max-concurrent-copies 16`,
  `--max-concurrent-starts 32`, `--start-timeout 300`, `--disk-guard-gb 20`, `--pg-port-base 24000`.

### 2. Backend: `dblib/pgcopy.py` (new; modelled on `dblib/neon_local.py`: `http()`, nonce scheme, lazy connection,
class-level `_INSTANCES` cache, `_connect`/`_ensure_conn` overrides, `purge/setup_run/teardown_run`)
- Env: `PGCOPY_AGENTS` (every host agent), `PGCOPY_ROOT_AGENT` (seed node), `PGCOPY_SEED_INSTANCE=seed`,
  `PGCOPY_LOCAL_AGENT` (co-located mode: branches this process creates go to this agent; its host is rewritten to
  `127.0.0.1` for connections), `PGCOPY_PG_USER/PASSWORD/DB` (`bench`), `PGCOPY_SHARED_BUFFERS`,
  `PGCOPY_INSTANCE_OPTS` (JSON: `statement_log`), `PGCOPY_START_ON_BRANCH` (default 1), `PGCOPY_RUN_NONCE` /
  `PGCOPY_RUN_ROOT` (set by `setup_run`).
- `instance_id_for(branch)`: `main` → run root; else `md5(nonce/branch)` (keep neon_local's `_<i>` spreading for the
  non-local mode). `agent_for(id)`: root agent for `main`; `PGCOPY_LOCAL_AGENT` if set; else hash over
  `PGCOPY_AGENTS`. Any process resolves any branch from its name; the agent's `GET /instances/<id>` gives the address.
- `create_instance(name, parent_name)`: `POST {agent_for(name)}/instances` with `parent = LAN addr(parent)` from
  the parent's agent, `start = PGCOPY_START_ON_BRANCH`, opts; `http` timeout 900 s. `setup_run(db_name)`: purge
  (keep seed), nonce, root created on the root agent from the seed with `parent_role: true`. `purge()`: `DELETE
  /instances` on every agent (the root agent protects `seed`). `teardown_run` = purge.
- Suite `PgCopyToolSuite(DBToolSuite)`: `BACKEND_NAME="pgcopy"`, `STORAGE_SCOPE="server"`, `SCHEMA_DIALECT="postgres"`,
  no commit refs / multi-ref exec. `_branch_impl` = `create_instance` (the timed BRANCH row = copy + start);
  `self.observe("fork_copy_ms"|"fork_start_ms"|"fork_bytes"|"forks")` so every summary's `backend_observations`
  carry the phase split; `last_fork` keeps the last response (cold start). `_connect_impl`: resolve (start if not
  started) → psycopg2 (3 attempts, refresh on failure). `_commit_impl` = `pg_current_wal_flush_lsn`. `_delete_impl`
  = DELETE (refuses `main`). `_storage_bytes` = Σ agents `data_bytes` (WAL excluded, as `file_copy`).
- Multi-process safe (nothing non-picklable in `BackendInfo`).

### 3. Runner plumbing (pattern = what NEON_LOCAL has)
- `microbench/task2.proto`, `macrobench/task.proto`: `PGCOPY = 14`; `build_protos.py`.
- `microbench/runner2.py`: import; `create_db_tools`; `BackendManager.setup` (`default_branch_name = MAIN_BRANCH`,
  `pgcopy.setup_run(db_name)`); `cleanup` (`teardown_run`); the seed_db allow-list at ~213-218 and
  `seed_generated_database` ~837 ("run root forked from the seed instance; nothing to copy").
- `scripts/run_throughput_bench.sh`: `PGCOPY` in the backend regex (~278), `pgcopy` in the `--seed-db` regex (~302).
- `macrobench/runner.py` `_capabilities_for`/`_implementation_for`: one line each.
- `microbench/fast_client/patches.py`: `patch_pgcopy` (no purge in `setup_run`, no-op `teardown_run` in multi-client
  mode, as `patch_neon_local`); `TARGETS["dblib.pgcopy"]`.
- Plot colour maps (`scripts/plotting/plot_micro_ops.py:40`, `plot_macro_cost.py`, `plot_micro_verbs.py`,
  `plot_branch_latency_micro.py`): add `pgcopy`.

### 4. Topology and setup
- `scripts/setup/topo.py`: `[pgcopy]` section `{seed, hosts, client = hosts[0], pg_version = 17, instance_root}`;
  `_check_pgcopy`; `role_node/client/wl/write_env/server_nodes/remote_cpu/shell_vars/main` learn the third backend.
  `server_nodes("pgcopy")` = seed + hosts; `remote_cpu` labels: `server=<viewer's own agent>?set=server` plus
  `seed=<seed agent>?set=server` and `computes=...?group=computes` (cpu_agent `--group-comm computes=postgres`, which
  `nodes.cpu_agent_service` already supports). `shell_vars`: `RUNNER_BACKEND=pgcopy`, `SEED_DB=bench`,
  `ENV_FILE=<state_dir>/pgcopy.env`, `SERVER_NODE` = the client's own node, `SERVER_SAME_HOST=1`. `pgcopy_env(view)`
  as in §2 (`PGCOPY_LOCAL_AGENT` = the viewer's agent when the viewer is a host).
- `scripts/setup/topologies/cloudlab_pgcopy.toml`: 16 nodes, one zone (addresses filled in when the new cluster
  exists), `[pgcopy] seed = <n0>, hosts = [<n1>..<n15>]`, `[workload] max_procs = 20`,
  `[pgcopy.workload] shared_buffers = "40MB"`; `state_dir = /mnt/ssd/bench-pgcopy` on the coordinator.
- `scripts/setup/pgcopy.py` (new; modelled on `scripts/setup/doltgres.py` with `nodes.Runner`): `install` (PGDG
  `postgresql-17` on seed + hosts, `neon.py`'s `prepare`/`remove_ipc_no`/`check_linger` equivalents, agent and
  `cpu_agent.py` to `<data_root>/pgcopy/svc`, units `pgcopy-agent` and `dbfork-cpu-agent`), `start|stop|status`,
  `seed` (on the seed node: `POST init` instance `seed` with `parent_role`; `ALTER ROLE bench REPLICATION`,
  `ALTER ROLE bench SET statement_timeout='600s'`, `CREATE DATABASE bench`, `CREATE EXTENSION pg_prewarm`; then
  `runner2.seed_generated_database(tp.Backend.PGCOPY, ...)` with `PGCOPY_RUN_ROOT=seed`, `VACUUM ANALYZE` (copies
  carry `pg_statistic`), sizes → `<state_dir>/pgcopy_seed.json`, as `neon_ctl.py seed`), `env-init [--view NODE]`,
  `deploy-client` (repo + venv + `pgcopy.env` on **every host**, plus `pgcopy.<node>.env` per host with its
  `PGCOPY_LOCAL_AGENT`, via `Runner.deploy_repo`), `purge`, `wipe --yes`, `smoke` (one fork per host from a run root,
  `select count(*) from stock` = 300,000, delete).

### 5. Workloads
- `scripts/workloads/common.sh`: `--backend neon|doltgres|pgcopy`; `load_env` also unsets `PGCOPY_*`; `guards` gets
  a `case $BACKEND` with a pgcopy arm (seed record present, local agent answers `/time`, no `DBFORK_SERVER_PID`;
  `start_agent` is skipped since the node's CPU agent already runs as a service); `shared_md5` adds `dblib/pgcopy.py`;
  `print_cmd` shows `PGCOPY_INSTANCE_OPTS`. The prewarm (`neon_prewarm_sql`) applies to pgcopy too (3,674 stock pages
  + `stock_pkey`, fits 40 MB).
- `low_contention.sh`, `scaling_data.sh`, `scaling_branch.sh`: the `[[ $BACKEND == neon ]]` blocks become
  `neon|pgcopy`; pgcopy exports `PGCOPY_INSTANCE_OPTS='{"statement_log":true|false}'`.
- `cold_start.py`: `pgcopy_run`: `create_instance(cold_k, main)` (timed `create_ms`; agent `copy_ms`/`start_ms`/
  `bytes` in the meta), first connect (`first_connect_ms`), reads, delete; choices gain `pgcopy`.
- `ceiling_check.py`: pgcopy arm = `cc_i` instances created in parallel (ThreadPool 16) on the local agent, targets
  `(127.0.0.1, port)`, prewarm, the backend-agnostic `sweep`.
- **Multi-node co-located driver** (`scripts/workloads/multi_client/multi_scaling.py` + `node_run.sh`, generalised):
  - `--backend neon|pgcopy`; per node the command sources `pgcopy.<node>.env` (its `PGCOPY_LOCAL_AGENT`), sets
    `PGCOPY_NO_PURGE` via the patch; the orchestrator purges once per point (`scripts/setup/pgcopy.py purge`). Run
    ids come from `RUNNER_BACKEND` (`pgcopy_chw3_...`), `md5s()` lists `dblib/pgcopy.py`.
  - `run` (scaling_data) unchanged otherwise: each node's runner has its own nonce → its own run root on the seed
    node, shares `n_i` of N, GO barrier via `BENCH_SYNC_ADDR`.
  - `run-branch` (scaling_branch): node 1 = creator runner (`--sweep-branches --threads 1 --branch-list n_1
    --processes min(n_1+1,20) --operations BRANCH_CREATE --branch-create-ops CREATES --branch-create-connect-every 5
    --background-op MIXED`); nodes 2..K = background-only runners (`--sweep-proportional --threads-per-branch 1
    --branch-list n_i --processes min(n_i,20) --operations MIXED --warmup-fraction 0 --num-ops <large>`), all on one
    GO barrier; when the creator exits the orchestrator sends SIGINT to the others (verify the runner writes its
    summary on SIGINT, as the drivers' `timeout -s INT` assumes; else add a `BENCH_STOP` check in `patches.py`).
    Merged summary = node 1's (create latencies) + a `multi_client` block (each node's background ops/s, client CPU),
    at `scaling_branch/branch_create/mixed/<rid>_BRANCH_CREATE_threads1_summary.json` so exp 12's `plot.py` reads it.
    `--single-node` runs the plain `scaling_branch.sh` on hosts[0] with `--ns 1,4,16,64,128` (secondary series).
  - `run-ceiling`: each node runs `ceiling_check.py` on its share, started together over ssh; per-node CSVs merged
    by summing throughput per (kind, N) into `scaling_data/ceiling_check.csv`.
  - `run_all.sh --multi` (default for pgcopy): scaling_data → `run`, scaling_branch → `run-branch` (+ single-node),
    ceiling → `run-ceiling`; low_contention and cold_start single node.

### 6. Experiment directory `experiment/13-pgcopy-nodes/`
`README.md`, `plan.md` (exported), `REPRODUCE.md`, `report.md`, `scripts/` (`launch.sh` as exp 12's with the client
host from the topology; `chain.sh` = install → start → seed → env-init → deploy-client → smoke → run_all; `pull.sh`;
`plot.py` reusing exp 12's `plot.py`/`refs.py` to add the pgcopy series next to Doltgres 10c, Neon local 10c, Neon
TCP, Neon RDMA; `fork_phases.py` for the copy/start split and bytes), `figures/`, `logs/`, `code/`,
`data -> /mnt/ssd/exp13`. Results layout `/mnt/ssd/exp13/pgcopy/<workload>/...`.

## Order of work
1. Export plan → `experiment/13-pgcopy-nodes/plan.md`; user backs up `/mnt/ssd/exp12` (a0, b0) and the Neon trees
   before the cluster changes.
2. Agent + backend + proto/runner plumbing; `tests/test_pgcopy.py` (mocked `http`, as `tests/test_neon_local.py`;
   no-purge in multi-client mode as `tests/test_multi_client.py`).
3. `topo.py` `[pgcopy]` (+ `tests/test_topo.py` cases), `pgcopy.py` setup script, topology file;
   `install/start/seed/smoke` on the new cluster.
4. Workload scripts and the multi-node driver; `--smoke` ladder: low_contention, scaling_data N=1,4 (multi),
   scaling_branch N=1,4 (multi + single), cold_start 1 run, ceiling N=4.
5. Full run via `chain.sh` (estimate: low contention ~20 min, scaling_data 32 points + ceiling ~1.5 h, scaling_branch
   7 + 5 points ~1.5 h, cold start ~10 min); pull, plot, report.

## Verification
- `uv run pytest tests/test_pgcopy.py tests/test_topo.py tests/test_multi_client.py`.
- `uv run python scripts/setup/topo.py check <topo>`; `scripts/setup/pgcopy.py <topo> smoke` (one fork per host,
  300,000 stock rows, delete; agents report 0 instances afterwards, root agent only `seed`).
- `scripts/workloads/run_all.sh --topology <topo> --backend pgcopy --outdir /mnt/ssd/exp13/smoke --smoke` passes
  every `check` (failed_ops 0, intended processes, `remote_cpu.*.node_cores` present for seed and hosts).
- Fork sanity: BRANCH rows ≈ copy_ms + start_ms (+ HTTP); `fork_bytes` ≈ 0.3 GB; a branch's `stock` count equals
  the seed's; `max_wal_senders` never exhausted during a 64-way setup (agent log).
- Netem: `az_netem.sh status` shows no `netem` on any node before the full run (captured into `logs/`).
