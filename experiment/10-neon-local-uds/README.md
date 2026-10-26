# Experiment 10: Neon on one machine, every connection on a Unix socket

Self-hosted Neon (the fork at `/mydata/jiyu/neon`, branch `local-uds`) deployed as plain local processes on
`dassl-serv-01`, with every service-to-service connection moved from TCP to Unix domain sockets. Neon runs on NUMA
node 2 (24 cores, its memory), the benchmark client on node 3, as experiment 7 placed Doltgres. Experiments 5/8's
three benchmarks are rerun: latency at low contention, data operations with every branch busy, and branch creation
with busy branches. 25-26 Oct 2026.

## Question

Experiment 7's local baseline, Doltgres, is limited on its write path by Dolt's own design. Where does a
single-machine branchable database hit its limit when the design is not the problem? Neon on one machine, with no
network in the way, is measured until it saturates a resource (CPU, disk, memory), and the resource is named. A
second question is what the IPC layer itself costs: the same binaries on loopback TCP give the comparison.

## Verdict

On one machine with no network, Neon's limits are resources, not design defects. With one busy branch every
operation takes well under a millisecond (point read 0.34 ms, update 0.54 ms, branch create 1.8 ms; cross-zone in
experiment 5 they took 41, 42 and 85 ms). With every branch busy:

- reads and inserts saturate node 2's 24 cores: 71-77k point reads/s, 18-20k range reads/s, 50k inserts/s, with the
  CPU split between the computes' Postgres and the pageserver (reads) or the safekeeper (inserts);
- updates stop at 20k/s with CPU to spare, limited by synchronous writes to the one NVMe: every commit of every
  branch is its own safekeeper `fdatasync` (branches are separate timelines, so nothing is batched across them), and
  the mean fsync grows from 0.15 ms to 7 ms between 16 and 512 branches;
- memory caps the busy branches at 512 (77 MB per running compute; N = 1,024 does not fit in node 2's 64 GB);
- branch creation stays at 2-3 ms up to 512 busy branches, while Doltgres's grows with load (experiment 7).

Moving every connection to Unix sockets cuts a raw round trip from 18 to 6 µs, which saves 13-47 µs per statement
and 4-7% of throughput where the CPU is the limit; disk-bound updates do not change.

- Report: [report.md](report.md)
- Plan: [plan.md](plan.md) (as approved; deviations are listed in the report)
- Rerun instructions: [REPRODUCE.md](REPRODUCE.md)
- Code: this repo at [code/base_commit.txt](code/base_commit.txt) plus [code/shared_code.patch](code/shared_code.patch);
  the Neon fork as [code/neon_fork.patch](code/neon_fork.patch) on its commit in
  [code/neon_fork_commit.txt](code/neon_fork_commit.txt); binary hashes in [code/bin_sha256.txt](code/bin_sha256.txt)

## Neon fork changes (branch `local-uds`, uncommitted on 4187e2da7)

Every listener accepts `unix:/abs/path` where it took `host:port`; TCP stays the default, so the same binaries run
the TCP arm. Libpq-protocol servers bind `<dir>/.s.PGSQL.<port>`, so every libpq client (compute backends, the
walproposer, compute_ctl, psql, pgbench) reaches them with `host=<dir> port=<port>` and needs no change. HTTP and
gRPC endpoints on a socket are written `http+unix://<hex of the socket path>/<path>`.

| Connection | Protocol | Change |
|---|---|---|
| compute backends -> pageserver (GetPage) | libpq `pagestream_v3` | pageserver listener: `pageserver/src/bin/pageserver.rs`, `pageserver/src/page_service.rs` |
| compute_ctl -> pageserver (basebackup) | libpq | none |
| walproposer -> safekeeper (WAL) | libpq | safekeeper listener: `safekeeper/src/bin/safekeeper.rs`, `safekeeper/src/wal_service.rs` |
| pageserver -> safekeeper (WAL pull) | tokio-postgres | host parser accepts a directory: `libs/postgres_connection/src/lib.rs` |
| safekeeper, pageserver <-> storage broker | gRPC (tonic) | broker accept loop + client connector: `storage_broker/src/{bin/storage_broker.rs,lib.rs}` |
| pageserver -> storage controller (re-attach, validate) | HTTP (reqwest) | `pageserver/src/controller_upcall_client.rs` via `libs/uds_http` |
| storage controller -> pageserver, safekeeper, hook | HTTP (reqwest) | `pageserver/client`, `safekeeper/client`, `storage_controller/src/{compute_hook,node,safekeeper,service}.rs` |
| storage controller -> its Postgres | tokio-postgres | no TLS for a socket host: `storage_controller/src/persistence.rs` |
| HTTP servers of pageserver, safekeeper, storage controller | hyper | `libs/http-utils/src/server.rs` |
| compute_ctl HTTP; neon extension -> compute_ctl | axum / libcurl | `--external-http-socket`, `--internal-http-socket`; `neon.extension_server_socket` |
| client, compute_ctl -> compute Postgres | libpq | configuration only (`listen_addresses=''`, `unix_socket_directories`) |

New: `libs/utils/src/listen_addr.rs` (listen address, listener and stream types for both transports) and
`libs/uds_http` (HTTP over a Unix socket for the reqwest clients; reqwest 0.12.4 has no Unix-socket connector).
Already local before: WAL redo (pipes), remote storage (`local_path` files). In the uds arm no Neon process holds a
TCP port; the only TCP listener of the experiment is the CPU sampler's (instrumentation on node 3, polled once a
second, never on a timed path).

## Shared code changed in this repo

- `dblib/neon_local.py`: service URLs may be `http+unix://...` (HTTP over a Unix socket); a compute's host may be a
  socket directory. Tests: `tests/test_neon_local.py` (URL handling, a round trip over a socket, directory hosts).
- Nothing else; the experiment also runs on experiment 5's uncommitted runner changes (in `code/shared_code.patch`).

## Layout

```
README.md, plan.md, report.md, REPRODUCE.md
code/        base_commit.txt, git_status*.txt, shared_code.patch, neon_fork_commit.txt, neon_fork.patch, bin_sha256.txt,
             rtt_probe/ (compiled probe)
config/      compute_config_template.json (compute spec template), compute_hook_stub.py (notification stub, UDS or TCP)
scripts/     env.sh (CPU sets, paths, ports, address helpers), neon_services.sh (install/start/stop/status),
             neon_node_agent.py (compute agent), neon_ctl.py (env-init, seed, smoke, purge, metrics),
             cpu_agent.py (CPU/disk/memory sampler with per-process-group CPU), common.sh, run_low_load.sh (A),
             run_scaling.sh (B), run_branch_create.sh (C), ceiling_pgbench.py (pgbench ceiling), run_all.sh,
             run_followup.sh, run_ceiling.sh, ipc_microbench.sh + rtt_probe.c, disk_probe.sh (fio), analyze.py
data/neon_local/
  seed/<transport>/      seed.json (tenant, timeline, relation sizes), env.exp10
  low_load/<transport>/  A: data_ops/<OP>/, branch_create/, cpu/, metrics/, logs/, progress.log
  scaling/<transport>/   B: data_ops/<OP>/, cpu/, metrics/, logs/, progress.log  (tcp: READ at N = 1, 16, 64, 256)
  branch_create/uds/     C: one parquet + summary per N, cpu/, metrics/, logs/, progress.log
  ceiling/<transport>/   ceiling_pgbench.csv, cpu/<kind>_n<N>[_r<rep>].csv, pgbench/ (per-process output),
                         agent_stats_n<N>.json
  ipc/                   rtt_probe.csv, pagebench.csv, fio.csv, raw/
data/superseded/         smoke runs; ceiling_*_512_resident and ceiling_uds_no_settle (first ceiling runs, see report);
                         probe_update_few_computes; smoke_ceiling_uds_pc1g (pageserver cache 1 GiB check)
figures/     low_load, data_ops, scaling_cpu, branch_create, tcp_vs_uds (.png and .csv/.md), ceiling.csv/.md,
             cpu_window.csv, summary.csv, tcp_vs_uds.md, write_path.csv/.md (WAL and fsync per commit)
logs/        fork_build.log, fork_tests.log, seed_*.log, smoke_*.log, driver_smoke_*.log, run_all.log,
             run_followup.log, run_ceiling.log, ipc_microbench.log, disk_probe.log
```

Outside the repo, `/mydata/jiyu/neon-exp10/`: `bin/` (binary snapshot), `run/` (sockets, pid files), `uds/` and
`tcp/` (pageserver, safekeeper and storage-controller state per transport), `computes/`, `archive/` (agent:
`starts.jsonl` compute start phases, `samples.jsonl` 1 s service metrics, `pg/` low-load compute logs), `logs/`
(service logs), `env.exp10`.

## File formats

- Runner runs (A, B, C) follow experiments 5 and 7: `<run_id>.parquet` (one row per timed statement and per
  CONNECT/BRANCH; `thread_id` 0 is the creating client in C), `<run_id>_<OP>_threads<T>_summary.json` (throughput,
  latency quantiles in seconds, `server_cpu_cores` = pageserver process, `remote_cpu.<label>` with
  `proc_cores`/`node_cores`/`disk_util` per label: `server` (node 2 cores, Neon's process groups summed as
  proc_cores, NVMe), `clients` (node 3), and one label per process group).
- `cpu/<run>.csv`: every 0.5 s and CPU set (`server` = node 2, `clients` = node 3): busy, user, system, softirq and
  iowait cores, per-core busy. `cpu/<run>.procs.csv`: every 1 s, cores per process group (`pageserver`, `walredo`,
  `safekeeper`, `storcon`, `storcon_pg`, `broker`, `compute_ctl`, `computes`, `agent`, `hook`, `neon` = all but
  agent and hook, `other_node2` = node-2 busy minus every group), `disk_*` rows (busy fraction, MB/s, IOPS) and
  `mem_*` rows (node-2 MemFree, FilePages, AnonPages, Shmem, Available, MB).
- `metrics/<run>_{before,after}.txt`: Prometheus metrics of pageserver, safekeeper and storage controller.
- `ceiling_pgbench.csv`: one row per (kind, N, rep): ops/s from pgbench, latency quantiles (ms) from its 10%-sampled
  transaction log, client CPU, `<label>_proc_cores` / `<label>_node_cores` for the same labels, NVMe MB/s and IOPS.
- `figures/cpu_window.csv`: every runner run, mean per-group cores over its exact timed window (`w_partial` when the
  window is under 2 s, i.e. fewer than two 1 s samples).
