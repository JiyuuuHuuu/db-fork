# Experiment 11: Neon on Doltgres's 10 cores

Experiment 10's local Neon (the fork at `/mydata/jiyu/neon`, branch `local-uds`, every service-to-service connection
on a Unix domain socket) rerun on experiment 7's hardware setup: Neon on 10 cores of NUMA node 2 (CPUs 2,6,...,38),
the clients on the other 38 cores of nodes 2 and 3, experiment 7's W=1 dataset. Same three benchmarks (latency at low
contention, data operations with every branch busy, branch creation with busy branches), plus branch creation under
a saturating pgbench load, with the CPU of the server cores and of the client cores recorded at every point.
26 Oct 2026.

## Question

How do the two single-machine branchable databases scale on the same hardware? Experiment 10 gave Neon 24 cores and
never saturated them during branch creation; Doltgres (experiment 7) had 10 and its creates slowed by orders of
magnitude once they were full. Does Neon's branch creation also degrade when its CPU is full, where do its data
operations stop on 10 cores, and which side (server or client cores) is the bottleneck?

## Verdict

Both systems are limited by the server's 10 cores, never by the clients (at most 5.6 of 38 client cores for reads).

- **Data operations.** Neon makes more of the 10 cores: 35k point reads/s at its peak (64 busy branches) against
  Doltgres's 26.5-28.4k, at 277 against 377 µs of server CPU per read. But Neon needs more branches to fill the cores
  (64 against 16), and past its peak it loses more: 36% by 512 branches, against Doltgres's 12% by 1,024. Each
  running branch is a separate Postgres, and the per-branch overhead raises the CPU per read by 57%. Below
  saturation, 10 cores cost Neon more than their share (21.7k reads/s at N = 16 against 56.7k on 24 cores), because
  its threads already wait on run queues. Writes are CPU-bound too: 20k inserts/s, 11-12.5k updates/s.
- **Branch creation.** It does suffer CPU contention in Neon: with the cores full, the create median rises from
  2.1 ms to 4-9 ms, with p95 up to 0.24 s (on 24 cores it stayed at 2-3 ms). That is 3-4x, against Doltgres's 10x
  at 64 busy branches, 65x at 256 and 5,000x at 1,024 under the same saturation.
- **Making a new branch usable** costs Neon a compute start, which degrades most: 0.15 s with one busy branch,
  1-5 s with 512. Doltgres has no such step.
- **Memory** caps Neon at 512 running branches (43 MB each); Doltgres served 1,024.

Addendum, 27 Oct 2026 (experiment 7's Doltgres setup on the same cores):

- **Doltgres writes** take 1.6-1.8 ms at one busy branch and peak at 1.6k/s at 16 busy branches, against Neon's 20k
  inserts/s and 11-12.5k updates/s. They are limited by neither side's CPU (server at most 3.2 of 10 cores) but by
  the database's chunk-store lock, which every chunk written takes exclusively: 49 of an update's 52 ms at 64 busy
  branches is waiting for it.
- **Why Doltgres's creation degrades more than Neon's.** A create needs only 1.2-2.6 ms of CPU at any load. It
  waits behind two queues inside the server process that the read load fills:
  - Go's run queues, which a create re-enters 23-52 times (a read about twice);
  - the same chunk-store lock, which a create takes exclusively 10-24 times and which reads missing the node cache
    also take exclusively.
  With dirty branches the lock waits become a convoy (87-98% of 0.5-2.6 s creates); with plain branches the run
  queues dominate (99 ms at 1,024). Filling the cores with spinning threads instead of reads costs a create only
  1-15x, the same order as Neon's 2-4x. Neon's create runs in separate, mostly idle processes that share no lock
  with the reads: its storage controller waits 1-4 ms for a core per create, and nice +19 changes the median by at
  most 1.6 ms. Report section D.

- Report: [report.md](report.md)
- Plan: [plan.md](plan.md) (decisions asked 26 Oct 2026; deviations in the report)
- Rerun instructions: [REPRODUCE.md](REPRODUCE.md)
- Code: this repo at [code/base_commit.txt](code/base_commit.txt) (no shared-code changes); the Neon fork as
  [code/neon_fork.patch](code/neon_fork.patch) on [code/neon_fork_commit.txt](code/neon_fork_commit.txt), with the
  io_uring patch alone in [code/tokio_epoll_uring.patch](code/tokio_epoll_uring.patch); binary hashes in
  [code/bin_sha256.txt](code/bin_sha256.txt)

## Changes against experiment 10

- **Neon fork:** a vendored `tokio-epoll-uring` (`vendor/tokio-epoll-uring`, wired in by a `[patch]` section of the
  workspace `Cargo.toml`). Before each thread's first io_uring submission it registers the thread's CPU affinity
  for the ring's io-wq workers (`IORING_REGISTER_IOWQ_AFF`). On kernel 5.15 those workers otherwise use every CPU of
  the NUMA node, whatever the process's affinity.
- **Scripts:** copied from experiment 10 and adapted (paths `neon-exp11`, `env.exp11`, `EXP11_*`). Changes:
  - `env.sh`: experiment 7's CPU sets. Services are bound with `taskset`. `EXP11_PROBE_24=1` gives experiment 10's
    placement, for the probe only.
  - `run_scaling.sh`: experiment 7's read op counts.
  - `run_branch_create.sh`: `--background READ|MIXED`.
  - `ceiling_pgbench.py`: absolute output path.
  - New: `create_under_pgbench.py` (C'), `sched_probe.py` and `run_probes.sh` (follow-up probes),
    `plot_placement.py`, and `analyze.py` (comparison with experiments 7 and 10).
  - Addendum (27 Oct 2026):
    - `create_under_pgbench.py` gains `--sched`, `--nice-storcon`, `--nice-pageserver`, `--out-sub` and `--tag`.
    - New: `run_create_sched.sh` (N) and `neon_create_spans.py`.
    - `analyze.py` adds the Doltgres writes from `data/dolt`.
    - New `scripts/dolt/` for Doltgres, copied from experiment 7 and adapted:
      - `env.sh`;
      - `doltgres_server.sh`, with `--trace`, `--godebug`, `--hog`, and the server bound with `taskset`;
      - `cpu_agent.py`;
      - `run_busy_branches.sh`, for writes, with per-op output directories;
      - `create_under_reads.py`, adding branch-set prefixes, update readers, per-create wall times, server
        `schedstat`, and a mode with no busy branches.
    - New in `scripts/dolt/`:
      - `run_create_point.sh` and `run_create_analysis.sh` (T and H);
      - `cpu_hog.c`;
      - `gotrace/`, a Go trace analyzer on `golang.org/x/exp/trace`;
      - `analyze_create.py`.
- **Shared code in this repo:** none changed.

## Layout

```
README.md, plan.md, report.md, REPRODUCE.md
code/        base_commit.txt, git_status.txt, neon_fork_commit.txt, neon_fork.patch, tokio_epoll_uring.patch,
             bin_sha256.txt
scripts/     env.sh (CPU sets, paths, ports), neon_services.sh (install/start/stop/status), neon_node_agent.py
             (compute agent), neon_ctl.py (env-init, seed, purge), cpu_agent.py (CPU/disk/memory sampler with
             per-process-group CPU), common.sh, run_low_load.sh (A), run_scaling.sh (B), run_branch_create.sh (C),
             ceiling_pgbench.py (B ceiling), create_under_pgbench.py (C'), smoke.sh, run_all.sh, sched_probe.py,
             run_probes.sh, analyze.py, plot_placement.py
data/neon_local/
  seed/uds/              seed.json (tenant, timeline, relation sizes), env.exp11
  low_load/uds/          A: data_ops/<OP>/, branch_create/, cpu/, metrics/, logs/, progress.log
  scaling/uds/           B, runner: data_ops/<OP>/, cpu/, metrics/, logs/, progress.log
  ceiling/uds/           B, pgbench: ceiling_pgbench.csv, cpu/, pgbench/, agent_stats_n<N>.json
  branch_create/read/    C, read-only busy branches: one parquet + summary per N, cpu/, metrics/, logs/, progress.log
  branch_create/mixed/   C, 80/20 busy branches: same
  create_under_pgbench/  C': creates.csv, summary.csv, cpu/, pgbench/
  probe_24cores/         ceiling point/range at N = 4, 16, 64 on 24 cores
  probe_sched/           sched_<cores>cores_n<N>.json (run-queue wait per process group), ceiling_<cores>cores_n<N>.csv
  create_sched/          addendum N: creates_<tag>.csv, creates_<tag>_spans.csv, summary_<tag>.csv (tag base, nice_sc,
                         nice_scps), cpu/, pgbench/
data/dolt/               addendum, Doltgres (experiment 7's setup)
  data_ops/{INSERT,UPDATE}/  runner summaries and parquets, N = 1, 4, 16, 64, 256, 1,024; cpu/, logs/, progress.log
  create_trace/<point>/  T: D64, D256, D1024, C1024, D1024half, U64, each also traced (<point>_tr); progress.log
  create_hog/<point>/    H: H0, H<T>s, H<T>o (T = 10, 128, 1,024), H1024s_tr; progress.log
data/superseded/
  before_iowq_pin/       smoke runs before the io_uring patch (pageserver io-wq workers on all of node 2)
  smoke_iowq_pin/        the ceiling smoke check after the patch
  smoke_dolt_writes/     addendum: the write sweep's smoke run (N = 1, 16, a tenth of the counts)
  smoke_create_trace/    addendum: the trace smoke run (D64, 10 creates, untraced and traced)
figures/     placement.png; low_load, data_ops, cpu_sides, scaling_cpu, branch_create (.png and .csv/.md);
             ceiling, create_under_pgbench, write_path, memory, scaling_cpu_runner, probe_24cores, probe_sched
             (.csv/.md); the plots show the benchmark runner only (Neon, and Doltgres from experiment 7),
             the tables every series;
             cpu_window.csv, summary.csv
logs/        fork_build.log, seed_uds.log, smoke_all_before_iowq_pin.log, smoke_iowq_pin.log, run_all.log,
             probe_24cores.log, probe_sched.log; addendum: create_sched.log (N), dolt/ (driver_smoke_writes.log,
             driver_writes.log, create_analysis*.log, create_setup_{rd,rc}.log, server_<time>_10c.log per server start,
             pinning_idle.log)
```

Outside the repo, `/mydata/jiyu/neon-exp11/`: `bin/` (binary snapshot), `run/` (sockets, pid files), `uds/`
(pageserver, safekeeper and storage-controller state), `computes/`, `archive/`, `logs/` (service logs),
`env.exp11`.

Outside the repo, `/mydata/jiyu/doltgres-exp11/` (addendum):
- `dl/`: experiment 7's Doltgres 1.4.0 binary, same sha256;
- `config.yaml`, `databases/` (`ch_w1_seed` with the 1,024 plain `rc_*` branches still present), `home/`;
- `traces/<point>/trace.out`: the 7 raw Go traces, 0.13-0.85 GB each;
- `gotrace` and `cpu_hog` binaries;
- `go/`: the module cache and Go 1.26.8 toolchain used to build them.

## File formats

As experiment 10 (see its README), plus:

- `cpu/<run>.csv` sets: `server` = the 10 Neon cores, `clients` = the 38 client cores.
- `create_under_pgbench/creates.csv`: one row per timed create: `kind` (point or update background),
  `busy_branches`, `i`, `t`, `create_ms`, and `connect_ms` for every 5th create (compute start plus `SELECT 1`).
- `create_under_pgbench/summary.csv`: one row per (kind, N): create and connect quantiles, `background_ops_per_s` and
  `background_failed` (pgbench), `<label>_proc_cores` / `<label>_node_cores` over the pgbench period, and
  `create_window_{server,clients}_node_cores` over the timed creates.
- `probe_sched/sched_*.json`: per process group (`pageserver`, `computes` = Postgres and compute_ctl, `control`),
  over 10 s of the pgbench run: threads, `run_cores` and `wait_cores` (time on a CPU and time runnable on a run queue,
  from `schedstat`), timeslices, voluntary and involuntary context switches per second.
- `figures/cpu_sides.csv`: per series, operation and N: throughput, `server_cores` and `client_cores` (per-core busy
  of each set), `client_own_cores` (the benchmark processes' own CPU), `client_processes`, `limit` (the verdict).

Addendum:

- `data/dolt/data_ops/<OP>/`: experiment 7's runner formats (`<run_id>.parquet`, `*_summary.json`).
  `data/dolt/cpu/<run>.csv` is experiment 7's CPU-agent format.
- **`data/dolt/create_{trace,hog}/<point>/`:**
  - `run.csv`: one row. It has:
    - the create quantiles in ms;
    - the reads' rate and quantiles (`read_*`; with `op` update they are the updates');
    - over the creates' window: `server_cores_busy` (`/proc/stat`), `server_cpu_cores` (process),
      `thread_run_cores` and `thread_wait_cores` (the server threads' `schedstat`: on a CPU, runnable on a kernel run
      queue);
    - `window_start` and `window_end` (Unix seconds), `hog`, `trace`.
  - `creates.csv`: one row per create, with `start_wall` and `latency_ms`.
  - `server.log`: the server's log, with `GODEBUG` `SCHED` lines (Go run queues every second) and `gc` lines.
  - `driver.log`, `server_start.txt`.
  - Traced points also have `trace_file.txt` and `trace/`, from `gotrace`:
    - `statements.csv`: one row per statement of the creating connection and of 64 sampled others. The columns are
      the time in each state (running, runnable = in a Go run queue, syscall, sync = lock, gc, network, other,
      family_wait = hand-offs inside the statement's goroutines) and the counts of run-queue entries, preemptions,
      system calls and lock waits.
    - `waits.csv`: time and count per wait or syscall site, for the creating connection; `U64_tr` has every written
      connection (`-allsites`).
    - `syncblocks.csv`: every lock wait of a create, with the goroutine that released it and that goroutine's time
      runnable, running, blocked or in a syscall during the wait.
    - `runq.csv`: mean runnable and running goroutines per 0.1 s.
    - `summary.json`.
- **`data/neon_local/create_sched/creates_<tag>.csv`:** C' columns, plus per create `timeline_id`,
  `storcon_run_ms` / `storcon_wait_ms` and `pageserver_run_ms` / `pageserver_wait_ms`. These are thread time on a
  CPU and runnable on a kernel run queue, summed over the process; the pageserver's includes its GetPage work.
  `*_spans.csv` adds the storage controller's and the pageserver's request spans from their logs.
- **`figures/`:**
  - `create_points`: one row per T/H point;
  - `create_breakdown`: traced creates, mean ms per state, and the reads in the same window;
  - `create_locks`: wait sites per create;
  - `update_breakdown` and `update_sites`: U64_tr;
  - `create_neon`: N per tag and N;
  - `create_breakdown.png`, `create_contention.png`.
