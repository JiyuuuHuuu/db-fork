# Experiment 7: Doltgres reads and branch creation on the local machine, server on 10 cores

Experiment 2's Doltgres READ / RANGE_READ runs and branch-creation runs, repeated on the local server
`dassl-serv-01` (4× Xeon Gold 6418H) with the server pinned to 10 cores of NUMA node 2 and the clients on 38 cores
of nodes 2-3, Doltgres 1.4.0, 23 Oct 2026.

## Questions

- A. Are reads across N busy branches (N = 1 to 1,024) limited by the server's CPU, and nothing else? CPU is
  monitored on both sides, with a minimal-client read check and a 5-vs-10-core sweep.
- B. Do only writes slow branch creation? Branch creation with every existing branch running point reads only
  (experiment 2 used 80% reads / 20% updates). Latency plot only.

## Verdict

- A: **yes.** From 16 busy branches on, the server uses 9.5-9.99 of its 10 cores and throughput is flat (about 25,000
  point reads/s and 7,300-8,500 range reads/s). The clients use 3-5 of 38 cores. A minimal client reaches the same
  ceiling, and 5 cores give half of it.
- B: **reads do not interfere through database logic, only through CPU.** With read-only busy branches, branch
  creation slows from 1.3 ms (1 busy branch) to 6.7 s (1,024), but only once the reads fill the server's cores. A
  follow-up at 1,024 busy branches keeps the same 23,000 reads/s and gives the server 20 cores instead of 10: the
  creates return to 1.84 ms (unloaded speed). Fewer reads on 10 cores do the same; more reads that fill 20 cores
  bring the slowdown back. Lock waits were not observed directly (no profiling endpoint in Doltgres 1.4.0, no root).
  A follow-up on 25 Oct 2026 with the same setup and experiment 2's 80% read / 20% update background (N = 1, 4, 16,
  three runs each) shows that **updates do slow creation beyond CPU**: 5.5-6.0 ms at 16 busy branches against 3.3 ms
  with reads only, with the server on about half its cores.

- Report: [report.md](report.md)
- Plan: [plan.md](plan.md)
- Step-by-step rerun instructions: [REPRODUCE.md](REPRODUCE.md)
- Exact shared-code state used: base commit `f004d9f` plus [code/shared_code.patch](code/shared_code.patch)
  (experiment 5's uncommitted changes, not used here; experiment 7 changes no shared code)

## Layout

```
README.md, plan.md, report.md, REPRODUCE.md
code/        base_commit.txt, git_status.txt, shared_code.patch (git diff f004d9f, untracked test included)
figures/     data_ops.png, branch_create.png (latency only; read-only and 80/20 backgrounds), branch_create_mixed.csv,
             core_sweep.md, cpu_window.csv,
             summary.csv / summary.md (every plotted point)
data/dolt/   data_ops/        READ and RANGE_READ, N = 1, 4, 16, 64, 256, 1024
             branch_create/   100 creates with every branch running point READs, same N
             cpu/             per-run CPU samples of the server's and clients' cores (cpu_agent.py)
             read_check.csv   minimal client, server on 10 cores, N = 64, 256, 1024
             read_check_5c.csv  the same at N = 256 with the server on 5 cores (core sweep)
             create_under_reads.csv, create_under_reads_latency.csv  the follow-up on branch creation
               (C0-C3 plain branches, D0-D3 with 100 uncommitted updates each; one row per run / per create)
             branch_create_mixed/   100 creates with every branch running 80% READ / 20% UPDATE, N = 1, 4, 16
               (25 Oct 2026; plotted), repeats_mixed/r2, r3: two repetitions of it
             cpu/create_mixed_n<N>.csv  CPU samples of those runs
             logs/, progress.log
data/superseded/smoke_dolt/   the smoke run (N = 1, 16; ops / 10; 10 creates), not analysed
                              (branch_create_mixed/: the 80/20 follow-up's smoke run, 25 Oct)
data/superseded/smoke_create_under_reads/   the follow-up script's smoke runs (64 branches, 10 creates), not analysed
logs/        driver_smoke.log, driver_full.log, server_<time>_<K>c.log, pinning_idle.log, pinning_busy.log,
             read_check_10c.log, read_check_5c.log, server_memory_readcheck.log,
             create_under_reads_{C,D}.log, create_under_reads_setup*.log, pinning_20c_idle_{C,D}.log
scripts/     env.sh (CPU sets, paths, ports), doltgres_server.sh (install / start K / stop / status),
             cpu_agent.py, run_busy_branches.sh (the sweep), read_ceiling_check.py, plot_busy_branches.py,
             cpu_window.py, create_under_reads.py + run_create_under_reads.sh (the follow-up)
```

Server files (binary, config, data) are outside the repo, on NVMe: `/mydata/jiyu/doltgres-exp7/`.

Run ids and files follow experiment 2 (`dolt_chw1_tp_t<clients>_b<N>`): `<run_id>.parquet` holds one row per timed
operation; in `branch_create/` the creating client is `thread_id` 0 and the background clients are 1..N. READ and
RANGE_READ at the same N share one parquet (range reads are the rows whose SQL contains `>=`).
`<run_id>_<OP>_threads<T>_summary.json` holds throughput, latency quantiles (seconds), `server_cpu_cores`
(server process), `client_cpu_cores` and `remote_cpu` (cores' busy time from the CPU agent, interpolated from
1 s polls, so coarse for short windows). `cpu/<run>.csv`: one row per 0.5 s and CPU set with busy, user, system,
softirq and iowait cores and each core's busy fraction. `data/dolt/branch_create/dolt_chw1_tp_t1_b1024.parquet`
is 1.2 GB (35 million rows, mostly background reads).

## Differences from experiment 2

| | Experiment 2 | Experiment 7 |
|---|---|---|
| Machine | CloudLab c220g2: 2× Xeon E5-2660 v3 | local: 4× Xeon Gold 6418H, no SMT |
| Server CPU | socket 0: 20 hardware threads | 10 cores of NUMA node 2 (5 for the core sweep) |
| Client CPU | socket 1: 20 hardware threads, ≤ 20 processes | 38 cores, ≤ 38 processes |
| Storage | SATA SSD, XFS | NVMe, ext4 |
| Port | 5432 | 5433 |
| Operations | READ, RANGE_READ, INSERT, UPDATE | READ, RANGE_READ |
| Branch-creation background | MIXED (80% READ / 20% UPDATE) | READ |
| N | every power of 2 from 1 to 1,024 | 1, 4, 16, 64, 256, 1,024 |
| Reads per run | 40,000 point / 10,000 range | 200,000 / 50,000, at most 4,000 / 1,000 per client |
| Unchanged | Doltgres 1.4.0 and config, data, table, 100 updates per branch, fan-out, sync clients with one request in flight, 10% warm-up, 100 creates | |

## Shared code

None changed. The experiment uses existing hooks: `DBFORK_SERVER_PID` (server CPU), `DBFORK_REMOTE_CPU`
(`RemoteCpuSampler`, fed by `scripts/cpu_agent.py`), `--processes` and `--background-op READ` of
`scripts/run_throughput_bench.sh`.
