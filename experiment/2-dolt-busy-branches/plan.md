# Plan: Dolt with every branch busy

Follow-up to `experiment/1-dolt-branch-scaling`. Requested on 2026-10-18:

> For H3, I want to also understand the behavior with busy branches, meaning each branch that runs are
> actually issuing reads. Also for H2, are you also running idle branches? I do not want idle branches.

> You do not need to over complicate the report and analysis. I just want to run data ops and branch creation
> with N existing branches. The branches should be running workloads as well, then gather the latency and
> throughput into plots.

## Runs

N = 1, 2, 4, …, 1024 existing branches on Dolt 2.4.2 and Doltgres 1.4.0. Every branch gets 100 untimed row
updates when it is created, and every branch is busy during the measurement.

| Part | What runs on the N branches | What is measured |
|---|---|---|
| Data operations | one client per branch running the operation back to back (READ, RANGE_READ of 100 rows, INSERT, UPDATE) | latency and total throughput of that operation |
| Branch creation | one client per branch running a mix of 80% point reads and 20% point updates | one more client creates 100 branches (10 more untimed first), starting 1 s after every background client is working: their latency and throughput |

- One request in flight per client. Clients are spread over up to 20 processes on the client CPU socket, so the
  client is not the bottleneck (experiment 1's runner used one Python process).
- Ops per client: READ max(50, 40000/N), RANGE_READ max(10, 10000/N), INSERT and UPDATE max(10, 6000/N), 10%
  untimed warm-up.
- Data: CH-benCHmark W=1 copied from `ch_w1_seed`, table `stock`. Same nodes and server settings as experiment 1;
  Dolt's `max_connections` raised to 2048.

## Outputs

Two figures (data operations, branch creation), each with latency and throughput against N, a short
`report.md`, and the raw results in `data/<backend>/{data_ops,branch_create}`.

## Shared code changes this needs

- `runner2`: `num_processes` (clients spread over processes), `background_load` / `background_operation` (one
  client per setup branch running a workload during the measurement), the `MIXED` operation, server and client
  CPU per run.
- Table metadata looked up once per client process. The write operations ran an untimed information_schema query
  before every operation, which loaded the server and capped an async worker near 50 writes/s (this affected
  experiment 1's per-branch write rates; its report is corrected).
- `run_throughput_bench.sh`: `--processes`, `--background-op`, `--sweep-threads`.
