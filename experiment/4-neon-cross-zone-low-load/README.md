# Experiment 4: Neon low-load latency with compute and storage in different zones

## Question

How much slower are Neon's operations at low load (one busy branch, one client) when the compute and the
client sit in one cloud zone and Neon's storage layer (pageserver, safekeepers, storage controller) in
another, and where does the time go? This re-measures the Neon bars of experiment 3's `low_load.png` on a
two-zone CloudLab cluster (38.9 ms round trip between zones), with the compute's local file cache (LFC)
enabled and a dataset large enough that most reads still miss it.

## Verdict

Every low-load operation costs a whole number of WAN round trips and little else; medians are 8 to 29 times
experiment 3's single-zone values (`report.md`, `figures/low_load.png`):

| Operation | One zone (exp 3) | Two zones | Why |
|---|---:|---:|---|
| Point read | 2.54 ms | 40.7 ms | one GetPage round trip for the 84% of reads that miss the compute's cache |
| Range read (100 rows) | 7.02 ms | 201 ms | 4.5 page fetches per range, one after another |
| Insert | 1.82 ms | 40.8 ms | the commit waits one round trip for the safekeeper quorum |
| Update | 5.11 ms | 41.6 ms | commit round trip, plus a page fetch for 42% of updates |
| Branch create | 6.68 ms | 84.9 ms | two round trips (new connection per call) + 6.8 ms in the storage controller |
| Compute start | 477 ms | 5,207 ms | first connections read catalog pages one round trip at a time (4.5 s) |

The storage layer's own time stays small (0.58 ms per GetPage, 0.57 ms per WAL flush). The breakdown per bar is
`figures/low_load_breakdown.png` and `figures/breakdown.md`.

## Layout

| Path | Contents |
|---|---|
| `plan.md` | the approved plan |
| `report.md` | results, figures, breakdown table, caveats |
| `REPRODUCE.md` | machines, versions, parameters, commands in order, checks, pitfalls |
| `config/` | compute config template (LFC, statement logging), compute-hook stub |
| `scripts/cluster.env` | node roles, addresses, paths and parameters |
| `scripts/setup_neon_cluster.sh` | deploys the cluster from the coordinator (systemd units, SSD and tmpfs mounts) |
| `scripts/neon_node_agent.py` | per-node agent: computes, CPU counters, 1 s counter sampler, log archive |
| `scripts/neon_ctl.py` | env files, seed, purge, metrics snapshots, clock check, smoke test |
| `scripts/run_low_load.sh` | the measured runs (on the client node b0) |
| `scripts/pull_data.sh`, `sync_client.sh` | copy results back, copy the repo out |
| `scripts/breakdown.py` | per-bar latency breakdown from client rows, compute logs and counter samples |
| `scripts/plot_low_load.py` | `figures/low_load.png`, `figures/low_load_breakdown.png` |
| `scripts/prewarm_probe.py` | probe: LFC settings, `pg_prewarm` speed, first-connection cost on a fresh compute |
| `scripts/summarize.py` | one line per run summary |
| `data/neon_local/data_ops/<OP>/` | client parquet and summary per data operation |
| `data/neon_local/branch_create/` | client parquet and summary of the branch-create run |
| `data/neon_local/metrics/` | pageserver, safekeeper and storage-controller `/metrics` before and after each run |
| `data/neon_local/archive/<node>/` | agent archives: `samples.jsonl`, `starts.jsonl`, `pg/*.log` (compute logs) |
| `data/neon_local/seed.json`, `clock_offsets.json` | seed sizes and timings; agent clock offsets |
| `data/superseded/` | smoke runs and aborted attempts (see below) |
| `logs/` | setup, seed, network probe and driver logs |
| `code/shared_code.patch` | shared-code changes this experiment ran with |

## File formats

- `*.parquet`: one row per operation (`dblib/result.proto`); statement rows (READ/INSERT/UPDATE) carry
  `latency`, `start_time`, `end_time` in seconds on the client's clock.
- `samples.jsonl`: one JSON object per second per node: `t` (agent clock, s), `computes` (timeline ->
  `pc:<neon_perf_counters metric>`, `lfc:<neon_get_lfc_stats key>`, `bp:backpressure_throttling_us`) and
  `services` (label -> selected Prometheus samples of the local pageserver, safekeeper or storage controller).
- `starts.jsonl`: per compute start: `t0`, `t_ready` (agent clock), `start_ms` and compute_ctl's `/metrics.json`.
- `pg/*.log`: compute_ctl output; lines `PG:<UTC time> [pid] user@db LOG:  duration: <ms> ms  statement: ...`.
- `figures/breakdown.{csv,json,md}`: components per bar in ms per operation, with the counters behind them.

## Shared code changes

In `microbench/runner2.py` (with tests in `tests/test_micro_runner2.py`):

1. `WorkerContext.prepare_range_query` sorts the key list once per process instead of on every range read
   (at 6 million keys the per-op sort took seconds; untimed, results unchanged).
2. `DBFORK_PREWARM_SQL`: statements run unrecorded on each measured worker's branch before its warm-up
   (synchronous mode). This experiment uses it to fill the compute's LFC before each data-op run.

## Deviations from the plan

- LFC 448 MB in 8 kB chunks (plan: 512 MB, default chunks), sized from the measured seed so that the cache
  holds the stock primary key plus 15% of the heap; 1 MB chunks would collapse the hit ratio under random reads.
- The LFC is filled before each data-op run (`DBFORK_PREWARM_SQL`, shared change 2): each run's compute starts
  empty, and the runner's key scan runs on another compute (the root branch's).
- 60 warehouses as planned; `count(*)`-style index-only scans proved to fetch one page per round trip, so the
  smoke run was cut to READ and 10 branch creates.
- Per-operation output directories (the parquet name has no operation in it).

## Superseded data

- `data/superseded/seed1_statement_log/`: the first seed attempt, stopped after one minute because the seeding
  compute logged every bulk INSERT (76 MB of log per minute). Its tenant was deleted; the seed was rerun with
  statement logging off on the seeding agent. Kept: the agent archive (gzipped compute log, start metrics, samples).
- `data/superseded/smoke_neon_local/`: the smoke run (READ with a tenth of the operations, 10 branch creates),
  used to test the run wrapper, the prewarm hook and `breakdown.py`. Not analysed in the report.
- The agent archives in `data/neon_local/archive/` also hold the samples and compute logs of the smoke run, the
  smoke test and the probes (`prewarm_probe.py`); `breakdown.py` only reads the timed windows of the full runs.
