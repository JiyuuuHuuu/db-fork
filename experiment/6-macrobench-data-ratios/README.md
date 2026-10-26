# Experiment 6: data access ratio and data diff ratio of the BranchBench macrobenchmarks

What share of its parent's rows does a branch in each macrobench scenario (S1 to S6) read or write (data access
ratio), and what share does it change (data diff ratio)?

**Report:** [`macrobench_data_ratios.md`](../../macrobench_data_ratios.md) in the project root, as requested.

| File | Contents |
|---|---|
| [plan.md](plan.md) | request, definitions, method, assumptions |
| [REPRODUCE.md](REPRODUCE.md) | commands, local Postgres setup |
| `scripts/pgsuite.py` | minimal Postgres `DBToolSuite` for the local server |
| `scripts/pg_seed.py` | seeds `bb_<scenario>` as `macrobench/runner.py` does, prints row counts |
| `scripts/plan_probe.py` | `EXPLAIN ANALYZE` of every branch statement shape → `data/plan_probe.json` |
| `scripts/s2_measure.py` | S2 at paper scale on the test backend → `data/s2_candidates.json` |
| `scripts/compute_ratios.py` | combines the above → `data/ratios.json`, Markdown table |
| `logs/` | seed counts, probe output, ratio table |

Nothing ran on CloudLab. Everything ran on the development machine:
- a local Postgres 17.5 (`/mydata/jiyu/bb-ratio-pg`, port 55990)
- the in-memory sqlite test backend

The scenario code analysed is unchanged since base commit `24ab525`. This experiment changes no shared code.
