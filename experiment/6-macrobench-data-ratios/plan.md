# Plan: data access ratio and data diff ratio of the BranchBench macrobenchmarks

## Request (23 Oct 2026)

> I want you to generate a report in the project root for the macrobenchmarks. I want to define two terms: data
> access ratio, and data diff ratio. Data access ratio defines what percentage of data a branch accesses (read and
> write) compared to the parent branch. Data diff ratio is write only. I want you to do this analysis for all the six
> scenarios.
>
> If static analysis itself does not give you full result. You should run some experiments and figure out, but make
> sure not to run anything on the cloudlab cluster because it is benchmarking.

The report goes to the project root (`macrobench_data_ratios.md`). This directory holds the method, scripts,
measured data and logs. Nothing runs on CloudLab.

## Definitions

- **Unit:** rows, counted as distinct rows. A row read twice counts once.
- **Denominator:** rows in the parent branch at the fork point. This is the spine (or the S1 task branch for a
  rollout) when the branch is created.
- **Data access ratio:** distinct rows the branch reads or writes, divided by the denominator.
  - A statement whose plan scans a whole table counts every row of that table once.
  - A primary-key lookup or range counts the rows it fetches.
  - Rows the branch inserts count, including rows in tables the branch creates.
- **Data diff ratio:** rows that differ between the branch's final state and its fork point (inserted, deleted, or
  changed in value), divided by the denominator.
- **Excluded:**
  - harness invariant checks (they belong to the benchmark, not the agent)
  - branch-verb internals (diff, merge, rebase)
  - the spine's background load

## Method

1. **Static trace.** Read the six scenarios (`macrobench/scenarios/s*_*.py`) and the paper configs
   (`macrobench/configs/*.textproto`), unchanged since base commit `24ab525`. Get the statements each branch runs and
   how many times.
2. **Local Postgres.** Seed one database per scenario on a local Postgres 17.5 (port 55990, never CloudLab), the way
   `macrobench/runner.py` does: the config's schema, `seed_ch`, then the scenario's `seed()`. This gives the exact
   parent sizes.
3. **Plan probe.** Run `EXPLAIN (ANALYZE, FORMAT JSON)` on every statement shape a branch runs, each in a
   rolled-back transaction. Record rows examined per table and whether a plan scans the whole table. Whether a read
   touches 1 row or a whole table depends on the plan, and the CH schema has primary keys only.
4. **S2 measurement.** S2's merges depend on the data (the policy keeps the candidate with the smallest token sum).
   Run S2 at paper scale on the in-memory test backend (`tests/fake_backend.py`), with a fast-forward merge shim.
   Measure every candidate's diff against its fork point.
5. **Combine.** `compute_ratios.py` combines the measured parent sizes, the plan probe, the S2 measurements and the
   per-branch statement counts.

## Assumptions to state

- S3: the parent grows with merged agent subtasks. Round 4 assumes the earlier rounds merged.
- S4 and S6: the spine grows with time-driven load or merged batches. The seed size is used as the parent, which is
  an upper bound for the ratios (and the S6 last batch adds 24 merged batches).
- S5: spine growth comes from the TPC-C transaction mix at the configured commits.
- The plans are Postgres 17 plans. Another engine, or Postgres 18 with skip scan, can change the S6 index reads.
