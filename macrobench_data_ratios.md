# BranchBench macrobenchmarks: data access ratio and data diff ratio

How much of its parent's data does a branch in each of the six BranchBench macrobenchmark scenarios read, and how
much does it change? This report defines two ratios and measures them for every branch type of S1 to S6 at the paper
configurations (`macrobench/configs/*.textproto`). The scenario code is unchanged since base commit `24ab525`.

Method, scripts, data and logs: [`experiment/6-macrobench-data-ratios/`](experiment/6-macrobench-data-ratios/).
Everything ran on a local Postgres 17.5 and the in-memory test backend; nothing ran on CloudLab.

## Definitions

**Data access ratio** = distinct rows the branch reads or writes ÷ rows in the parent branch at the fork point.

**Data diff ratio** = rows that differ between the branch's final state and its fork point (inserted, deleted or
changed in value) ÷ rows in the parent branch at the fork point.

Counting rules:

- **Rows are the unit**, counted once each. A row read 30 times counts once.
- **Reads.** A statement whose plan scans a whole table (Postgres Seq Scan) counts every row of that table. A
  primary-key lookup or range counts the rows it fetches. The CH schema has primary keys only, so whether a filter
  reads 1 row or a whole table depends on whether it matches a key prefix. This was measured with
  `EXPLAIN ANALYZE`, not guessed.
- **Writes.** Rows the branch inserts count for both ratios, including rows in tables the branch creates (they are
  not in the parent, so these ratios can in principle exceed 100%).
- **Parent.** The parent is the spine when the branch is created, or for an S1 rollout, the branch it forks from.
- **Excluded:**
  - harness invariant checks (the benchmark verifying itself, labelled `invariant` in the code)
  - the internals of branch verbs (diff, merge, rebase, revert)
  - the spine's own background load

  Policy reads that are part of the agent's logic count: the S1 scoring checks and the S2 policy check.

## Summary

| Scenario | Branch type (per run) | Parent rows at fork | Data access ratio | Data diff ratio |
|---|---|---:|---:|---:|
| S1 RL environment | task branch (16) | 608,435 | 0.002% to 4.9% (by fault type) | < 0.001% |
| S1 RL environment | rollout (256) | 608,439 | **61% to 62%** | 0.07% to 1.9% (1 to 30 own steps) |
| S2 context management | curation candidate (3 per cycle, 24) | 300 to 1,507 | **30% to 100%** | **8% to 63%** |
| S3 multi-agent | agent branch (128 per round, 512) | 1,280 to ~3,008 | **47% to 50%** | 0.3% to 0.7% |
| S4 dev agent | dev branch (24) | ≥ 608,435 | ≤ 5.0% | ≤ 5.0% |
| S5 ops agent | investigation branch (8) | ~650,000 to ~660,000 | **63% to 64%** | 0.001% |
| S6 data agent | batch branch (32) | 639,241 to 927,241 | 1.7% to 2.5% heap rows (7.2% to 10.1% counting index entries) | 1.5% to 2.2% |

The two ratios tell different stories.

**Diff ratios are small.** In five of the six scenarios a branch changes at most 5% of its parent, often far less
than 1%. That supports the claim that each branch touches a small part of the data, if "touch" means "change". The
exception is S2: its parent is a few hundred to 1,500 rows of conversation, and a curation candidate rewrites or
deletes a large share of it by design.

**Access ratios are large in four scenarios.** S1 rollouts, S2, S3 and S5 read 30% to 100% of their parent. The
reason is reads, not writes:

- S1 and S5 run TPC-C consistency checks that aggregate over six whole tables (order_line alone is 299,394 of the
  608,435 rows).
- S3 filters `task` on a non-key column.
- S2 reads the whole conversation.

With primary keys only, these all become full scans. Only S4, S6 and the S1 task branch read about as little as they
write.

What this means for systems that branch by copy-on-write is in [Implications](#implications).

## S1 RL environment

16 tasks. Each task branch injects one of 5 consistency faults into a fresh copy of the CH data. A group of 16
rollout leaves of up to 30 steps then searches for the repair. Rollouts fork either from the task commit or from an
earlier rollout's commit (`historical_fork_prob: 0.5`), so a rollout runs between 1 and 30 steps of its own. Each step:

- updates 4 adjacent 100-row blocks of `stock` from a random item
- inserts 4 `repair_audit` rows
- runs 4 random consistency checks out of 8
- in half the rollouts, repairs the fault once

Scoring runs all 8 checks on every leaf.

| Branch | Rows accessed | Access ratio | Rows changed | Diff ratio |
|---|---:|---:|---:|---:|
| task branch, fault 1 `district_ytd` | 13 | 0.002% | 4 | < 0.001% |
| task branch, fault 2 `lost_new_order` / 3 `ol_cnt` | 5 | < 0.001% | 4 | < 0.001% |
| task branch, fault 4 `carrier` | 3,003 | 0.49% | 4 | < 0.001% |
| task branch, fault 5 `history` | 30,003 | 4.93% | 4 | < 0.001% |
| rollout, 1 own step | 368,811 | 60.6% | ~406 | 0.07% |
| rollout, 15 own steps | 374,308 | 61.5% | ~5,900 | 0.97% |
| rollout, 30 own steps | 379,867 | 62.4% | ~11,460 | 1.88% |

**Task branches.** A task branch changes 4 rows: the fault, 2 sentinel `item` rows and one `rl_task` row. Fault 1
reads all 10 `district` rows, because the planner prefers a Seq Scan over a 10-row table. Fault 4 reads one
district's 3,000 orders through a bitmap scan. Fault 5 scans all of `history`, which has no primary key.

**Rollouts.** Access is dominated by the checks:

- C3, C4, C5, C7, C8 and C9 scan `orders`, `order_line`, `new_order` and `history` in full, plus the small
  `warehouse` and `district` tables: 368,405 distinct rows.
- C7 reads `orders` 3 times and C9 reads `history` 10 times per run, but the distinct count is what matters here.

The diff grows with the steps: 30 steps of 400 random adjacent `stock` rows cover about 11,300 of the 100,000
`stock` rows, which is 11% of that table.

## S2 context management

The spine is a conversation: 200 `turn` and 100 `tool_call` rows per 25-step cycle. At each of 8 compactions, three
candidate branches each try one curation strategy:

| Candidate | Strategy | What it does |
|---|---|---|
| A | summarize | rewrites old turns |
| B | reference | replaces large tool results by references |
| C | dedup | deletes duplicate turns |

Each candidate adds a `note`, then reads its curated context through a view. A cross-branch policy check picks the
candidate with the smallest token sum and the spine fast-forwards to it.

Which candidate wins depends on the data, and the parent of later cycles depends on which ones won. So S2 was **run
at paper scale** on the in-memory test backend (`tests/fake_backend.py`), with a fast-forward merge added. Every
candidate's diff was measured against its fork point by primary key.

| Cycle | Parent rows | summarize: access / diff | reference: access / diff | dedup: access / diff | Merged |
|---:|---:|---:|---:|---:|---|
| 1 | 300 | 67% / 62% | 100% / 31% | 67% / 50% | summarize |
| 2 | 602 | 67% / 34% | 100% / 32% | 67% / 57% | summarize |
| 3 | 904 | 67% / 22% | 100% / 32% | 66% / 60% | reference (forced) |
| 4 | 1,205 | 67% / 33% | 100% / 8% | 66% / 59% | summarize |
| 5 | 1,507 | 66% / 13% | 100% / 13% | 66% / 63% | dedup |
| 6 | 866 | 30% / 28% | 99% / 15% | 30% / 20% | summarize |
| 7 | 1,168 | 39% / 17% | 99% / 19% | 39% / 34% | summarize |
| 8 | 1,470 | 45% / 14% | 99% / 22% | 45% / 41% | dedup |

**Access.**
- Every candidate reads all of `turn`: the policy check and the curated-context view sum its token counts.
- Reference also scans `tool_call`, so it reads its whole parent (99% to 100%; the rest is earlier notes, which
  nothing reads).
- Summarize and dedup read `turn`, which is two thirds of the parent until cycle 5. The dedup merged at cycle 5
  shrank `turn` to 258 rows, so their access drops to 30% at cycle 6.

**Diff.**
- Summarize changes 184 to 400 old turns per cycle (400 at cycle 4, after the forced reference win left cycle 3's
  turns unsummarized). Its statement writes 186 to 986 rows; the rest are no-op updates of turns already summarized,
  which the diff does not count.
- Reference changes 92 to 325 `tool_call` rows.
- Dedup deletes 148 to 942 turns.

**Cycle 3.** The config's `rejected_cycle: 3` makes the scenario pick the second finished candidate at cycle 3
instead of the policy winner, so it can reject it at the end of the block.

**End of the run.** The run stops at that rejection (cycle 8), where the scenario reverts the compaction. The test
backend has no revert. All 24 candidates had already been measured.

## S3 multi-agent

128 agents work on a shared task graph (641 `task` and 639 `dependency` rows) for 4 rounds, each on its own branch.

An agent branch:
- updates up to 4 tasks by key and the sentinel task 0
- inserts 2 subtasks and 2 to 3 dependency edges
- runs a 4-times-repeated read of its assigned tasks: `WHERE assignee = ...`

`assignee` has no index, so that read scans `task` in full. Everything else is a key lookup.

| Branch | Parent rows | Access ratio | Rows changed | Diff ratio |
|---|---:|---:|---:|---:|
| agent branch, round 1 | 1,280 | 50.4% | ~9.5 | 0.74% |
| agent branch, round 4 | ~3,008 | 47.0% | ~9.5 | 0.32% |

The round-4 parent assumes that the 384 agents of rounds 1 to 3 merged their subtasks and edges. `task` is about
half of the parent, hence the access ratio of about 50%.

## S4 dev agent

24 dev branches each ship a schema migration on the customer table.
- **The migration:** add `c_tier`, then backfill it in a deterministic sweep of 200-row key ranges (174 UPDATE
  statements), so every one of the 30,000 customers is rewritten.
- **New tables:** it creates `loyalty_event` and `loyalty_tier` (177 rows in all).
- **Reads:** at every step it reads a tier histogram (`GROUP BY c_tier`, a full `customer` scan, 29 times).

| Branch | Parent rows | Rows accessed | Access ratio | Rows changed | Diff ratio |
|---|---:|---:|---:|---:|---:|
| dev branch | 608,435 (seed) | 30,178 | 4.96% | 30,177 | 4.96% |

Access and diff are the same set: the branch reads what it rewrites.

The 5% is a whole table. The diff covers 100% of `customer` and none of the other tables.

Background load on the spine (8 clients) grows the parent while the branches work, so the seed-size parent makes
these upper bounds.

## S5 ops agent

The spine runs a TPC-C-style load with a commit every 20 transactions. A fault lands at commit 460, and 8
investigation branches fork from historical commits around commits 399 to 498 to find it.

An investigation branch:
- runs all 8 consistency checks, which scan the same six tables as S1
- reads `deploy_log`
- writes 8 rows into an `incident_finding` table it creates

| Branch | Parent rows (model) | Access ratio | Rows changed | Diff ratio |
|---|---:|---:|---:|---:|
| investigation branch at spine commit 399 | ~649,700 | 63.1% | 8 | 0.001% |
| investigation branch at spine commit 498 | ~659,900 | 63.6% | 8 | 0.001% |

**Parent model.** The parent is the seed (608,435 rows) plus the spine's growth up to the fork commit. The growth
model is 20 transactions per commit:
- 45% new orders of about 10 lines
- 43% payments that add a `history` row
- 4% deliveries that remove `new_order` rows

The growth falls into the scanned tables, so the access ratio rises slightly with the fork commit.

## S6 data agent

32 ingestion batches, 8 at a time, each on its own branch.

A batch:
- loads 2,000 orders and 10,000 order lines in 10 steps
- looks up each order's customer by key
- upserts the derived tables:
  - `customer_summary`: about 1,935 distinct customers per batch, all already present, so all updates
  - `daily_revenue`: up to 4 days
  - `nation_sales`: up to 100 rows
- writes a manifest row
- after each step, runs an integrity check per touched day

The branch is reset once to an earlier commit and the lost steps are redone. It is then rebased and merged.

| Branch | Parent rows | Heap rows accessed | Access ratio (heap) | Access ratio (with index entries) | Rows changed | Diff ratio |
|---|---:|---:|---:|---:|---:|---:|
| batch branch, first 8 | 639,241 | ~16,006 | 2.50% | 7.2% | ~14,040 | 2.20% |
| batch branch, last | 927,241 | ~16,006 | 1.73% | 10.1% | ~14,040 | 1.51% |

The batch reads about 1,935 customers, all 31 `daily_revenue` rows (a Seq Scan at that size) and the rows it writes.

**The two access ratios.** The integrity check (`o_w_id = ? AND o_id BETWEEN ...`) and the per-batch order count
(`o_id` range only) do not constrain the second primary-key column `o_d_id`. Postgres 17 therefore walks the whole
`orders_pkey` index: 83 to 84 of its 84 pages, measured with `EXPLAIN (ANALYZE, BUFFERS)`. It fetches no heap rows
except the batch's own. Counting those index entries as access adds every order in the parent (30,000 at the start,
78,000 for the last batch). Either way the batch's footprint is small.

**Parent size.** The parent grows by 12,000 rows per merged batch. The last batch assumes 24 batches merged before
it forks.

**Reset.** Steps redone after the reset draw new random customers, which can add up to a few hundred distinct
customer reads, less than 0.1 percentage points.

## How the numbers were obtained

1. **Static trace** of every scenario's branch work from `macrobench/scenarios/s*_*.py` and the paper configs:
   statements, how often they run, and which rows they address.
2. **Seeded parents.** Each scenario's database was seeded on a local Postgres 17.5 (port 55990) exactly as
   `macrobench/runner.py` does: the config's schema, `seed_ch` with W=1 and seed 42, then the scenario's `seed()`.
   - S1, S4 and S5: 608,435 rows
   - S6: 639,241 rows (with its derived tables)
   - S3: 1,280 rows
   - S2: empty
3. **Plan probe.** `EXPLAIN (ANALYZE, FORMAT JSON)` ran on every statement shape a branch runs, each in a rolled-back
   transaction, recording rows examined per table and the scan type. Probe output:
   [`logs/plan_probe.out`](experiment/6-macrobench-data-ratios/logs/plan_probe.out).
4. **S2 run** at paper scale on the test backend, with diffs per candidate.
5. **Combination.** `compute_ratios.py` combines these into the tables above. Full output:
   [`logs/compute_ratios.out`](experiment/6-macrobench-data-ratios/logs/compute_ratios.out).

## Caveats

- **Plan dependence.** The access ratios of S1, S3 and S5 come from full scans that the schema forces, because it
  has no secondary indexes.
  - An index on `task.assignee` would cut S3 to about 1%.
  - Postgres 18's skip scan, or Dolt's and Neon's own planners, could change the S6 index walk and the small-table
    Seq Scans (10 `district` rows, 31 `daily_revenue` rows).
  - The plans are Postgres 17's. S2's plans are sqlite's, but S2's reads are whole-table by construction.
- **Rows, not bytes.** An `order_line` row and a `warehouse` row count the same. Byte- or page-weighted ratios would
  give somewhat different numbers, and copy-on-write systems work in pages.
- **Growing parents.** S3 (merged subtasks), S5 (spine load) and S6 (merged batches) use modelled growth. S4 uses
  the seed size, an upper bound for both ratios.
- **Excluded work.**
  - Harness invariants re-read large parts of the data in several scenarios.
  - Branch verbs (diff, merge, rebase) read the changed rows, or more, depending on the engine.
  - Neither is in the ratios.
- **One configuration.** These are the paper configurations at CH W=1. A larger warehouse count grows the full
  scans in step with the parent, so the full-scan access ratios stay roughly constant. The ratios of key-addressed
  work (S1 task branches, S4, S6) shrink as the parent grows.

## Implications

On systems that branch by copy-on-write (Dolt's prolly trees, Neon's pageserver layers), a new branch shares all of
its parent's storage.

**Diff ratio drives branch storage and merge cost.** It is small everywhere except S2, so branches stay cheap to
keep and merge.

**Access ratio drives a different cost: data a fresh branch must bring into its own cache.** On Neon every branch
has its own compute with a cold cache. In four of the six scenarios a branch reads half or more of its parent, so it
pulls most of the parent's pages from the pageserver before it reaches a steady state, even though it changes very
little.
