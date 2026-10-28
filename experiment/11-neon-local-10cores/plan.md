# Experiment 11 plan: experiment 10 on experiment 7's hardware setup

## Request (26 Oct 2026)

"I want you to rerun experiment 10 with 10 cores so that it matches the hardware setup of doltgres."

Context: experiment 10 gave Neon all 24 cores of NUMA node 2; experiment 7 gave Doltgres 10. Experiment 10's branch
creation runs never saturated Neon (about 13 of 24 cores busy), while Doltgres's creates slowed by orders of
magnitude once its 10 cores were full. The question is how the two scale with the same hardware: whether Neon's
creates also degrade when its CPU is full, and where its data operations stop on 10 cores.

## Decisions (asked 26 Oct 2026)

| Question | Answer |
|---|---|
| Workload | experiment 7's, plus writes: W=1, experiment 7's read op counts, read-only busy branches for branch creation (experiment 7) and also 80/20 (experiments 2/5/10), insert/update scaling with experiment 5's counts |
| Cache | the experiment 10 rule: no LFC, shared_buffers = 30% of stock + stock_pkey = 11 MB at W=1, prewarmed (key + first 1,062 heap pages) |
| Placement | experiment 7's exactly: Neon on CPUs 2,6,...,38 (10 cores of node 2), memory node 2; clients on the other 14 cores of node 2 plus all 24 of node 3 (38 cores), memory nodes 2 and 3 |
| Scope | all parts, Unix sockets only (experiment 10 already measured TCP) |

## Method

Same binaries as experiment 10 (fork branch `local-uds`, snapshot hashes in `code/bin_sha256.txt`), same scripts
adapted (`scripts/`), own state in `/mydata/jiyu/neon-exp11` (experiment 10's state is left untouched).

| Part | What | Notes |
|---|---|---|
| A low load | one busy branch, one client: READ 40,000, RANGE_READ 10,000, INSERT 6,000, UPDATE 6,000; 100 branch creates with one MIXED busy branch | experiment 10's counts |
| B scaling | N = 1, 4, 16, 64, 128, 256, 512, benchmark runner, one client per branch, up to 38 client processes | reads: experiment 7's `min(4000, max(50, ceil(200000/N)))` and `min(1000, max(10, ceil(50000/N)))`; writes: experiment 5's |
| B ceiling | same N, one pgbench per branch, 30 s, point / range / insert once, update three times | `ceiling_pgbench.py`, exactly N computes per point |
| C branch create | 100 creates (every 5th connected) with N = 1..512 busy branches, background READ, then MIXED | runner, as experiments 7 and 10 |
| C' create under pgbench | the same creates while N = 1, 16, 64, 256, 512 pgbench clients run point reads (CPU-saturating) or updates (fsync-saturating) | `create_under_pgbench.py`, new: guarantees saturation |

N = 1,024 (experiment 7's largest) does not fit: each running compute holds about 45-77 MB of node-2 memory
(measured again here at 11 MB shared buffers); 512 is the largest N, plus 1,024 only if the measured per-compute
memory predicts a fit with 4 GB to spare.

## Analysis

- Throughput and latency against N for Neon (10 cores), Neon (24 cores, experiment 10) and Doltgres (10 cores,
  experiment 7), point and range; insert and update for Neon.
- CPU per process group at each point, and per-core throughput.
- Branch-create latency against N and against the server's busy cores, for Neon READ / MIXED / pgbench backgrounds and
  Doltgres READ / MIXED: the comparison the request is about.
- Report in `report.md`; README with the verdict; REPRODUCE.

## Expected duration

About 1 hour of setup and smoke tests, 2-2.5 hours of runs, 1 hour of analysis and writing.


---

# Addendum, 27 Oct 2026 (approved plan, as written before execution)


## Context

Experiment 11 compared Neon with Doltgres on the same 10 server cores. It took Doltgres's numbers from experiment 7,
which ran reads only, so the low-contention (A) and scaling (B) tables have no Doltgres inserts or updates. Its
branch-creation result is also unexplained. With read-only busy branches and the server's CPU full, Neon's create
median rises 3-4x (2.1 to 4-9 ms), while Doltgres's rises 10x at N = 64, 65x at 256 and 5,000x at 1,024.
Experiment 7 showed the Doltgres slowdown needs a full CPU (C1/D1: the same reads on 20 cores give 1.8 ms). It did
not find why a create suffers far more than a read, and experiment 11 only guessed (report section C: "likely reason
(not measured)").

This addendum does two things:

1. **Doltgres writes.** Add Doltgres INSERT and UPDATE points to A and B with experiment 7's setup.
2. **Explain the creation gap.** Work out from source why Doltgres's creation degrades more than Neon's, then verify it
   with experiments on both systems.

Everything goes into `experiment/11-neon-local-10cores/`. No shared code changes.

User decisions (27 Oct 2026):
- Neon side: measure and intervene.
- Write sweep: experiment 7's N (1, 4, 16, 64, 256, 1,024). The N = 1 point is also the low-contention point, as it
  is for reads.

## What the source says (Doltgres v1.4.0, Dolt `8689322abc2b`, GMS `0b7903b32e6c`)

**The create path.** `SELECT dolt_branch(new, 'main')` runs on the connection's goroutine. Doltgres has one goroutine
per connection and no worker pool (`server/listener.go:89`). The create:
- scans every dataset for a case conflict (`doltdb.go:1586`), the one O(branches) step;
- does two optimistic `datas.update` commits (`SetHead`, then `UpdateWorkingSet`). Each holds the per-database
  `NomsBlockStore.mu` exclusively while it writes the chunk journal and fsyncs it (`store.go:1611-1760`,
  `journal_writer.go:532`). That is 2 fsyncs under `nbs.mu`, plus `journalWriter.lock` in write mode;
- updates branch control (global `Access` RWMutex). This writes a file only if a branch-control path is set; our
  config sets none, which the smoke run confirms.

**What the reads share with it.** A read's autocommit transaction start takes `nbs.mu` in read mode (`Root`). Every
node-cache miss takes `nbs.mu` **exclusively** (`store.go:1146`), then `journalWriter.lock` in read mode across a
`pread` (`journal_writer.go:451`). Readers hold no lock during per-row work.

**Hypotheses.** These are what the experiments must separate.
- **H1, Go run queue.** A create blocks many times: two fsyncs, lock waits, syscalls. After each block it goes back
  into a Go run queue behind about N runnable session goroutines on 10 Ps, and Go has no wake-up priority. Create
  latency is then about (blocking points) × (queue wait), and the queue wait grows with N.
- **H2, lock-holder delay.** The create's `nbs.mu` and journal-lock acquisitions wait for readers that hold those
  locks while they themselves wait in the run queue. Dirty branches (100 uncommitted updates each) miss the shared
  node cache more often, so readers take these locks more often. That would explain D being 15x slower than C at
  N = 1,024 only when the CPU is full.
- **H3, more work.** The create's own CPU time grows with N (the dataset scan, branch control) or with GC assists.
- **Neon's create** runs in separate processes (storage controller, pageserver management). Each was started with
  `setsid`, so with `sched_autogroup_enabled = 1` each is its own kernel autogroup. Each compute is its own autogroup
  as well (`start_new_session=True`). The kernel's CFS gives a waking, mostly idle process a core within a scheduling
  period, so the read load does not queue ahead of it.

## Part 1: Doltgres inserts and updates (A, B)

**Scripts.** New directory `scripts/dolt/`, copied from experiment 7 and adapted:
- `env.sh`, `doltgres_server.sh`, `cpu_agent.py` (experiment 7's `/proc/stat` method), `run_busy_branches.sh`.
- `ROOT=/mydata/jiyu/doltgres-exp11`: experiment 7's binary copied with its sha256 recorded, the same `config.yaml`
  (port 5433), and a fresh `ch_w1_seed` built on the first run. Experiment 7's state stays untouched.
- `EXP` = experiment 11; the default ops are `INSERT UPDATE`; output goes to `data/dolt/`.

**Parameters** (unchanged from experiments 2 and 7):
- placement: server on CPUs 2,6,…,38 with `GOMAXPROCS=10` and `--membind=2`; clients on the other 38 cores;
- `--mode sync --warmup-fraction 0.1 --table-name stock --updates-per-branch 100 --seed-db ch_w1_seed`;
- one client per branch, at most 38 processes;
- write counts: experiment 7 used 5x experiment 2's read counts, so writes get 5x experiment 2's write count. That is
  30,000 per run, at least 20 and at most 6,000 per client.

**Runs:**
- smoke: N = 1, 16 at a tenth of the counts, written to `data/superseded/smoke_dolt_writes`;
- then the full sweep, N = 1, 4, 16, 64, 256, 1,024 × {INSERT, UPDATE}. Doltgres serializes writes at about 650/s
  (experiment 2), so this takes about 30-40 min, mostly setting up the 1,024 branches.

**Pass checks** (experiment 7's `check()`): `failed_ops == 0`; `server_cpu_cores` and `remote_cpu.server` present;
`num_processes == min(N, 38)`. A guard aborts if any Neon service holds the server CPUs.

**Analysis.** `scripts/analyze.py` gets a loader for `data/dolt/data_ops`. Doltgres INSERT and UPDATE are added to:
- `low_load.png` and the A table (N = 1);
- `data_ops.png` and `data_ops.md`;
- `cpu_sides.md`, with the per-point verdict: server CPU, client CPU, or neither.

The expected verdict is "neither: serialized commit path", tied to the `nbs.mu`-across-fsync finding above.

## Part 2: the branch-creation analysis

### T: Go execution trace of creates under saturating reads (Doltgres)

**Tooling.** Doltgres has `--prof trace --prof-path DIR` (`cmd/doltgres/main.go:66-189`). It traces from start to
exit, and the trace is written on SIGTERM. No metrics or pprof endpoint serves the trace, so the server is
restarted for each traced run.
- `doltgres_server.sh start` gains `--trace DIR` and `--godebug` (`GODEBUG=schedtrace=1000,gctrace=1`: run-queue
  lengths and GC cycles every second, to the server log).
- Driver `scripts/dolt/create_under_reads.py`: experiment 7's follow-up driver plus an `--op update` reader mode.
- Wrapper `scripts/dolt/run_create_trace.sh` runs: set up the branches once; restart the server with the trace on;
  start the readers; warm up 10 s; make 30-100 creates; stop the readers; stop the server so the trace is written.
- Each point is also run once **untraced**, to check that tracing does not change the create and read numbers
  (tolerance about 20%).

**Points.** All on 10 server cores, readers back to back unless marked:

| point | branches | busy N |
|---|---|---|
| D64, D256, D1024 | dirty (the benchmark's branches) | 64, 256, 1,024 |
| C1024 | plain | 1,024 |
| D1024-half | dirty | 1,024, readers paced to half the rate (spare-CPU control, as experiment 7's D2) |
| U64 | dirty | 64, readers doing updates, no creates (explains the write limit with the same tool) |

About 1 h 15 min.

**Analyzer.** `scripts/dolt/gotrace/` is a small Go program on `golang.org/x/exp/trace`, built with the Go 1.26
toolchain because the binary is go1.26.8.
- It finds the create goroutine (the stack contains `dprocedures.doDoltBranch` / `actions.CreateBranch`) and splits
  each create statement into: running; runnable waiting (count and total); blocked on sync, named by the top Dolt
  frame (`nbs.mu`, journal lock, other); syscall (fsync); GC assist; other.
- For sync blocks it also records the goroutine that unblocked the create and how long that goroutine had been
  runnable, which tests H2.
- It does the same for a sample of read goroutines and for update goroutines (U64).
- Output: `data/dolt/create_trace/<point>/breakdown.csv` and `summary.json`. Raw traces (likely GBs) stay in
  `/mydata/jiyu/doltgres-exp11/traces/`, listed in the README with sizes and sha256.
- Fallback if `x/exp/trace` cannot read go1.26 traces: `go1.26 tool trace -pprof=sched|sync|syscall`, with the
  goroutine analysis restricted to the create goroutine.

### H: CPU-hog control (Doltgres)

Is it the full CPU, or the full Go run queue?
- `ch_w1_seed` keeps the same 1,024 dirty branches, so the create's own work is unchanged, but there are no readers.
- `scripts/dolt/cpu_hog.c` (pthreads spin loops, built with gcc) runs K threads pinned to the 10 server cores.
- K = 10, 128 and 1,024, in two placements:
  - its own session, a separate autogroup like Neon's processes;
  - the server's session (`doltgres_server.sh start --hog K` launches it inside the server's `setsid`), the same
    autogroup, so it competes thread for thread.
- 100 creates each, plus a no-hog baseline. The server cores' busy time, and the Doltgres threads' kernel run-queue
  wait from `schedstat`, are recorded.
- One run (K = 1,024, same session) is also traced.
- About 20 min.

**Reading T and H together:**
- **H1 confirmed:** in T, create latency is mostly runnable waiting. The number of waits per create times the read
  goroutines' per-wake-up wait reproduces the latency. The per-wake-up wait grows with N, matching `schedtrace`'s
  queue lengths. In H, creates stay at a few ms with the cores 100% busy.
- **H2 confirmed:** sync blocks on `nbs.mu` or the journal lock dominate, their unblockers had been runnable for most
  of the wait, and this share is larger in D1024 than in C1024.
- **H3 confirmed:** running time per create grows with N or with GC assists.
- **Plain CPU contention suffices:** if H slows creates as much as reads do, the "full CPU" explanation stands.
  Neon's advantage would then come from fewer blocking points, compared from the counts.

### N: Neon, measure and intervene

- Restart Neon on its 10 cores (`neon_services.sh start --transport uds`, `neon_ctl.py env-init`). Doltgres must be
  stopped; the drivers check this.
- `create_under_pgbench.py` gains `--sched`. It takes `sched_probe.groups()` snapshots at the start and end of the
  timed creates and writes, per process group, the run time and run-queue wait per create. Where the services log
  request durations, it also splits each create between the storage controller and the pageserver (checked in the
  smoke run; best effort).
- Runs: point-read pgbench at N = 64, 256 and 512, with 100 creates each, against experiment 11's C' numbers.
- Intervention: at N = 256, the storage controller's autogroup is set to nice +19 (`echo 19 > /proc/<pid>/autogroup`;
  allowed for the owner, and reset afterwards). A second run nices the storage controller and the pageserver; it
  slows reads too, which is reported as a confound.
- Prediction: the run-queue wait per create stays at a few ms at full CPU, and the niced runs push the creates up.
  That would show Neon's protection comes from kernel scheduling of separate processes.
- About 1 h, including restarts and memory checks (the N = 512 guard from experiment 11).

## Order of work (about 4.5 h of machine time, plus analysis)

| step | what | time |
|---|---|---|
| 0 | Exclusive machine reservation with the `machine-timeshare` skill; check that Neon is stopped and node-2 memory is free | 5 min |
| 1 | Set up `scripts/dolt/` and `/mydata/jiyu/doltgres-exp11`; append this plan to `plan.md` as "Addendum, 27 Oct 2026" | 15 min |
| 2 | Part 1: smoke run, then the full write sweep | 45 min |
| 3 | T: build the analyzer, trace smoke (D64, 10 creates), check that the create goroutine is found; then all points | 1.5 h |
| 4 | H: hog runs | 20 min |
| 5 | Stop Doltgres; N: smoke, runs, intervention; stop Neon | 1 h |
| 6 | Release the reservation | |
| 7 | Analysis, figures and report sections | |

## Deliverables in `experiment/11-neon-local-10cores/`

- **data:**
  - `data/dolt/data_ops/` plus `cpu/`, `logs/`, `progress.log`;
  - `data/dolt/create_trace/<point>/`, `data/dolt/create_hog/`;
  - `data/neon_local/create_sched/`;
  - `data/superseded/` for the smoke runs.
- **figures:**
  - `low_load`, `data_ops` and `cpu_sides` with the Doltgres writes;
  - new `create_breakdown.png` (where each create's time goes: Doltgres T and H points against Neon N points) and
    `create_vs_runqueue.png`, plus `.csv`/`.md` tables. The plotting follows the `dataviz` skill.
- **report.md:**
  - Doltgres writes in A and B and the CPU-sides table;
  - a new section "D. Why Doltgres's branch creation degrades more than Neon's" covering the source path,
    hypotheses, T, H and N results, and a verdict that states what was and was not shown;
  - section C's "likely reason (not measured)" paragraph and the summary items updated.
- **README.md:** verdict, layout and file formats. **REPRODUCE.md:** the new steps, durations, pitfalls (trace size,
  the Go toolchain, autogroup nice reset, never `pkill -f` a pattern in the caller's command line).
- **code/:** `doltgres_sources.txt` (Doltgres v1.4.0 and the Dolt/GMS pseudo-versions cited); `bin_sha256.txt`
  updated. Shared code stays unchanged.
- Memory notes updated at the end. Nothing is committed unless asked.

## Verification

- **Part 1:** every run DONE in `progress.log`, with no CHECK-FAIL.
  - The N = 1 update latency is in the expected range: about 2-4 ms (experiment 2 measured 4.2 ms on older hardware).
  - The throughput plateau is at or below a few thousand per second, with the server cores well under 9: the
    serialized commit path.
  - `analyze.py` runs cleanly and the new rows appear in `figures/*.md`.
- **T:**
  - Untraced and traced runs agree within about 20%.
  - The untraced D1024 / D256 / D64 medians land near experiment 7's 6.7 s / 88 ms / 13.6 ms.
  - The analyzer's per-create components add up to the client-measured create latency within 10%.
- **H:** the server cores are at least 97% busy during the hog runs, and the no-hog baseline is about 1.3-1.8 ms.
- **N:** the non-niced runs reproduce experiment 11's C' numbers within the run-to-run noise (10-15%), and the nice
  value is reset afterwards (read back from `/proc/<pid>/autogroup`).
