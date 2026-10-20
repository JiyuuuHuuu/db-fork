# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A benchmarking framework for branchable databases (Dolt, Neon, Xata, SeekDB, MatrixOne, Bauplan, Chronos, TerminusDB, plain Postgres file copies). Every backend is driven through one git-like API in `dblib/db_api.py`; macrobenchmarks (six agent-workflow scenarios) and microbenchmarks (latency, throughput, per-verb cost) sit on top of it. README.md is long and authoritative on CLI flags, per-backend setup, env vars and output formats; consult it before guessing.

## Commands

All commands run from the repo root. Python always goes through `uv run` (the `.venv` is managed by uv; Python 3.13 pinned in `.python-version`).

```bash
uv sync --group dev                      # install deps + pytest; also compiles .proto -> *_pb2.py (needs protoc)
uv run python build_protos.py            # recompile protos by hand (the *_pb2.py files are gitignored)
uv add <package>                         # add a dependency; commit uv.lock

uv run pytest tests/                                   # unit tests (fake sqlite backend, no server)
uv run pytest tests/test_db_api.py -k test_ref_parse   # one test
uv run pytest tests/test_seekdb_live.py                # *_live.py tests self-skip unless that server/key is reachable

./scripts/run_macrobench.sh --mini --outdir run_stats rl_env dolt     # smoke-test a scenario; always start with --mini
uv run python -m macrobench.runner --config macrobench/configs/ops_agent_mini.textproto --outdir run_stats
./scripts/run_single_thread_bench.sh dolt db_setup/tpcc_schema.sql 16  # micro latency
./scripts/run_throughput_bench.sh dolt db_setup/ch-w1.sql --sweep-proportional
uv run python -m microbench.verbs --backend dolt --scale 1 --outdir run_stats/micro_verbs/dolt
uv run python scripts/plotting/plot_macrobench.py --data-dir run_stats --outdir figures/macro
```

There is no linter or formatter configured. Backend servers are never started by the single-run scripts; `scripts/run_macro_all.sh` is the only one that manages servers. Backend credentials and hosts come from a gitignored `.env` (see README "Environment Variables"); the shell scripts export it and several adapters call `load_dotenv()` themselves. `DBFORK_SERVER_HOST` / `DBFORK_SERVER_SSH` point every local adapter at a remote server machine and make storage measurements run `du`/`df` over ssh (`dblib/util.py`).

## Experiments

Rule: every experiment keeps all of its data and files in its own directory, `experiment/<n>-<short-name>/`, numbered in the order experiments are started (`experiment/1-dolt-branch-scaling/` is the first; use it as the template). Never leave an experiment's results in `run_stats/` or `figures/`, its scripts in the shared `scripts/` tree, or its report only in a scratchpad.

- Create the directory before the first run, with this layout: `README.md` (question, verdict, layout, file formats), `plan.md` (the approved plan), `REPRODUCE.md`, `report.md`, `figures/`, `data/<backend>/` (one subdirectory per phase), `logs/`, `scripts/`, and `code/`.
- `REPRODUCE.md` must let someone rerun the experiment later without this conversation: machines and hardware, server versions and configs, the code version, every parameter, the commands in order with expected durations, pass checks, how to collect and plot, cleanup, and known pitfalls.
- Record the exact code the experiment ran with: the commit hash, or, if shared-code changes are uncommitted, `code/shared_code.patch` (`git diff` against the base commit, untracked files included) checked with `git apply --check`.
- Reports are Markdown: `report.md` in the experiment directory, with figures embedded as relative links (`![…](figures/<name>.png)`). Do not write HTML reports or publish reports as web pages.
- Experiment-specific scripts (drivers, calibration, plotting, node setup, one-off probes) go in its `scripts/`, resolve the repo root from their own location, and write to the experiment's `data/` by default.
- Reusable benchmark code stays in the shared tree (`microbench/`, `macrobench/`, `dblib/`, `util/`, `scripts/`). List the shared changes an experiment depends on in its README.
- Pull everything produced on remote machines (results, smoke tests, aborted attempts, logs) back into the directory. Keep data that was not analysed under `data/superseded/` with a note in the README saying why.
- Remote machines used by experiments are described in `.agent/cloudlab.md`.

## Architecture

### The API layer (`dblib/`)

`DBToolSuite` (`dblib/db_api.py`) is the abstract base every backend subclasses, one instance per worker thread. The design rule that everything else depends on:

- **Public verbs** (`branch`, `commit`, `diff`, `schema_diff`, `log`, `merge`, `rebase`, `revert`, `reset`, `delete`, `gc`, `exec`, `exec_async`) live only in the base class. They wrap the call in `_run_verb`: timing, optional storage measurement (`_storage_bytes` before/after), cancellation checks, and emitting a row to the `ResultCollector`. They never raise for a failed op; they return an `OpResult` with `status` OK/FAILED/UNSUPPORTED.
- **Backends override protected hooks** (`_branch_impl`, `_commit_impl`, ..., `_connect_impl`, `_storage_bytes`) and must not time themselves. A hook that is not overridden makes `supports(verb)` false and the verb returns UNSUPPORTED. Class attributes declare the rest: `SUPPORTS_COMMIT_REFS` (`branch@commit` refs), `SUPPORTS_MULTI_REF_EXEC` (one session addressing several branches), `STORAGE_SCOPE`, `SCHEMA_DIALECT` (enables the catalog-based `schema_diff` simulation), and `IMPLEMENTATION` (which verbs are `"simulated"` rather than native). With `DBFORK_NATIVE_ONLY=1` every simulated verb behaves as unsupported.
- **`exec()`** runs a workload script (SQL list, Python source defining `run(db)`, or a callable) on refs, either `per_ref` or `multi`. Each `db.sql()` statement is recorded as its own row; one EXEC row and possibly a CONNECT row (connection switched to the branch) are recorded per ref, all sharing an `exec_id`.
- Shared helpers: `mysql_common.py` (Dolt-over-MySQL and SeekDB/MatrixOne), `pg_merge.py` (`PgMergeMixin`: simulated merge/rebase for Postgres-family backends such as Neon and file_copy).

`ResultCollector` (`dblib/result_collector.py`) is thread-aware (per-thread context: table, step, thread id), spills rows to parquet, tracks in-flight ops for the stall watchdog, and computes `workflow_supported` / `support_summary`. Row schema is `dblib/result.proto`.

### Adding or changing a backend

The backend enum is duplicated in `macrobench/task.proto` and `microbench/task2.proto` with the same numbering, and the enum-to-class mapping is repeated in several places that must stay in sync: `create_db_tools` in `microbench/runner2.py` (shared by both runners), `_capabilities_for` and `_implementation_for` in `macrobench/runner.py`, the `VALID_BACKENDS` lists in `scripts/run_*.sh`, and the backend lists in `scripts/run_macro_all.sh` / `run_micro_verbs.sh`. Each backend also exposes `init_for_bench(result_collector, db_name, default_branch, measure_storage)` plus module-level `setup_database` / `drop_database` style helpers, and a live test `tests/test_<backend>_live.py` gated by a `pytestmark = skipif(...)` server check.

### Macrobench (`macrobench/`)

`runner.py` loads a `MacroBenchConfig` textproto, seeds data (generators in `datagen/ch.py`, schema from `db_setup/ch_benchmark_schema.sql` plus the scenario's extension in `schema/*.sql`), applies the intensity multipliers (`intensity.py`), runs the scenario under a `StallWatchdog` and `StorageSampler`, and writes `<run_id>.parquet` + `<run_id>_e2e_stats.json`. Scenarios are classes in `scenarios/s<N>_<key>.py` registered via `@register` on `Scenario`; `scenarios/base.py` provides `ScenarioContext` (worker pool, retries, cancellation, invariant recorder, metrics) and the `ScenarioStopped` / `UnsupportedOperationAbort` control flow: an UNSUPPORTED verb ends the run, a FAILED one does not. `faults.py` holds the fault catalog and TPC-C consistency checks; `tpcc.py` the spine load transactions. All DDL in `schema/` is written in the SQL subset Postgres and MySQL share, applied one statement at a time.

### Microbench (`microbench/`)

`runner2.py` is the latency/throughput runner (config `task2.proto`; operations registered from `operations/{crud,branch,ddl}.py` via `register_all_operations`). It owns `BackendManager` / `BackendSetup` / `create_db_tools`, which the macrobench runner and `microbench/verbs/` reuse. `microbench/verbs/` is a separate per-verb cost benchmark on the macrobench's CH data (`grid.py` defines the points, `ops.py` the untimed preparation and timed verb for each).

### Tests

`tests/fake_backend.py` is an in-memory sqlite `DBToolSuite` (one sqlite DB per branch, snapshot commits; merge/rebase/revert deliberately unsupported) used by `test_db_api.py`, `test_macrobench.py`, `test_scenarios.py` and the micro tests. Prefer extending it over requiring a server when testing API or runner behaviour.
