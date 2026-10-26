# Reproduce experiment 6

Everything runs on one machine with a local Postgres. Nothing touches CloudLab.

## 1. Local Postgres 17.5

Any Postgres 17 build works. We used the vanilla build that ships inside the Neon checkout:

```bash
PGB=/mydata/jiyu/neon/pg_install/v17
export LD_LIBRARY_PATH=$PGB/lib
D=/mydata/jiyu/bb-ratio-pg
$PGB/bin/initdb -D $D/data -U postgres --auth=trust
cat >> $D/data/postgresql.conf <<'EOF'
port = 55990
listen_addresses = '127.0.0.1'
shared_buffers = 8GB
fsync = off
synchronous_commit = off
full_page_writes = off
max_wal_size = 20GB
EOF
$PGB/bin/pg_ctl -D $D/data -l $D/server.log start
```

The scripts connect to `127.0.0.1:55990` as `postgres`. Override this with `BB_PG_HOST`, `BB_PG_PORT` and
`BB_PG_USER`.

## 2. Seed one database per scenario

```bash
X=experiment/6-macrobench-data-ratios
for k in rl_env context_mgmt multi_agent dev_agent ops_agent data_agent; do
  uv run python $X/scripts/pg_seed.py $k > $X/logs/seed_$k.out 2>&1
done
```

This creates `bb_<key>` with the paper config's schema, `seed_ch` (CH W=1, seed 42) and the scenario's own `seed()`,
then prints the row count of every table. Each database takes a few minutes. `data_agent` is the slowest because of
its roll-up join; the script runs `ANALYZE` first so the join gets a sane plan.

Collect the counts:

```bash
python3 - <<'EOF'
import json, pathlib
X = pathlib.Path("experiment/6-macrobench-data-ratios")
out = {}
for f in sorted(X.glob("logs/seed_*.out")):
    key, total, counts = f.read_text().strip().splitlines()[-1].split(" ", 2)
    out[key] = json.loads(counts)
(X / "data/seed_counts.json").write_text(json.dumps(out, indent=1))
EOF
```

## 3. Plan probe

```bash
uv run python $X/scripts/plan_probe.py | tee $X/logs/plan_probe.out
```

This writes `data/plan_probe.json`: rows examined per table and the scan type of every statement shape, from
`EXPLAIN (ANALYZE, FORMAT JSON)`, with each statement rolled back.

## 4. S2 at paper scale

```bash
uv run python $X/scripts/s2_measure.py | tee $X/logs/s2_measure.out
```

This writes `data/s2_candidates.json`. It runs on the in-memory sqlite test backend and needs no server. The run
ends at the cycle-8 revert, which the test backend does not support, after all 24 candidates have been measured.

## 5. Ratios

```bash
python3 $X/scripts/compute_ratios.py | tee $X/logs/compute_ratios.out
```

This writes `data/ratios.json` and prints the table used in `macrobench_data_ratios.md`.

## 6. Stop the server

```bash
LD_LIBRARY_PATH=$PGB/lib $PGB/bin/pg_ctl -D /mydata/jiyu/bb-ratio-pg/data stop
```
