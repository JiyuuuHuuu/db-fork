#!/bin/bash
# run_single_thread_bench.sh - Single-threaded benchmark script using runner2.py
#
# Usage: ./scripts/run_single_thread_bench.sh <backend> [<sql_dump_path>] <branch_counts> [OPTIONS]
#
# Required Arguments:
#   backend:        dolt, dolt_mysql, seekdb, neon, xata, file_copy
#   sql_dump_path:  Path to SQL dump file (e.g., schemas/tpcc_mini.sql); without
#                   one, CH-benCHmark data is generated in the server at
#                   --generated-scale W (foreign keys skipped)
#   branch_counts:  Comma-separated list of branch counts (e.g., 8,16,32) or single value (e.g., 16)
#
# Options:
#   --generated-scale <W>  W warehouses of generated CH data (default 1; no SQL dump)
#   --seed-db <name>      Dolt only: copy the generated data from this database,
#                         generated once on first use, instead of regenerating per run
#   --inserts-per-branch <n>, --updates-per-branch <n>, --deletes-per-branch <n>
#                         untimed data ops on every setup branch (default 0), so
#                         branches diverge from their parent
#   --branch-create-ops <n> timed creations per BRANCH_CREATE run after the
#                         N-branch setup (default 10)
#   --seed <seed>         Random seed for reproducibility
#   --shape <shape>       Branch tree shape: spine, bushy, or fan_out (default: spine)
#   --measure-storage     Measure disk size before/after each update
#   --operations <ops>    Comma-separated list (e.g., READ,UPDATE; default: all)
#   --range-size <n>      Range size for RANGE_UPDATE operation (default: 200)
#   --table-name <name>   Table to run operations against (default: orders)
#   --num-ops <n>         Number of operations to perform (overrides defaults)
#   --output-dir <dir>    Output directory for results (default: ./run_stats)
#
# Examples:
#   ./scripts/run_single_thread_bench.sh dolt schemas/tpcc_mini.sql 16
#   ./scripts/run_single_thread_bench.sh neon schemas/tpcc_mini.sql 8,16,32
#   ./scripts/run_single_thread_bench.sh dolt schemas/tpcc_mini.sql 8 --measure-storage
#   ./scripts/run_single_thread_bench.sh neon schemas/tpcc_mini.sql 16,32 --operations READ,UPDATE

set -e

# Parse arguments
BACKEND=""
SQL_DUMP_PATH=""
NUM_BRANCHES=""
SEED=""
SHAPE="spine"
MEASURE_STORAGE=false
OPS_STRING=""
NUM_OPS_OVERRIDE=""
OUTPUT_DIR="./run_stats"
RANGE_SIZE=200
TABLE_NAME=""
BACKFILL_FRACTION=1.0
GENERATED_SCALE=""
SEED_DB=""
INSERTS_PER_BRANCH=0
UPDATES_PER_BRANCH=0
DELETES_PER_BRANCH=0
BRANCH_CREATE_OPS=10
POSITIONALS=()


while [[ $# -gt 0 ]]; do
    case $1 in
        --seed)
            SEED="$2"
            shift 2
            ;;
        --shape)
            SHAPE="$2"
            shift 2
            ;;
        --measure-storage)
            MEASURE_STORAGE=true
            shift
            ;;
        --operations)
            OPS_STRING="$2"
            shift 2
            ;;
        --range-size)
            RANGE_SIZE="$2"
            shift 2
            ;;
        --table-name)
            TABLE_NAME="$2"
            shift 2
            ;;
        --backfill-fraction)
            BACKFILL_FRACTION="$2"
            shift 2
            ;;
        --num-ops)
            NUM_OPS_OVERRIDE="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --sql-dump)
            SQL_DUMP_PATH="$2"
            shift 2
            ;;
        --generated-scale)
            GENERATED_SCALE="$2"
            shift 2
            ;;
        --seed-db)
            SEED_DB="$2"
            shift 2
            ;;
        --inserts-per-branch)
            INSERTS_PER_BRANCH="$2"
            shift 2
            ;;
        --updates-per-branch)
            UPDATES_PER_BRANCH="$2"
            shift 2
            ;;
        --deletes-per-branch)
            DELETES_PER_BRANCH="$2"
            shift 2
            ;;
        --branch-create-ops)
            BRANCH_CREATE_OPS="$2"
            shift 2
            ;;
        -*)
            echo "Error: Unknown option '$1'"
            exit 1
            ;;
        *)
            POSITIONALS+=("$1")
            shift
            ;;
    esac
done

# Positionals: <backend> <branch_counts>, or <backend> <sql_dump_path> <branch_counts>
case ${#POSITIONALS[@]} in
    2) BACKEND="${POSITIONALS[0]}"; NUM_BRANCHES="${POSITIONALS[1]}" ;;
    3) BACKEND="${POSITIONALS[0]}"; SQL_DUMP_PATH="${POSITIONALS[1]}"; NUM_BRANCHES="${POSITIONALS[2]}" ;;
    *) BACKEND=""; NUM_BRANCHES="" ;;
esac
if [ -z "$SQL_DUMP_PATH" ] && [ -z "$GENERATED_SCALE" ]; then
    GENERATED_SCALE=1
fi

# Validate required arguments
if [ -z "$BACKEND" ] || [ -z "$NUM_BRANCHES" ]; then
    echo "Usage: $0 <backend> [<sql_dump_path>] <branch_counts> [options]"
    echo ""
    echo "Required:"
    echo "  backend: dolt, dolt_mysql, seekdb, neon, xata, file_copy"
    echo "  sql_dump_path: Path to SQL dump file (e.g., db_setup/tpcc_schema.sql); omit to generate CH data"
    echo "  branch_counts: Comma-separated list of branch counts (e.g., 8,16,32) or single value (e.g., 16)"
    echo ""
    echo "Options:"
    echo "  --generated-scale <W>: generate CH-benCHmark data at W warehouses (default 1 without a dump)"
    echo "  --seed-db <name>: Dolt only; copy the generated data from this database (generated once)"
    echo "  --inserts-per-branch/--updates-per-branch/--deletes-per-branch <n>: untimed data ops per setup branch"
    echo "  --branch-create-ops <n>: timed creations per BRANCH_CREATE run (default 10)"
    echo "  --seed <seed>: Random seed for reproducibility (default: random)"
    echo "  --shape <shape>: Branch tree shape: spine, bushy, or fan_out (default: spine)"
    echo "  --measure-storage: Measure disk_size_before/after for each update"
    echo "  --operations <ops>: Comma-separated list (e.g., READ,UPDATE; default: all)"
    echo "  --range-size <n>: Range size for RANGE_UPDATE operation (default: 200)"
    echo "  --table-name <name>: Table to run operations against (default: orders)"
    echo "  --num-ops <n>: Number of operations (overrides defaults)"
    echo "  --output-dir <dir>: Output directory for results (default: ./run_stats)"
    echo ""
    echo "Examples:"
    echo "  $0 dolt schemas/tpcc_mini.sql 8                      # Single branch count"
    echo "  $0 neon schemas/tpcc_mini.sql 8,16,32                # Multiple branch counts"
    echo "  $0 dolt schemas/tpcc_mini.sql 16 --operations READ   # Only READ operations"
    echo "  $0 dolt schemas/tpcc_mini.sql 8 --measure-storage    # Storage measurement"
    exit 1
fi

# Convert backend to uppercase for proto config
BACKEND_UPPER=$(echo "$BACKEND" | tr '[:lower:]' '[:upper:]')

# Validate backend
if [[ ! "$BACKEND_UPPER" =~ ^(DOLT|DOLT_MYSQL|SEEKDB|NEON|XATA|FILE_COPY)$ ]]; then
    echo "Error: Invalid backend '$BACKEND'. Must be one of: dolt, dolt_mysql, seekdb, neon, xata, file_copy"
    exit 1
fi

# Convert shape to uppercase for proto config and validate
SHAPE_UPPER=$(echo "$SHAPE" | tr '[:lower:]' '[:upper:]')
if [[ ! "$SHAPE_UPPER" =~ ^(SPINE|BUSHY|FAN_OUT)$ ]]; then
    echo "Error: Invalid shape '$SHAPE'. Must be one of: spine, bushy, fan_out"
    exit 1
fi

# Check if SQL dump file exists (when one is used)
if [ -n "$SQL_DUMP_PATH" ] && [ ! -f "$SQL_DUMP_PATH" ]; then
    echo "Error: SQL dump file not found: $SQL_DUMP_PATH"
    exit 1
fi
if [ -n "$SEED_DB" ] && [[ ! "$BACKEND" =~ ^(dolt|dolt_mysql)$ ]]; then
    echo "Error: --seed-db is supported on dolt and dolt_mysql only"
    exit 1
fi

# Generate random seed if not provided
if [ -z "$SEED" ]; then
    SEED=$(( (RANDOM * 32768 + RANDOM) % 2147483647 ))
fi

# Default operations (using runner2 operation names)
OPERATIONS=(BRANCH_CREATE READ BRANCH_CONNECT INSERT UPDATE RANGE_READ RANGE_UPDATE)

# Override OPERATIONS if --operations was provided
if [ -n "$OPS_STRING" ]; then
    IFS=',' read -ra OPERATIONS <<< "$OPS_STRING"
fi

# Parse branch counts into array
IFS=',' read -ra BRANCH_COUNTS <<< "$NUM_BRANCHES"

# Other fixed config values
TABLE_NAME="${TABLE_NAME:-orders}"
DB_NAME="microbench"

# Create temporary config file
TEMP_CONFIG=$(mktemp /tmp/${BACKEND}_bench_config_XXXXXX)

cleanup() {
    rm -f "$TEMP_CONFIG"
}
trap cleanup EXIT

# Data tag for run_id: first 4 chars of the dump name, or chw<W> for generated data
if [ -n "$SQL_DUMP_PATH" ]; then
    SQL_BASENAME=$(basename "$SQL_DUMP_PATH" .sql)
    SQL_PREFIX=${SQL_BASENAME:0:4}
    DB_SOURCE_BLOCK="  sql_dump {
    sql_dump_path: \"${SQL_DUMP_PATH}\"
  }"
else
    SQL_PREFIX="chw${GENERATED_SCALE}"
    DB_SOURCE_BLOCK="  generated {
    scale_factor: ${GENERATED_SCALE}
    seed_db: \"${SEED_DB}\"
  }"
fi

# Function to get num_ops based on operation type
get_num_ops() {
    local op=$1
    case $op in
        BRANCH_CREATE|BRANCH_DELETE)
            echo "$BRANCH_CREATE_OPS"
            ;;
        RANGE_UPDATE)
            echo 200
            ;;
        BRANCH_COMMIT|BRANCH_DIFF|BRANCH_LOG|BRANCH_MERGE|BRANCH_REBASE|BRANCH_REVERT|BRANCH_RESET)
            echo 100
            ;;
        BRANCH_CONNECT|READ|INSERT|UPDATE|DELETE|RANGE_READ)
            echo 1000
            ;;
                DDL_ADD_INDEX|DDL_REMOVE_INDEX|DDL_VACUUM|DDL_ADD_COLUMN|DDL_REMOVE_COLUMN|DDL_BACKFILL|DDL_ADD_COLUMN_WITH_DEFAULT|DDL_TYPE_CHANGE|DDL_RENAME_COLUMN|DDL_RENAME_TABLE|DDL_ADD_FK|DDL_ADD_CHECK|DDL_SET_DEFAULT|DDL_SET_NOT_NULL|DDL_EXPAND_CONTRACT)
            echo 10
            ;;
        *)
            echo 1000
            ;;
    esac
}

echo "==================================================="
echo "Single-Threaded Microbenchmark (runner2.py)"
echo "Backend: $BACKEND"
echo "Data: ${SQL_DUMP_PATH:-generated CH-benCHmark W=$GENERATED_SCALE${SEED_DB:+ (copied from $SEED_DB)}}"
echo "Per setup branch: $INSERTS_PER_BRANCH inserts, $UPDATES_PER_BRANCH updates, $DELETES_PER_BRANCH deletes"
echo "Operations: ${OPERATIONS[*]}"
echo "Branch Counts: ${BRANCH_COUNTS[*]}"
echo "Branch Shape: $SHAPE"
echo "Random Seed: $SEED"
echo "Measure Storage: $MEASURE_STORAGE"
echo "Backfill Fraction: $BACKFILL_FRACTION"
if [ -n "$NUM_OPS_OVERRIDE" ]; then
    echo "Num Ops (override): $NUM_OPS_OVERRIDE"
fi
echo "==================================================="

# Loop through all operations, then all branch counts
for OPERATION in "${OPERATIONS[@]}"; do
    # Use override if provided
    if [ -n "$NUM_OPS_OVERRIDE" ]; then
        NUM_OPS="$NUM_OPS_OVERRIDE"
    else
        NUM_OPS=$(get_num_ops "$OPERATION")
    fi

    for NUM_BRANCHES in "${BRANCH_COUNTS[@]}"; do
        SHAPE_LOWER=$(echo "$SHAPE" | tr '[:upper:]' '[:lower:]')
        RUN_ID="${BACKEND}_${SQL_PREFIX}_${NUM_BRANCHES}_${SHAPE_LOWER}"

        # Every operation, BRANCH_CREATE included, runs after the N-branch
        # setup: a BRANCH_CREATE run times further creations on top of N.
        SETUP_NUM_BRANCHES=$NUM_BRANCHES

        echo ""
        echo "---------------------------------------------------"
        echo "Running: $RUN_ID"
        echo "  Operation: $OPERATION, Num Ops: $NUM_OPS, Setup Branches: $SETUP_NUM_BRANCHES"
        echo "---------------------------------------------------"

    # Generate config file (task2.proto format for runner2.py)
    cat > "$TEMP_CONFIG" << EOF
# Auto-generated config for runner2.py
run_id: "${RUN_ID}"
backend: ${BACKEND_UPPER}
table_name: "${TABLE_NAME}"
scale_factor: 1

database_setup {
  db_name: "${DB_NAME}"
  cleanup: true
${DB_SOURCE_BLOCK}
}

num_threads: 1
measure_storage: ${MEASURE_STORAGE}

operation_benchmark {
  operation: ${OPERATION}
  num_ops: ${NUM_OPS}

  setup {
    num_branches: ${SETUP_NUM_BRANCHES}
    branch_shape: ${SHAPE_UPPER}
    inserts_per_branch: ${INSERTS_PER_BRANCH}
    updates_per_branch: ${UPDATES_PER_BRANCH}
    deletes_per_branch: ${DELETES_PER_BRANCH}
  }

  range_config {
    range_size: ${RANGE_SIZE}
  }
  ddl_config {
    backfill_fraction: ${BACKFILL_FRACTION}
  }
}
EOF

    echo "Config generated at: $TEMP_CONFIG"
    cat "$TEMP_CONFIG"
    echo ""

    # Run the benchmark
    echo "Starting benchmark..."
    uv run python -m microbench.runner2 --config "$TEMP_CONFIG" --output-dir "$OUTPUT_DIR"

        # Clean up dropped databases to prevent disk space explosion (Dolt only)
        if [ "$BACKEND" = "dolt" ] || [ "$BACKEND" = "dolt_mysql" ]; then
            if [ "$BACKEND" = "dolt_mysql" ]; then
                DOLT_DIR="${DOLT_MYSQL_DATA_DIR:-$HOME/dolt/databases}"
            else
                DOLT_DIR="${DOLT_DATA_DIR:-$HOME/doltgres/databases}"
            fi
            if [ -d "$DOLT_DIR/.dolt_dropped_databases" ]; then
                DROPPED_COUNT=$(ls -1 "$DOLT_DIR/.dolt_dropped_databases" 2>/dev/null | wc -l)
                if [ "$DROPPED_COUNT" -gt 0 ]; then
                    echo "Cleaning up $DROPPED_COUNT dropped database(s) from $DOLT_DIR/.dolt_dropped_databases"
                    rm -rf "$DOLT_DIR/.dolt_dropped_databases"/*
                    echo "Cleanup complete"
                fi
            fi
        fi

        echo "Completed: $RUN_ID"
    done  # BRANCH_COUNT loop
done  # OPERATION loop

echo ""
echo "==================================================="
echo "All benchmarks completed!"
echo "Results are in $OUTPUT_DIR/"
echo "==================================================="
