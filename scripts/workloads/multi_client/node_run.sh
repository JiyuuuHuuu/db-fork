#!/usr/bin/env bash
# One client node's share of one scaling_data point, started by multi_scaling.py (same directory) on every client
# node. The environment and the runner arguments are exactly those of scripts/workloads/scaling_data.sh (common.sh's
# load_env, compute_opts, neon_prewarm_sql and COMMON; scaling_data.sh's ops_per_client for the point's total N),
# with this node's branch count and client processes. What scaling_data.sh's per-run wrapper (common.sh run) also
# does is left to multi_scaling.py, which runs once per point for all nodes: the purge, the pass checks, the
# progress log; the client-node guard and the workload lock (single-client assumptions) are not taken.
#
#   node_run.sh --topology T --backend neon --outdir D --op OP --n-total N --n-node n --procs p --node NAME
#               [--setup-parallelism K]
# Output: D/neon/scaling_data/data_ops/OP/NAME/ (runner summary + parquet), log on stdout.
# Cross-node start: BENCH_FAST_CLIENT=1, PYTHONPATH=<repo>/microbench/fast_client, BENCH_SYNC_ADDR, BENCH_SYNC_KEY
# (microbench/fast_client/patches.py); none of them is a DBFORK_ variable, so load_env keeps them.
source "$(dirname "${BASH_SOURCE[0]}")/../common.sh"
parse_args "$@"
set -- "${REST[@]}"
OP="" NT="" NN="" PROCS="" NODE="" SP=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --op) OP=$2; shift 2 ;;
        --n-total) NT=$2; shift 2 ;;
        --n-node) NN=$2; shift 2 ;;
        --procs) PROCS=$2; shift 2 ;;
        --node) NODE=$2; shift 2 ;;
        --setup-parallelism) SP=$2; shift 2 ;;
        *) die "unknown argument $1" ;;
    esac
done
[[ -n $OP && -n $NT && -n $NN && -n $PROCS && -n $NODE ]] || die "--op --n-total --n-node --procs --node are required"
[[ $BACKEND_ARG == neon ]] || die "multi-client runs are implemented for neon only"

WORKLOAD=scaling_data
load_env
BACKEND=$BACKEND_ARG
client_bind
[[ -n ${NEON_LOCAL_TENANT_ID:-} ]] || die "no seed in $ENV_FILE (neon.py TOPO seed, deploy-client, multi_scaling.py deploy)"
export NEON_LOCAL_COMPUTE_OPTS="$(compute_opts false true)"
export DBFORK_PREWARM_SQL="$(neon_prewarm_sql)"
COMMON=(--mode sync --warmup-fraction "$WARMUP_FRACTION" --table-name "$TABLE"
        --updates-per-branch "$UPDATES_PER_BRANCH" --seed-db "$SEED_DB" --generated-scale "$W"
        --setup-parallelism "${SP:-$SETUP_PARALLELISM}")

ops_per_client() {  # OP N: scripts/workloads/scaling_data.sh's (experiment 5's run_scaling.sh), for the total N
    local op=$1 n=$2 total min cap
    case "$op" in
        READ)       total=12000; min=3000; cap=1000000 ;;
        RANGE_READ) total=2500;  min=600;  cap=1000000 ;;
        *)          total=6000;  min=2500; cap=640000 ;;
    esac
    local k=$(( (total + n - 1) / n ))
    (( k < min )) && k=$min
    (( k * n > cap )) && k=$(( cap / n ))
    echo "$k"
}

OUTROOT="$OUTDIR/$BACKEND/$WORKLOAD"
dir="$OUTROOT/data_ops/$OP/$NODE"
mkdir -p "$dir"
rid="${RUNNER_BACKEND}_chw${W}_tp_t${NN}_b${NN}"
shopt -s nullglob
old=("$dir/$rid".* "$dir/${rid}"_*)
shopt -u nullglob
if (( ${#old[@]} )); then   # the runner appends to <run_id>.parquet: an earlier attempt must not mix in
    mkdir -p "$dir/superseded_$(date +%s)" && mv "${old[@]}" "$dir/superseded_$(date +%s)/"
fi
echo "node_run $NODE: op=$OP n_total=$NT n_node=$NN procs=$PROCS ops_per_client=$(ops_per_client "$OP" "$NT") host=$(hostname)"
exec env timeout -k 120 -s INT "$RUN_TIMEOUT" "${CLIENT_BIND[@]}" "$TP" "$RUNNER_BACKEND" --sweep-proportional \
    --threads-per-branch 1 --branch-list "$NN" --processes "$PROCS" --operations "$OP" \
    --num-ops "$(ops_per_client "$OP" "$NT")" "${COMMON[@]}" --output-dir "$dir"
