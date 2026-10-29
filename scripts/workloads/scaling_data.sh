#!/usr/bin/env bash
# Data-op scaling with every branch busy (experiment 5, sub-experiment B; experiments 7 and 11 B).
# N branches, one client per branch with one request in flight, running READ, RANGE_READ (100 rows), INSERT or
# UPDATE back to back; min(N, MAX_PROCS) client processes; 10% untimed warm-up. Neon: the default cache filled on
# every branch before its client's warm-up, statement logging off.
#
# Statements per client (experiment 5): at least 12,000 point reads, 2,500 range reads, 6,000 writes per run and at
# least 3,000 / 600 / 2,500 / 2,500 per client, but at most 1,000,000 reads and 640,000 writes per run (the
# runner keeps every result row in memory until the run ends).
#
#   scripts/workloads/scaling_data.sh --topology T --backend neon|doltgres --outdir DIR \
#       [--ns 1,4,...] [--ops READ,...] [--ceiling [--ceiling-ns 128,256,...] [--ceiling-seconds 30]] [--smoke] [--dry-run]
#
# --ceiling: after the sweep, ceiling_check.py (experiment 5's minimal asyncio client, same load shape) at the
# N in --ceiling-ns (default 128,256,512,1024 that are in the sweep's N) -> ceiling_check.csv
{
source "$(dirname "$0")/common.sh"
parse_args "$@"
CEILING=false
CEILING_NS=""
CEILING_S=30
set -- "${REST[@]}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --ceiling)         CEILING=true; shift ;;
        --ceiling-ns)      CEILING_NS="$2"; shift 2 ;;
        --ceiling-seconds) CEILING_S="$2"; shift 2 ;;
        *) die "unknown option $1" ;;
    esac
done
wl_init scaling_data
NS=${NS_ARG:-$NS_DATA}
$SMOKE && NS=${NS_ARG:-1,4}
RUN_ENV=()
if [[ $BACKEND == neon ]]; then
    export NEON_LOCAL_COMPUTE_OPTS="$(compute_opts false true)"
    RUN_ENV=("DBFORK_PREWARM_SQL=$(neon_prewarm_sql)")
fi

ops_per_client() {  # OP N (experiment 5's run_scaling.sh)
    local op=$1 n=$2 total min cap
    case "$op" in
        READ)       total=12000; min=3000; cap=1000000 ;;
        RANGE_READ) total=2500;  min=600;  cap=1000000 ;;
        *)          total=6000;  min=2500; cap=640000 ;;
    esac
    local k=$(( (total + n - 1) / n ))
    (( k < min )) && k=$min
    (( k * n > cap )) && k=$(( cap / n ))
    smoke_k "$k"
}

one() {  # op n
    local op=$1 n=$2 procs=$(( $2 < MAX_PROCS ? $2 : MAX_PROCS )) dir="$OUTROOT/data_ops/$1"
    $DRY || mkdir -p "$dir"
    run "data_${op}_n${n}" "$dir" "$n" "$n" "$op" "$procs" \
        "$TP" "$RUNNER_BACKEND" --sweep-proportional --threads-per-branch 1 --branch-list "$n" \
        --processes "$procs" --operations "$op" --num-ops "$(ops_per_client "$op" "$n")" "${COMMON[@]}" \
        --output-dir "$dir"   # one directory per op: the parquet name has no op in it
}

wl_start "ns=$NS ops='$DATA_OPS' prewarm_heap_blocks=$PREWARM_HEAP_BLOCKS opts=${NEON_LOCAL_COMPUTE_OPTS:-}"
IFS=',' read -ra NLIST <<< "$NS"
for n in "${NLIST[@]}"; do
    for op in $DATA_OPS; do
        $DRY && log "plan data_${op}_n${n}: procs=$(( n < MAX_PROCS ? n : MAX_PROCS )) per_client=$(ops_per_client "$op" "$n") total=$(( $(ops_per_client "$op" "$n") * n ))"
        if is_done "data_${op}_n${n}"; then log "SKIP  data_${op}_n${n} (done before)"; continue; fi
        one "$op" "$n"
    done
done
if ! $DRY; then   # a run that did not pass is retried once, at the end of the sweep
    for n in "${NLIST[@]}"; do
        for op in $DATA_OPS; do
            is_done "data_${op}_n${n}" && continue
            log "RETRY data_${op}_n${n}"
            one "$op" "$n"
        done
    done
fi

if $CEILING; then
    if [[ -z $CEILING_NS ]]; then
        if $SMOKE; then CEILING_NS=4; else
            CEILING_NS=$(for c in 128 256 512 1024; do [[ ",$NS," == *",$c,"* ]] && echo "$c"; done | paste -sd,)
        fi
    fi
    $SMOKE && CEILING_S=5
    kinds=$(for op in $DATA_OPS; do case $op in READ) echo point ;; RANGE_READ) echo range ;; INSERT) echo insert ;; UPDATE) echo update ;; esac; done | paste -sd,)
    if [[ -z $CEILING_NS ]]; then log "ceiling: none of 128,256,512,1024 in ns=$NS; skipped"
    elif is_done ceiling; then log "SKIP  ceiling (done before)"
    else
        run_plain ceiling uv run python scripts/workloads/ceiling_check.py --topology "$TOPO" --backend "$BACKEND" \
            --ns "$CEILING_NS" --kinds "$kinds" --seconds "$CEILING_S" --procs "$MAX_PROCS" \
            --out "$OUTROOT/ceiling_check.csv"
    fi
fi
wl_end
exit 0
}
