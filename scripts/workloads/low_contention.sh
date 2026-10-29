#!/usr/bin/env bash
# Low contention (experiments 4 and 5, sub-experiment A; experiment 11 A): one busy branch, one client.
#
#   data     one branch, one client running READ (40,000), RANGE_READ (10,000; 100-row ranges), INSERT (6,000) or
#            UPDATE (6,000) back to back, each its own run (10% untimed warm-up); Neon: before its warm-up the client
#            fills the compute's caches (DBFORK_PREWARM_SQL)
#   branch   one client creates CREATES (100) branches, connecting to every CONNECT_EVERY-th (5th; on Neon that
#            starts its compute), while the one setup branch has a background client running MIXED (80% point
#            reads, 20% point updates)
# Neon: statement logging on (the compute log holds per-statement durations).
#
#   scripts/workloads/low_contention.sh --topology T --backend neon|doltgres --outdir DIR \
#       [--parts data,branch] [--ops READ,...] [--smoke] [--dry-run]
{
source "$(dirname "$0")/common.sh"
parse_args "$@"
PARTS="data,branch"
set -- "${REST[@]}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --parts) PARTS="$2"; shift 2 ;;
        *) die "unknown option $1" ;;
    esac
done
[[ -n $NS_ARG ]] && warn "--ns is ignored: low contention runs at N = 1"
wl_init low_contention
(( RUN_TIMEOUT < 7200 )) && RUN_TIMEOUT=7200   # experiments 5 and 11: 40,000 reads over a WAN round trip
$SMOKE && CREATES=10
RUN_ENV=()
if [[ $BACKEND == neon ]]; then
    export NEON_LOCAL_COMPUTE_OPTS="$(compute_opts true false)"
    RUN_ENV=("DBFORK_PREWARM_SQL=$(neon_prewarm_sql)")
fi

ops() {  # experiment 4's counts
    local k
    case "$1" in READ) k=40000 ;; RANGE_READ) k=10000 ;; *) k=6000 ;; esac
    smoke_k "$k"
}

wl_start "parts=$PARTS ops='$DATA_OPS' creates=$CREATES connect_every=$CONNECT_EVERY prewarm_heap_blocks=$PREWARM_HEAP_BLOCKS opts=${NEON_LOCAL_COMPUTE_OPTS:-}"
one_data() {
    local op=$1 dir="$OUTROOT/data_ops/$1"
    $DRY || mkdir -p "$dir"
    run "data_${op}_n1" "$dir" 1 1 "$op" 1 \
        "$TP" "$RUNNER_BACKEND" --sweep-proportional --threads-per-branch 1 --branch-list 1 --processes 1 \
        --operations "$op" --num-ops "$(ops "$op")" "${COMMON[@]}" \
        --output-dir "$dir"   # one directory per op: the parquet name has no op in it
}
one_create() {
    local dir="$OUTROOT/branch_create"
    $DRY || mkdir -p "$dir"
    run create_n1 "$dir" 1 1 BRANCH_CREATE 2 \
        "$TP" "$RUNNER_BACKEND" --sweep-branches --threads 1 --branch-list 1 --processes 2 \
        --operations BRANCH_CREATE --branch-create-ops "$CREATES" --branch-create-connect-every "$CONNECT_EVERY" \
        --background-op MIXED "${COMMON[@]}" --output-dir "$dir"
}
for pass in first retry; do   # a run that did not pass is retried once, at the end
    if [[ ",$PARTS," == *",data,"* ]]; then
        for op in $DATA_OPS; do
            if is_done "data_${op}_n1"; then [[ $pass == first ]] && log "SKIP  data_${op}_n1 (done before)"; continue; fi
            [[ $pass == retry ]] && { $DRY && continue; log "RETRY data_${op}_n1"; }
            one_data "$op"
        done
    fi
    if [[ ",$PARTS," == *",branch,"* ]]; then
        if is_done create_n1; then [[ $pass == first ]] && log "SKIP  create_n1 (done before)"
        elif [[ $pass == first ]]; then one_create
        elif ! $DRY; then log "RETRY create_n1"; one_create; fi
    fi
done
wl_end
exit 0
}
