#!/usr/bin/env bash
# Branch creation with N busy branches (experiment 5, sub-experiment C; experiments 7 and 11 C with --background READ).
# One client creates CREATES (100) branches, connecting to every CONNECT_EVERY-th (5th; on Neon that starts its
# compute), while each of the N setup branches has a background client running the background operation: MIXED
# (80% point reads, 20% point updates; experiments 2, 5, 10, the default) or READ (point reads; experiments 7, 11).
# min(N + 1, MAX_PROCS) client processes. Neon: the default cache filled on every busy branch before its client
# starts, statement logging off.
#
#   scripts/workloads/scaling_branch.sh --topology T --backend neon|doltgres --outdir DIR \
#       [--ns 1,4,...] [--background MIXED|READ] [--smoke] [--dry-run]
# Results: DIR/<backend>/scaling_branch/branch_create/<background, lower case>/
{
source "$(dirname "$0")/common.sh"
parse_args "$@"
BG=""
set -- "${REST[@]}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --background) BG="$2"; shift 2 ;;
        *) die "unknown option $1" ;;
    esac
done
[[ -n $OPS_ARG ]] && warn "--ops is ignored: the timed operation is BRANCH_CREATE"
wl_init scaling_branch
BG=${BG:-$BACKGROUND}
[[ $BG == MIXED || $BG == READ ]] || die "--background MIXED|READ"
NS=${NS_ARG:-$NS_BRANCH}
$SMOKE && { NS=${NS_ARG:-1,4}; CREATES=10; }
RUN_ENV=()
if [[ $BACKEND == neon ]]; then
    [[ ${NEON_LOCAL_PS_OPTS:-} == *pageservers* ]] && warn "the tenant is sharded (NEON_LOCAL_PS_OPTS); experiment 5 created branches on the unsharded tenant"
    export NEON_LOCAL_COMPUTE_OPTS="$(compute_opts false true)"
    RUN_ENV=("DBFORK_PREWARM_SQL=$(neon_prewarm_sql)")
fi
bg=${BG,,}
DIR="$OUTROOT/branch_create/$bg"

one() {  # n
    local n=$1 procs=$(( $1 + 1 < MAX_PROCS ? $1 + 1 : MAX_PROCS ))
    $DRY || mkdir -p "$DIR"
    run "create_${bg}_n${n}" "$DIR" 1 "$n" BRANCH_CREATE "$procs" \
        "$TP" "$RUNNER_BACKEND" --sweep-branches --threads 1 --branch-list "$n" --processes "$procs" \
        --operations BRANCH_CREATE --branch-create-ops "$CREATES" --branch-create-connect-every "$CONNECT_EVERY" \
        --background-op "$BG" "${COMMON[@]}" --output-dir "$DIR"
}

wl_start "ns=$NS background=$BG creates=$CREATES connect_every=$CONNECT_EVERY opts=${NEON_LOCAL_COMPUTE_OPTS:-}"
IFS=',' read -ra NLIST <<< "$NS"
for n in "${NLIST[@]}"; do
    if is_done "create_${bg}_n${n}"; then log "SKIP  create_${bg}_n${n} (done before)"; continue; fi
    one "$n"
done
if ! $DRY; then   # a run that did not pass is retried once, at the end of the sweep
    for n in "${NLIST[@]}"; do
        is_done "create_${bg}_n${n}" && continue
        log "RETRY create_${bg}_n${n}"
        one "$n"
    done
fi
wl_end
exit 0
}
