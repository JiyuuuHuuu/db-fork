#!/usr/bin/env bash
# Every workload for one backend, in order (experiment 5's run_all.sh, generalized). Resumable: every driver skips
# runs already DONE in its progress.log and retries the others, so rerunning this resumes where it stopped.
#
#   low_contention   low_contention.sh (N = 1: data ops, then 100 creates with one MIXED busy branch)
#   scaling_data     scaling_data.sh --ceiling (N = ns_data, four ops; then the minimal-client ceiling check)
#   scaling_branch   scaling_branch.sh (N = ns_branch, background = the topology's background)
#   cold_start       cold_start.py (cold_runs x cold_reads point reads on a new branch)
#
#   scripts/workloads/run_all.sh --topology T --backend neon|doltgres --outdir DIR \
#       [--steps low_contention,scaling_data,scaling_branch,cold_start] [--no-ceiling] [--background READ|MIXED] \
#       [--smoke] [--dry-run]
# Long runs: start it detached on the client node, e.g.
#   setsid nohup scripts/workloads/run_all.sh ... > DIR/run_all.out 2>&1 < /dev/null &
# (CloudLab: as a transient systemd unit, systemd-run --user, so that an ssh logout does not end it.)
{
source "$(dirname "$0")/common.sh"
parse_args "$@"
STEPS="low_contention,scaling_data,scaling_branch,cold_start"
CEIL=(--ceiling)
BG_ARGS=()
set -- "${REST[@]}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --steps)      STEPS="$2"; shift 2 ;;
        --no-ceiling) CEIL=(); shift ;;
        --background) BG_ARGS=(--background "$2"); shift 2 ;;
        *) die "unknown option $1" ;;
    esac
done
PASS=(--topology "$TOPO" --backend "$BACKEND_ARG" --outdir "$OUTDIR")
$SMOKE && PASS+=(--smoke)
$DRY && PASS+=(--dry-run)
NSP=(); [[ -n $NS_ARG ]] && NSP=(--ns "$NS_ARG")
OPSP=(); [[ -n $OPS_ARG ]] && OPSP=(--ops "${OPS_ARG// /,}")
BASE="$OUTDIR/$BACKEND_ARG"; $SMOKE && BASE="$OUTDIR/$BACKEND_ARG/smoke"
LOG=$BASE/run_all.log
$DRY || mkdir -p "$BASE"
note() { if $DRY; then echo "$(date -u +%FT%TZ) $*"; else echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; fi; }

cold_start_step() {  # the cold start as one run of a small driver: progress.log, client placement, CPU agent
    wl_init cold_start
    (( RUN_TIMEOUT < 10800 )) && RUN_TIMEOUT=10800   # experiment 5: 3 x 30,000 reads over the WAN took 56 min
    RUN_ENV=()
    $SMOKE && { COLD_RUNS=1; COLD_READS=3000; COLD_PAUSE_S=5; }
    wl_start "runs=$COLD_RUNS reads=$COLD_READS pause=${COLD_PAUSE_S}s"
    if is_done cold_start; then log "SKIP  cold_start (done before)"
    else
        local a=(--topology "$TOPO" --backend "$BACKEND" --outdir "$OUTDIR"); $SMOKE && a+=(--smoke)
        $DRY && uv run python scripts/workloads/cold_start.py "${a[@]}" --dry-run
        run_plain cold_start uv run python scripts/workloads/cold_start.py "${a[@]}"
    fi
    wl_end
}

cold_start_sub() { ( trap stop_agent EXIT; cold_start_step ); }   # a subshell: a guard's exit ends only the step

step() {  # name cmd...
    local name=$1; shift
    note "START $name"
    local t0=$SECONDS rc
    if $DRY; then "$@"; rc=$?
    else "$@" > "$BASE/driver_$name.out" 2>&1; rc=$?; fi
    if (( rc == 0 )); then note "DONE $name ($((SECONDS - t0))s; see $name/progress.log)"; return 0; fi
    note "FAILED $name ($((SECONDS - t0))s, exit $rc, see driver_$name.out)"; return 1
}

note "=== run_all $BACKEND_ARG: steps=$STEPS smoke=$SMOKE dry=$DRY topology=$TOPO"
IFS=',' read -ra L <<< "$STEPS"
for s in "${L[@]}"; do
    case "$s" in
        low_contention) step "$s" "$WL_DIR/low_contention.sh" "${PASS[@]}" "${OPSP[@]}" || exit 1 ;;
        scaling_data)   step "$s" "$WL_DIR/scaling_data.sh" "${PASS[@]}" "${NSP[@]}" "${OPSP[@]}" "${CEIL[@]}" || exit 1 ;;
        scaling_branch) step "$s" "$WL_DIR/scaling_branch.sh" "${PASS[@]}" "${NSP[@]}" "${BG_ARGS[@]}" || exit 1 ;;
        cold_start)     step "$s" cold_start_sub || exit 1 ;;
        *) note "unknown step $s"; exit 1 ;;
    esac
done
note "=== run_all $BACKEND_ARG done"
exit 0
}
