# Shared part of the workload drivers in scripts/workloads/ (sourced, never run). Lifted from experiment 5's and
# experiment 11's common.sh: the topology (scripts/setup/topo.py) replaces cluster.env / env.sh, the backend's
# environment file (<state_dir>/<backend>.env, written by scripts/setup/<backend>.py env-init) replaces .env.
#
# Common arguments of every driver:
#   --topology FILE        scripts/setup/topologies/<name>.toml
#   --backend neon|doltgres
#   --outdir DIR           results go to DIR/<backend>/<workload>/ (smoke: DIR/<backend>/smoke/<workload>/)
#   [--smoke]              counts /10 (at least 30), N = 1,4, 10 creates
#   [--ns LIST]            busy-branch counts (default: the topology's ns_data / ns_branch)
#   [--ops LIST]           data operations (default READ,RANGE_READ,INSERT,UPDATE)
#   [--dry-run]            print every runner command (and the environment it gets) instead of running it
#
# Workloads never start or stop servers: run scripts/setup/<backend>.py TOPO install/start/seed/env-init first.
# A driver parses its own options with parse_args "$@" (unknown ones land in REST), then calls wl_init <workload>.
set -uo pipefail
WL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$WL_DIR/../.." && pwd)"
cd "$REPO" || exit 1
export PATH="$HOME/.local/bin:$PATH" PYTHONUNBUFFERED=1
TP=./scripts/run_throughput_bench.sh

die() { echo "error: $*" >&2; exit 1; }
warn() { echo "warning: $*" >&2; }

# ---------------------------------------------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------------------------------------------
TOPO="" BACKEND_ARG="" OUTDIR="" SMOKE=false DRY=false NS_ARG="" OPS_ARG=""
REST=()
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --topology) TOPO="$2"; shift 2 ;;
            --backend)  BACKEND_ARG="$2"; shift 2 ;;
            --outdir)   OUTDIR="$2"; shift 2 ;;
            --smoke)    SMOKE=true; shift ;;
            --dry-run)  DRY=true; shift ;;
            --ns)       NS_ARG="$2"; shift 2 ;;
            --ops)      OPS_ARG="${2//,/ }"; shift 2 ;;
            *)          REST+=("$1"); shift ;;
        esac
    done
    [[ -n $TOPO && -n $BACKEND_ARG && -n $OUTDIR ]] || die "--topology FILE --backend neon|doltgres --outdir DIR are required"
    [[ $BACKEND_ARG == neon || $BACKEND_ARG == doltgres ]] || die "--backend neon|doltgres"
    [[ -f $TOPO ]] || die "no topology file $TOPO"
    TOPO="$(cd "$(dirname "$TOPO")" && pwd)/$(basename "$TOPO")"
    $DRY || mkdir -p "$OUTDIR" || die "cannot create $OUTDIR"
    OUTDIR="$(realpath -m "$OUTDIR")"
}

# ---------------------------------------------------------------------------------------------------------------
# Environment: inherited settings cleared, then the topology's variables, then the backend's environment file
# ---------------------------------------------------------------------------------------------------------------
load_env() {
    unset DBFORK_SERVER_HOST DBFORK_SERVER_SSH DBFORK_PREWARM_SQL DBFORK_REMOTE_CPU DBFORK_SERVER_PID DBFORK_SETUP
    unset DOLT_HOST DOLT_PORT DOLT_USER DOLT_PASSWORD DOLT_DATA_DIR
    local v
    for v in $(compgen -e | grep -E '^(NEON_LOCAL_|DBFORK_)'); do unset "$v"; done
    local sh
    sh=$(uv run python scripts/setup/topo.py shell "$TOPO" --backend "$BACKEND_ARG") || die "topology $TOPO is invalid"
    eval "$sh"
    if [[ -f $ENV_FILE ]]; then
        set -a; source "$ENV_FILE"; set +a
    elif $DRY; then
        warn "no environment file $ENV_FILE (scripts/setup/$BACKEND.py $TOPO env-init); dry run continues without it"
    else
        die "no environment file $ENV_FILE: run scripts/setup/$BACKEND.py $TOPO env-init"
    fi
    export NEON_LOCAL_PURGE_PARALLEL=${NEON_LOCAL_PURGE_PARALLEL:-64}
    # Several dblib modules load the repo's .env at import (load_dotenv): it would fill in variables the environment
    # file leaves unset (e.g. a stale NEON_LOCAL_PS_OPTS) in every runner process.
    if [[ -f $REPO/.env ]] && grep -qE '^(export +)?(NEON_LOCAL_|DOLT_|DBFORK_)' "$REPO/.env"; then
        if $DRY; then warn "$REPO/.env sets NEON_LOCAL_/DOLT_/DBFORK_ variables the runner would pick up"
        else die "$REPO/.env sets NEON_LOCAL_/DOLT_/DBFORK_ variables the runner would pick up: move it away"; fi
    fi
}

# ---------------------------------------------------------------------------------------------------------------
# Placement of the benchmark client (taskset -c is used, not numactl --physcpubind: numactl rejects CPUs outside the
# caller's affinity)
# ---------------------------------------------------------------------------------------------------------------
client_bind() {
    CLIENT_BIND=()
    if [[ -n $CLIENT_CPUS ]]; then
        CLIENT_BIND=(taskset -c "$CLIENT_CPUS")
        [[ -n $CLIENT_MEM ]] && CLIENT_BIND+=(numactl "--membind=$CLIENT_MEM")
    elif [[ -n $CLIENT_NUMA ]]; then
        CLIENT_BIND=(numactl "--cpunodebind=$CLIENT_NUMA" "--membind=$CLIENT_NUMA")
    fi
}

on_client_host() {  # the driver must run on the client node of the topology
    [[ $CLIENT_IS_LOCAL == 1 ]] && return 0
    local h; for h in "$(hostname)" "$(hostname -f 2>/dev/null)"; do [[ $h == "$CLIENT_HOST" ]] && return 0; done
    return 1
}

# ---------------------------------------------------------------------------------------------------------------
# Workload parameters
# ---------------------------------------------------------------------------------------------------------------
smoke_k() {  # K: per-client count, smoke: a tenth, at least 30
    local k=$1
    $SMOKE && k=$(( k / 10 > 30 ? k / 10 : 30 ))
    echo "$k"
}

# Neon: the prewarm of the default cache, experiment 5's: the first PREWARM_HEAP_BLOCKS stock heap pages, then the
# whole stock primary key (key last: the most recently used part of the LFC; key first evicts index leaves on the
# first heap misses). Untimed, on every client's branch before its warm-up (runner2 DBFORK_PREWARM_SQL).
neon_prewarm_sql() {
    echo "SET maintenance_io_concurrency = 128; SELECT pg_prewarm('stock','buffer','main',0,$((PREWARM_HEAP_BLOCKS - 1))); SELECT pg_prewarm('stock_pkey')"
}

# compute_opts STATEMENT_LOG(true|false) [PGDATA_ON_DISK(true|false)]: JSON for NEON_LOCAL_COMPUTE_OPTS: the
# pageserver shards (NEON_LOCAL_PS_OPTS, after a shard split), the LFC size, statement logging (on for low
# contention and cold start: the compute log holds per-statement durations; off for the scaling runs) and, for the
# runs with hundreds of computes, the data directory on disk rather than the tmpfs (experiment 5; ignored by an agent
# without a disk compute root). The LFC chunk size is not a per-compute option: the compute agent takes it at start
# (setup), experiment 5's --lfc-chunk.
compute_opts() {
    python3 -c 'import json, os, sys
o = json.loads(os.environ.get("NEON_LOCAL_PS_OPTS") or "{}")
o.update(lfc_mb=int(sys.argv[1]), statement_log=sys.argv[2] == "true")
if sys.argv[3] == "true":
    o["pgdata_on_disk"] = True
print(json.dumps(o, separators=(",", ":")))' "$LFC_MB" "$1" "${2:-false}"
}

summary_path() {  # DIR THREADS BRANCHES OP: the runner's summary file of one run
    echo "$1/${RUNNER_BACKEND}_chw${W}_tp_t$2_b$3_$4_threads$2_summary.json"
}
run_id_of() { echo "${RUNNER_BACKEND}_chw${W}_tp_t$1_b$2"; }  # THREADS BRANCHES

# ---------------------------------------------------------------------------------------------------------------
# Logging, guards
# ---------------------------------------------------------------------------------------------------------------
log() {
    if $DRY; then echo "$(date -u +%FT%TZ) $*"; else echo "$(date -u +%FT%TZ) $*" | tee -a "$PROGRESS"; fi
}

is_done() { [[ -f $PROGRESS ]] && grep -q "^.* DONE  $1 " "$PROGRESS"; }

shared_md5() {
    local f files=(microbench/runner2.py dblib/db_api.py scripts/run_throughput_bench.sh)
    [[ $BACKEND == neon ]] && files+=(dblib/neon_local.py) || files+=(dblib/dolt.py)
    files+=(scripts/workloads/common.sh scripts/workloads/"$WORKLOAD".sh scripts/workloads/"$WORKLOAD".py)
    for f in "${files[@]}"; do
        [[ -f $f ]] && md5sum "$f" | awk '{printf "%s:%s ", $2, substr($1,1,8)}'
    done
}

pid_from() {  # PIDFILE: the pid if that process is alive, else nothing
    local p; p=$(cat "$1" 2>/dev/null) || return 0
    [[ -n $p && -d /proc/$p ]] && echo "$p"
}

doltgres_pidfile() {  # the local Doltgres server's pid file (setup/doltgres.py), DOLTGRES_PIDFILE overrides
    local f
    for f in "${DOLTGRES_PIDFILE:-}" "$STATE_DIR/doltgres/run/doltgres.pid" "$STATE_DIR/doltgres/doltgres.pid" \
             "$STATE_DIR/run/doltgres.pid"; do
        [[ -n $f && -n "$(pid_from "$f")" ]] && { echo "$f"; return; }
    done
}

neon_running() { pgrep -u "$(id -u)" -x 'pageserver|safekeeper|storage_control|storage_broker|compute_ctl' 2>/dev/null; }
doltgres_running() { pgrep -u "$(id -u)" -x doltgres 2>/dev/null; }

setup_ctl() {  # SUBCMD ARGS...: scripts/setup/<backend>.py TOPO SUBCMD ... if that script exists (output to stdout)
    local s=scripts/setup/$BACKEND.py
    [[ -f $s ]] || { echo "($s not present: $1 skipped)"; return 0; }
    uv run python "$s" "$TOPO" "$@"
}

guards() {  # refuse to run unless the client host, the server, the environment and the disk are as intended
    on_client_host || die "run this on the client node $CLIENT_NODE ($CLIENT_HOST), after scripts/setup/$BACKEND.py $TOPO deploy-client"
    DBFORK_SERVER_PID=""
    if [[ $BACKEND == neon ]]; then
        [[ -n ${NEON_LOCAL_TENANT_ID:-} ]] || die "no seed in $ENV_FILE: run scripts/setup/neon.py $TOPO seed and env-init"
        if [[ $SERVER_SAME_HOST == 1 ]]; then
            DBFORK_SERVER_PID=$(pid_from "$NEON_RUN_DIR/pageserver.pid")
            [[ -n $DBFORK_SERVER_PID ]] || die "the pageserver is not running ($NEON_RUN_DIR/pageserver.pid): scripts/setup/neon.py $TOPO start"
            [[ -z "$(doltgres_running)" ]] || die "Doltgres runs on this host: Neon and Doltgres never run together on shared cores"
        fi
    else
        if [[ $SERVER_SAME_HOST == 1 ]]; then
            DOLTGRES_PIDFILE=$(doltgres_pidfile)
            [[ -n $DOLTGRES_PIDFILE ]] || die "Doltgres is not running (no live pid file under $STATE_DIR; set DOLTGRES_PIDFILE): scripts/setup/doltgres.py $TOPO start"
            DBFORK_SERVER_PID=$(pid_from "$DOLTGRES_PIDFILE")
            [[ -z "$(neon_running)" ]] || die "Neon runs on this host: Neon and Doltgres never run together on shared cores"
        fi
    fi
    [[ -n $DBFORK_SERVER_PID ]] && export DBFORK_SERVER_PID || unset DBFORK_SERVER_PID
    if [[ $SERVER_SAME_HOST == 1 && -n $CLIENT_CPUS && -n $SERVER_CPUS ]]; then
        local c; for c in ${SERVER_CPUS//,/ }; do
            [[ ",$CLIENT_CPUS," == *",$c,"* ]] && die "client CPUs include server CPU $c"
        done
    fi
    [[ -z ${DBFORK_REMOTE_CPU:-} ]] && warn "DBFORK_REMOTE_CPU is empty: no server CPU in the summaries"
    disk_guard
    mkdir -p "$STATE_DIR" 2>/dev/null
    exec 9> "$STATE_DIR/workload.lock" || die "cannot open $STATE_DIR/workload.lock"
    flock -n 9 || die "another workload driver holds $STATE_DIR/workload.lock"
}

disk_guard() {  # the data directory (same host) or the output directory must have DISK_GUARD_GB free
    local d=$OUTDIR free
    [[ $SERVER_SAME_HOST == 1 && -d $STATE_DIR ]] && d=$STATE_DIR
    free=$(df -BG --output=avail "$d" | tail -1 | tr -dc 0-9)
    (( free >= DISK_GUARD_GB )) || { log "only ${free} GB free on $d (guard $DISK_GUARD_GB GB): stopping"; exit 1; }
}

mem_guard() {  # this host's available memory must be at least MEM_GUARD_GB
    local avail; avail=$(awk '/^MemAvailable:/ {print int($2 / 1048576)}' /proc/meminfo)
    (( avail >= MEM_GUARD_GB )) || { log "only ${avail} GB memory available (guard $MEM_GUARD_GB GB): stopping"; exit 1; }
}

# ---------------------------------------------------------------------------------------------------------------
# wl_init WORKLOAD: environment, placement, output directories, guards
# ---------------------------------------------------------------------------------------------------------------
wl_init() {
    WORKLOAD=$1
    load_env
    BACKEND=$BACKEND_ARG
    client_bind
    if ! on_client_host; then
        if $DRY; then warn "this is not the client node $CLIENT_NODE ($CLIENT_HOST); a real run must be started there"
        else die "run this on the client node $CLIENT_NODE ($CLIENT_HOST), after scripts/setup/$BACKEND.py $TOPO deploy-client"; fi
    fi
    if $SMOKE; then OUTROOT="$OUTDIR/$BACKEND/smoke/$WORKLOAD"; else OUTROOT="$OUTDIR/$BACKEND/$WORKLOAD"; fi
    PROGRESS="$OUTROOT/progress.log"
    COMMON=(--mode sync --warmup-fraction "$WARMUP_FRACTION" --table-name "$TABLE"
            --updates-per-branch "$UPDATES_PER_BRANCH" --seed-db "$SEED_DB" --generated-scale "$W"
            --setup-parallelism "$SETUP_PARALLELISM")
    DATA_OPS=${OPS_ARG:-READ RANGE_READ INSERT UPDATE}
    DISK_DEV=$(uv run python scripts/setup/topo.py get "$TOPO" "nodes.$SERVER_NODE.disk_dev" 2>/dev/null || true)
    AGENT_NUMA=$SERVER_NUMA
    [[ -z $AGENT_NUMA && $SERVER_MEM =~ ^[0-9]+$ ]] && AGENT_NUMA=$SERVER_MEM
    if $DRY; then
        DBFORK_SERVER_PID="<pid of the ${BACKEND/neon/pageserver} on $SERVER_NODE>"
        [[ $SERVER_SAME_HOST == 1 ]] || DBFORK_SERVER_PID=""
        DOLTGRES_PIDFILE=${DOLTGRES_PIDFILE:-"<doltgres pid file>"}
    else
        mkdir -p "$OUTROOT"/{logs,cpu,metrics} || die "cannot create $OUTROOT"
        guards
    fi
}

# ---------------------------------------------------------------------------------------------------------------
# Per-run CPU sampling: with the server on this host, scripts/setup/cpu_agent.py runs for the length of each run
# and writes cpu/<run>.csv (experiment 11); a remote server's agent runs as a service (setup) and only feeds
# DBFORK_REMOTE_CPU.
# ---------------------------------------------------------------------------------------------------------------
AGENT_PID=""
numa_cpus() {  # N: the CPUs of NUMA node N on this host (empty without N)
    [[ -n $1 ]] || return 0
    if $DRY; then echo "<cpus of NUMA node $1>"; else
        python3 -c 'import sys
from pathlib import Path
spec = Path(f"/sys/devices/system/node/node{sys.argv[1]}/cpulist").read_text().strip()
out = []
for p in spec.split(","):
    a, _, b = p.partition("-")
    out += range(int(a), int(b or a) + 1)
print(",".join(map(str, out)))' "$1"
    fi
}
agent_cmd() {  # NAME: the argv of the per-run CPU agent
    local name=$1
    local srv=${SERVER_CPUS:-$(numa_cpus "$SERVER_NUMA")} cli=${CLIENT_CPUS:-$(numa_cpus "$CLIENT_NUMA")}
    AGENT_ARGV=("${CLIENT_BIND[@]}" python3 scripts/setup/cpu_agent.py --set "server=${srv:-all}")
    [[ -n $cli ]] && AGENT_ARGV+=(--set "clients=$cli")
    if [[ $BACKEND == neon ]]; then AGENT_ARGV+=(--run-dir "$NEON_RUN_DIR")
    else AGENT_ARGV+=(--group-pidfile "doltgres=$DOLTGRES_PIDFILE"); fi
    AGENT_ARGV+=(--out "$OUTROOT/cpu/$name.csv" --port "$CPU_AGENT_PORT")
    [[ -n $DISK_DEV ]] && AGENT_ARGV+=(--disk "$DISK_DEV")
    [[ -n $AGENT_NUMA ]] && AGENT_ARGV+=(--numa-node "$AGENT_NUMA")
}

start_agent() {  # NAME
    AGENT_PID=""
    [[ $SERVER_SAME_HOST == 1 ]] || return 0
    [[ -f scripts/setup/cpu_agent.py ]] || { log "WARN  scripts/setup/cpu_agent.py missing: no CPU sampling for $1"; return 0; }
    if (exec 3<>"/dev/tcp/127.0.0.1/$CPU_AGENT_PORT") 2>/dev/null; then
        log "WARN  port $CPU_AGENT_PORT already serves (a CPU agent service?): no per-run agent for $1"; return 0
    fi
    agent_cmd "$1"
    "${AGENT_ARGV[@]}" > "$OUTROOT/logs/$1.agent.log" 2>&1 &
    AGENT_PID=$!
    sleep 2
    [[ -d /proc/$AGENT_PID ]] || { log "WARN  the CPU agent of $1 exited (logs/$1.agent.log)"; AGENT_PID=""; }
}

stop_agent() {
    [[ -n $AGENT_PID ]] || return 0
    kill -TERM "$AGENT_PID" 2>/dev/null; wait "$AGENT_PID" 2>/dev/null
    AGENT_PID=""
}
trap 'stop_agent' EXIT

# ---------------------------------------------------------------------------------------------------------------
# check SUMMARY CPU_CSV PROCS: the run's pass checks (failed_ops = 0, no failed background op, the intended number
# of client processes, server CPU present; node memory above the guard when the agent recorded it)
# ---------------------------------------------------------------------------------------------------------------
check() {
    uv run python - "$1" "$2" "$3" "$MEM_GUARD_GB" <<'PY'
import csv, json, os, sys
summary, cpu_csv, procs, mem_guard = sys.argv[1], sys.argv[2], int(sys.argv[3]), float(sys.argv[4])
try:
    s = json.load(open(summary))
except (OSError, ValueError) as e:
    print(f"PROBLEMS: no summary {summary} ({e})")
    sys.exit(1)
bg = s.get("background") or {}
rc = s.get("remote_cpu") or {}
spec = os.environ.get("DBFORK_REMOTE_CPU", "")
labels = dict(x.split("=", 1) for x in filter(None, (y.strip() for y in spec.split(";"))))
problems = [m for m, bad in [
    (f"failed_ops={s.get('failed_ops')}", s.get("failed_ops")),
    (f"background failed={bg.get('failed')}", bg.get("failed")),
    (f"num_processes {s.get('num_processes')} != {procs}", s.get("num_processes") != procs),
    ("no server_cpu_cores", os.environ.get("DBFORK_SERVER_PID") and s.get("server_cpu_cores") is None),
] if bad]
for label, url in labels.items():
    v = rc.get(label) or {}
    if "set=server" in url and v.get("node_cores") is None:
        problems.append(f"no remote_cpu.{label}.node_cores")
    if label in ("pageserver", "doltgres") and v.get("proc_cores") is None:
        problems.append(f"no remote_cpu.{label}.proc_cores")
mem = []
procs_csv = cpu_csv[:-4] + ".procs.csv"
if os.path.exists(procs_csv):
    rows = list(csv.DictReader(open(procs_csv)))
    kind = "mem_Available" if any(r.get("group") == "mem_Available" for r in rows) else "mem_MemFree"
    mem = [float(r["cores"]) for r in rows if r.get("group") == kind]
    if mem and min(mem) < mem_guard * 1024:
        problems.append(f"server node memory fell to {min(mem):.0f} MB")
lat = s.get("latency") or {}
q = (lat.get("ops") if s.get("operation") == "BRANCH_CREATE" else lat.get("statements") or lat.get("ops")) or {}
r2 = lambda x: None if x is None else round(x, 2)
cpu = " ".join(f"{k}={r2(v.get('proc_cores'))}/{r2(v.get('node_cores'))}" for k, v in rc.items()
               if v.get("proc_cores") is not None or v.get("node_cores") is not None)
print(f"tput={s.get('throughput', 0):.0f}/s window={s.get('elapsed_time', 0):.1f}s procs={s.get('num_processes')} "
      f"p50={q.get('p50', 0) * 1e3:.3f}ms mean={q.get('mean', 0) * 1e3:.3f}ms "
      f"server_cpu={r2(s.get('server_cpu_cores'))} client_cpu={r2(s.get('client_cpu_cores'))} "
      f"bg_ops={bg.get('ops_in_window')} {cpu} "
      + (f"mem_min={min(mem):.0f}MB " if mem else "")
      + ("PROBLEMS: " + ", ".join(problems) if problems else "ok"))
sys.exit(1 if problems else 0)
PY
}

# set_aside DIR RUN_ID NAME: move the files of an earlier, unfinished attempt of a run out of the way (the runner
# appends to <run_id>.parquet, so a rerun would mix two attempts)
set_aside() {
    local dir=$1 rid=$2 name=$3 f moved=0 dst
    dst="$OUTROOT/superseded/${name}_$(date +%s)"
    shopt -s nullglob
    for f in "$dir/$rid".* "$dir/${rid}"_*; do
        mkdir -p "$dst"; mv "$f" "$dst/"; moved=1
    done
    shopt -u nullglob
    (( moved )) && log "MOVED earlier attempt of $name to ${dst#"$OUTROOT"/}"
    return 0
}

print_cmd() {  # what a dry run shows for one run
    local name=$1; shift
    echo "---- DRY-RUN $name"
    local e; for e in "${RUN_ENV[@]}"; do echo "  env $e"; done
    [[ $BACKEND == neon ]] && echo "  env NEON_LOCAL_COMPUTE_OPTS=${NEON_LOCAL_COMPUTE_OPTS:-}"
    echo "  env DBFORK_SERVER_PID=${DBFORK_SERVER_PID:-} DBFORK_REMOTE_CPU=${DBFORK_REMOTE_CPU:-<from $ENV_FILE>}"
    echo "  env file $ENV_FILE$( [[ -f $ENV_FILE ]] || echo ' (missing)')"
    if [[ $SERVER_SAME_HOST == 1 ]]; then agent_cmd "$name"; echo "  cpu agent: ${AGENT_ARGV[*]} &"; fi
    printf '  $ timeout -k 120 -s INT %s' "$RUN_TIMEOUT"
    local a; for a in "$@"; do
        if [[ $a =~ ^[A-Za-z0-9_./=,:+@-]+$ ]]; then printf ' %s' "$a"; else printf ' %q' "$a"; fi
    done; echo
}

# ---------------------------------------------------------------------------------------------------------------
# run NAME DIR THREADS BRANCHES OP PROCS CMD...: one runner call writing to DIR (RUN_ENV: extra VAR=value for it).
# Progress lines START, DONE (with the check's summary), CHECK-FAIL, FAILED. A run already DONE is skipped by the
# drivers (is_done); files of an earlier unfinished attempt are moved to superseded/ first.
# ---------------------------------------------------------------------------------------------------------------
RUN_ENV=()
run() {
    local name=$1 dir=$2 threads=$3 branches=$4 op=$5 procs=$6; shift 6
    if $DRY; then print_cmd "$name" "${CLIENT_BIND[@]}" "$@"; return 0; fi
    disk_guard; mem_guard
    set_aside "$dir" "$(run_id_of "$threads" "$branches")" "$name"
    log "START $name t=$(date +%s.%N)"
    local t0=$SECONDS rc res summary
    summary=$(summary_path "$dir" "$threads" "$branches" "$op")
    start_agent "$name"
    [[ $BACKEND == neon ]] && setup_ctl metrics "$OUTROOT/metrics/${name}_before.txt" > /dev/null 2>&1
    env "${RUN_ENV[@]}" timeout -k 120 -s INT "$RUN_TIMEOUT" "${CLIENT_BIND[@]}" "$@" > "$OUTROOT/logs/$name.log" 2>&1
    rc=$?
    if (( rc == 0 )); then
        if res=$(check "$summary" "$OUTROOT/cpu/$name.csv" "$procs" 2>&1); then
            log "DONE  $name ($((SECONDS - t0))s) $res t=$(date +%s.%N)"
        else
            log "CHECK-FAIL $name ($((SECONDS - t0))s) $res t=$(date +%s.%N)"
        fi
    else
        log "FAILED $name ($((SECONDS - t0))s, exit $rc, see logs/$name.log) t=$(date +%s.%N)"
        setup_ctl purge >> "$OUTROOT/logs/$name.log" 2>&1
    fi
    [[ $BACKEND == neon ]] && setup_ctl metrics "$OUTROOT/metrics/${name}_after.txt" > /dev/null 2>&1
    stop_agent
    return 0
}

# run_plain NAME CMD...: a run without a runner summary (ceiling check, cold start): DONE on exit 0, else FAILED
run_plain() {
    local name=$1; shift
    if $DRY; then print_cmd "$name" "${CLIENT_BIND[@]}" "$@"; return 0; fi
    disk_guard; mem_guard
    log "START $name t=$(date +%s.%N)"
    local t0=$SECONDS rc
    start_agent "$name"
    env "${RUN_ENV[@]}" timeout -k 120 -s INT "$RUN_TIMEOUT" "${CLIENT_BIND[@]}" "$@" > "$OUTROOT/logs/$name.log" 2>&1
    rc=$?
    if (( rc == 0 )); then log "DONE  $name ($((SECONDS - t0))s) t=$(date +%s.%N)"
    else log "FAILED $name ($((SECONDS - t0))s, exit $rc, see logs/$name.log) t=$(date +%s.%N)"; fi
    stop_agent
    return 0
}

wl_start() {  # log the parameters and the shared code's md5; clean leftovers of earlier runs (Neon)
    log "=== $WORKLOAD ($BACKEND, topology $TOPO_NAME): smoke=$SMOKE $* W=$W SB=$SB LFC_MB=$LFC_MB" \
        "client_procs<=$MAX_PROCS bind='${CLIENT_BIND[*]}' host=$(hostname)"
    log "shared code md5 (start): $(shared_md5)"
    $DRY || setup_ctl purge >> "$OUTROOT/logs/purge_start.log" 2>&1
}

wl_end() {
    $DRY || setup_ctl purge >> "$OUTROOT/logs/purge_end.log" 2>&1
    log "shared code md5 (end):   $(shared_md5)"
    log "=== $WORKLOAD ($BACKEND) done"
}
