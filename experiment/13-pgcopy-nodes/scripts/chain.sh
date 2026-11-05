#!/usr/bin/env bash
# Experiment 13 end to end from the coordinator (pc20): setup, smoke, the full run on n1 (detached), then collect.
#   chain.sh setup        install, start, seed, deploy-client, smoke (each step logged to logs/)
#   chain.sh smoke        run_all --smoke --multi on n1 into /mydata/exp13/smoke (waits for it)
#   chain.sh full         run_all --multi on n1 into /mydata/exp13/run (detached unit exp13-full; returns at once)
#   chain.sh pull         scripts/pull.sh
set -euo pipefail
X="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$X/../.." && pwd)"
T=scripts/setup/topologies/cloudlab_pgcopy.toml
cd "$REPO"
export PATH=$HOME/.local/bin:$PATH
P=(uv run python scripts/setup/pgcopy.py "$T")
RUN_ALL=(scripts/workloads/run_all.sh --topology "$T" --backend pgcopy --multi)
case ${1:-} in
    setup)
        for s in install start seed deploy-client smoke; do
            echo "== $s"; "${P[@]}" "$s" 2>&1 | tee "$X/logs/$s.log"
        done ;;
    smoke)
        "$X/scripts/launch.sh" n1 exp13-smoke /mydata/exp13/smoke/run.out "${RUN_ALL[@]}" --outdir /mydata/exp13/smoke --smoke ;;
    full)
        "${P[@]}" status > "$X/logs/status_before_full.txt" 2>&1
        "$X/scripts/launch.sh" n1 exp13-full /mydata/exp13/run/run.out "${RUN_ALL[@]}" --outdir /mydata/exp13/run ;;
    pull)
        "$X/scripts/pull.sh" ;;
    *) echo "usage: chain.sh setup|smoke|full|pull" >&2; exit 2 ;;
esac
