#!/usr/bin/env bash
# Collect experiment 13's results on the coordinator: the driver's output tree from the client node (n1, which also
# holds every other node's per-point results: multi_scaling.py rsyncs them back), each node's results directory
# (in case a fetch was missed), and every agent's archive (forks.jsonl: one line per copy, failures, kept logs).
#
#   pull.sh [SUBDIR]      SUBDIR of /mydata/exp13 on the nodes (default: everything) -> data/ (= /mydata/exp13 here)
set -euo pipefail
X="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$X/../.." && pwd)"
TOPO=$REPO/scripts/setup/topologies/cloudlab_pgcopy.toml
SUB=${1:-}
DATA=$(readlink -f "$X/data")
cd "$REPO"
export PATH=$HOME/.local/bin:$PATH
SSH="ssh -i $HOME/.ssh/dassl_rsa -o BatchMode=yes"
user=$(uv run python scripts/setup/topo.py get "$TOPO" ssh.user)
for n in n1 n2 n3 n4 n5 n6 n7 n8 n9 n10 n11 n12 n13 n14 n15; do
    host=$(uv run python scripts/setup/topo.py get "$TOPO" "nodes.$n.host")
    if [[ $n == n1 ]]; then dst="$DATA/$SUB"; else dst="$DATA/nodes/$n/$SUB"; fi
    mkdir -p "$dst"
    rsync -a -e "$SSH" "$user@$host:/mydata/exp13/$SUB/" "$dst/" 2>/dev/null || echo "$n: nothing under /mydata/exp13/$SUB"
done
uv run python scripts/setup/pgcopy.py "$TOPO" pull-archive "$DATA/archive"
uv run python scripts/setup/pgcopy.py "$TOPO" status > "$X/logs/status_at_pull_$(date +%Y%m%dT%H%M%S).txt" 2>&1 || true
echo "pulled into $DATA"
