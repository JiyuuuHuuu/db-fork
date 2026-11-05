#!/usr/bin/env bash
# Start a long command on a cluster node as a transient systemd unit, so it survives the coordinator's ssh session
# (the coordinator, pc20 at UMass, is 51 ms away and may disconnect). Output goes to a file on that node.
#
#   launch.sh NODE UNIT OUTFILE CMD...      e.g. launch.sh n1 exp13-smoke /mydata/exp13/smoke/run.out \
#                                                 scripts/workloads/run_all.sh --topology ... --multi
#   launch.sh NODE --status UNIT            systemctl status + the last lines of its output
# CMD runs in ~/db-fork on the node, as the ssh user, with ~/.local/bin on PATH.
set -euo pipefail
X="$(cd "$(dirname "$0")/.." && pwd)"
REPO="$(cd "$X/../.." && pwd)"
TOPO=${TOPO:-$REPO/scripts/setup/topologies/cloudlab_pgcopy.toml}
node=$1; shift
host=$(cd "$REPO" && uv run python scripts/setup/topo.py get "$TOPO" "nodes.$node.host")
user=$(cd "$REPO" && uv run python scripts/setup/topo.py get "$TOPO" ssh.user)
SSH=(ssh -i "$HOME/.ssh/dassl_rsa" -o BatchMode=yes "$user@$host")
if [[ $1 == --status ]]; then
    "${SSH[@]}" "systemctl status --no-pager $2 | head -5; f=\$(systemctl show -p Description --value $2); tail -5 \"\${f#out=}\" 2>/dev/null"
    exit 0
fi
unit=$1 out=$2; shift 2
cmd=$(printf '%q ' "$@")
"${SSH[@]}" "mkdir -p \$(dirname $out) && sudo -n systemd-run --unit=$unit --description=out=$out --uid=\$(id -u) \
    --gid=\$(id -g) --working-directory=\$HOME/db-fork -p LimitNOFILE=1048576 -p TasksMax=infinity \
    -E HOME=\$HOME -E USER=\$(id -un) -E PATH=\$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin \
    bash -c 'exec $cmd > $out 2>&1'"
echo "started $unit on $node ($host); output $out"
