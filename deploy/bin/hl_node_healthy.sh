#!/bin/bash
# Exit 0 if hl-node is alive AND caught up to real time, else exit 1.
#
# Signal: hl-visor rewrites hyperliquid_data/visor_abci_state.json every block
# with the block's wall_clock_time. When hl-node is down, stalled, or still
# replaying historical blocks after a restart/upgrade, that timestamp goes
# stale. So "fresh wall_clock_time" == "alive and caught up to chain head",
# which is exactly when the order book server should be running.
#
# Usage: hl_node_healthy.sh [max_age_seconds]   (default 60)
set -euo pipefail
STATE=/home/alex/hl/hyperliquid_data/visor_abci_state.json
MAX_AGE=${1:-60}

# hl-node process must exist (name is <=15 chars so pgrep -x works)
pgrep -x hl-node >/dev/null 2>&1 || exit 1

[ -f "$STATE" ] || exit 1

age=$(python3 - "$STATE" <<'PY'
import json, sys, datetime
try:
    d = json.load(open(sys.argv[1]))
    # visor_abci_state.json wall_clock_time is naive UTC; pin both sides to UTC
    wc = datetime.datetime.fromisoformat(d["wall_clock_time"]).replace(tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    print(int((now - wc).total_seconds()))
except Exception:
    print(10**9)
PY
)

[ "$age" -le "$MAX_AGE" ] || exit 1
exit 0
