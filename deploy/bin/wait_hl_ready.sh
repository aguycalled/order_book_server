#!/bin/bash
# ExecStartPre gate for orderbook.service: block until hl-node has been healthy
# (alive + caught up) continuously for HL_GRACE seconds. This enforces the
# "only restart the order book server a minute or two after hl-node has
# bootstrapped" rule, and prevents orderbook from grabbing data/locks while
# hl-node is mid-restart after a network upgrade.
#
# Exits 0 once ready; exits 1 on overall timeout so systemd retries cleanly.
set -uo pipefail
GRACE=${HL_GRACE:-90}
TIMEOUT=${HL_WAIT_TIMEOUT:-1800}
STALE=${HL_STALE:-60}

start=$(date +%s)
healthy_since=0
while :; do
    if /home/alex/bin/hl_node_healthy.sh "$STALE"; then
        [ "$healthy_since" -eq 0 ] && healthy_since=$(date +%s)
        if [ $(( $(date +%s) - healthy_since )) -ge "$GRACE" ]; then
            echo "wait_hl_ready: hl-node healthy for >=${GRACE}s, proceeding"
            exit 0
        fi
    else
        healthy_since=0
    fi
    if [ $(( $(date +%s) - start )) -ge "$TIMEOUT" ]; then
        echo "wait_hl_ready: timed out after ${TIMEOUT}s waiting for hl-node"
        exit 1
    fi
    sleep 5
done
