#!/bin/bash
# hl-coordinator: keeps hl-visor alive and coordinates orderbook.service around
# hl-node restarts (network upgrades, crashes).
#
# Loop every INTERVAL seconds:
#   1. If hl-visor is not running, relaunch it (hl-visor is what downloads new
#      hl-node binaries on a network upgrade and restarts hl-node; if hl-visor
#      itself dies, nothing restarts the node).
#   2. If hl-node is unhealthy (down / stalled / replaying after an upgrade),
#      stop orderbook.service so it releases its data dir handles and lets the
#      new hl-node start cleanly.
#   3. If hl-node is healthy and caught up, ensure orderbook.service is started.
#      orderbook.service's ExecStartPre (wait_hl_ready) enforces the post-restart
#      grace period before it actually serves.
#
# DRY_RUN=1 -> log decisions only, take no action (for validation).
set -uo pipefail

INTERVAL=${HL_COORD_INTERVAL:-15}
STALE=${HL_STALE:-60}
DRY_RUN=${DRY_RUN:-0}
LOGTAG="hl-coordinator"

HL_VISOR_CMD='/home/alex/hl-visor run-non-validator --write-fills --write-order-statuses --write-raw-book-diffs --stream-with-block-info --disable-output-file-buffering --replica-cmds-style actions-and-responses --serve-info --write-system-and-core-writer-actions --write-misc-events --write-hip3-oracle-updates'

log() { echo "$(date -u +%FT%TZ) [$LOGTAG] $*"; }

ob_active() { systemctl --user is-active --quiet orderbook.service; }

start_ob() {
    if [ "$DRY_RUN" = 1 ]; then log "DRY_RUN: would start orderbook.service"; return; fi
    log "starting orderbook.service"
    systemctl --user start orderbook.service
}

stop_ob() {
    if [ "$DRY_RUN" = 1 ]; then log "DRY_RUN: would stop orderbook.service"; return; fi
    log "stopping orderbook.service (hl-node not healthy)"
    systemctl --user stop orderbook.service
}

relaunch_visor() {
    if [ "$DRY_RUN" = 1 ]; then log "DRY_RUN: would relaunch hl-visor"; return; fi
    log "hl-visor not running -> relaunching"
    local vlog="/home/alex/logs/hl-visor_$(date +%F).log"
    cd /home/alex || return
    setsid bash -c "exec $HL_VISOR_CMD < /dev/null >> $vlog 2>&1" &
    disown || true
}

log "coordinator started (interval=${INTERVAL}s stale=${STALE}s dry_run=${DRY_RUN})"
while :; do
    if ! pgrep -x hl-visor >/dev/null 2>&1; then
        relaunch_visor
    fi

    if /home/alex/bin/hl_node_healthy.sh "$STALE"; then
        if ! ob_active; then
            log "hl-node healthy, orderbook inactive -> start"
            start_ob
        fi
    else
        if ob_active; then
            log "hl-node unhealthy, orderbook active -> stop"
            stop_ob
        fi
    fi

    sleep "$INTERVAL"
done
