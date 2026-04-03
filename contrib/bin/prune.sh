#!/bin/bash
DATA_PATH="/home/alex/hl/data"

# Folders to exclude from pruning
# Example: EXCLUDES=("visor_child_stderr" "rate_limited_ips" "node_logs")
EXCLUDES=("visor_child_stderr")

# Log startup for debugging
echo "$(date): Prune script started"

# Check if data directory exists
if [ ! -d "$DATA_PATH" ]; then
    echo "$(date): Error: Data directory $DATA_PATH does not exist."
    exit 1
fi

echo "$(date): Starting pruning process at $(date)"

# Get directory size before pruning
size_before=$(du -sh "$DATA_PATH" | cut -f1)
files_before=$(find "$DATA_PATH" -type f | wc -l)
echo "$(date): Size before pruning: $size_before with $files_before files"

# Build the -prune arguments for excluding directories
PRUNE_ARGS=()
for dir in "${EXCLUDES[@]}"; do
    PRUNE_ARGS+=(-path "*/$dir" -prune -o)
done

# Delete data older than 6 hours = 60 minutes * 2 hours
HOURS=$((60*2))
find "$DATA_PATH" -mindepth 1 "${PRUNE_ARGS[@]}" -type f -mmin +$HOURS -exec rm {} +

# Get directory size after pruning
size_after=$(du -sh "$DATA_PATH" | cut -f1)
files_after=$(find "$DATA_PATH" -type f | wc -l)
echo "$(date): Size after pruning: $size_after with $files_after files"
echo "$(date): Pruning completed. Reduced from $size_before to $size_after ($(($files_before - $files_after)) files removed)."


LOG_DIR="/home/alex/logs"
RETENTION_DAYS=7

echo "[$(date -Is)] Starting log pruning..."

# delete .log files older than N days
find "$LOG_DIR" -type f -name "*.log" -mtime +"$RETENTION_DAYS" -print -delete

echo "[$(date -Is)] Done."
