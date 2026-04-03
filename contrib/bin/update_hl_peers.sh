#!/usr/bin/env bash
set -Eeuo pipefail

# update_hl_peers.sh
# Refresh ~/override_gossip_config.json for a Hyperliquid node
#
# Usage:
#   bash update_hl_peers.sh
# Optional env:
#   CONFIG_PATH=/home/ubuntu/override_gossip_config.json
#   CHAIN=Mainnet
#   TRY_NEW_PEERS=true
#   API_URL=https://api.hyperliquid.xyz/info

CONFIG_PATH="${CONFIG_PATH:-$HOME/override_gossip_config.json}"
CHAIN="${CHAIN:-Mainnet}"
TRY_NEW_PEERS="${TRY_NEW_PEERS:-true}"
API_URL="${API_URL:-https://api.hyperliquid.xyz/info}"

TMP_JSON="$(mktemp)"
TMP_OUT="$(mktemp)"
BACKUP_PATH="${CONFIG_PATH}.bak.$(date +%Y%m%d-%H%M%S)"

cleanup() {
  rm -f "$TMP_JSON" "$TMP_OUT"
}
trap cleanup EXIT

# Your preferred peers
PREFERRED_IPS=(
  "64.31.48.111"
  "64.31.51.137"
  "180.189.55.18"
  "180.189.55.19"
  "72.46.86.185"
  "72.46.86.159"
  "13.230.78.76"
  "52.195.133.97"
  "52.68.71.160"
  "13.114.116.44"
  "79.127.159.173"
  "79.127.159.174"
  "23.81.40.69"
  "157.90.207.92"
  "109.123.230.189"
  "31.223.196.172"
  "31.223.196.238"
  "91.134.71.237"
  "57.129.140.247"
  "67.213.123.85"
  "72.46.87.141"
  "199.254.199.12"
  "199.254.199.54"
  "45.250.255.111"
  "109.94.99.131"
  "23.81.41.3"
  "15.235.231.247"
  "199.254.199.48"
  "64.34.83.57"
  "15.235.232.101"
)

echo "Fetching peers from ${API_URL} ..."
curl -fsS \
  -X POST \
  -H 'Content-Type: application/json' \
  --data '{ "type": "gossipRootIps" }' \
  "$API_URL" > "$TMP_JSON"

python3 - "$TMP_JSON" "$TMP_OUT" "$CHAIN" "$TRY_NEW_PEERS" "${PREFERRED_IPS[@]}" <<'PY'
import ipaddress
import json
import sys

api_json_path = sys.argv[1]
out_path = sys.argv[2]
chain = sys.argv[3]
try_new_peers_raw = sys.argv[4].strip().lower()
preferred_ips = sys.argv[5:]

def valid_ip(ip: str) -> bool:
    try:
        ipaddress.ip_address(ip)
        return True
    except Exception:
        return False

with open(api_json_path, "r", encoding="utf-8") as f:
    api_data = json.load(f)

# Accept a few plausible shapes defensively:
# - [{"Ip":"1.2.3.4"}, ...]
# - ["1.2.3.4", ...]
# - {"root_node_ips":[...]}
api_ips = []

if isinstance(api_data, list):
    for item in api_data:
        if isinstance(item, dict) and "Ip" in item and valid_ip(str(item["Ip"])):
            api_ips.append(str(item["Ip"]))
        elif isinstance(item, str) and valid_ip(item):
            api_ips.append(item)

elif isinstance(api_data, dict):
    # Common fallback shape
    candidates = api_data.get("root_node_ips") or api_data.get("ips") or api_data.get("peers") or []
    if isinstance(candidates, list):
        for item in candidates:
            if isinstance(item, dict) and "Ip" in item and valid_ip(str(item["Ip"])):
                api_ips.append(str(item["Ip"]))
            elif isinstance(item, str) and valid_ip(item):
                api_ips.append(item)

merged = []
seen = set()

for ip in preferred_ips + api_ips:
    if valid_ip(ip) and ip not in seen:
        seen.add(ip)
        merged.append({"Ip": ip})

out = {
    "root_node_ips": merged,
    "try_new_peers": try_new_peers_raw == "true",
    "chain": chain,
}

with open(out_path, "w", encoding="utf-8") as f:
    json.dump(out, f, indent=2)
    f.write("\n")
PY

if [[ -f "$CONFIG_PATH" ]]; then
  cp -a "$CONFIG_PATH" "$BACKUP_PATH"
fi

RETENTION_DAYS=7
KEEP_BACKUPS=10

BACKUP_DIR="$(dirname "$CONFIG_PATH")"
BACKUP_BASE="$(basename "$CONFIG_PATH").bak.*"

# delete old by age
find "$BACKUP_DIR" -type f -name "$BACKUP_BASE" -mtime +"$RETENTION_DAYS" -delete

# enforce max count
ls -1t "$BACKUP_DIR"/$BACKUP_BASE 2>/dev/null | tail -n +$((KEEP_BACKUPS+1)) | xargs -r rm -f

install -m 600 "$TMP_OUT" "$CONFIG_PATH"

echo "Updated: $CONFIG_PATH"
echo "Peer count: $(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["root_node_ips"]))' "$CONFIG_PATH")"
[[ -f "$BACKUP_PATH" ]] && echo "Backup: $BACKUP_PATH"
