#!/usr/bin/env python3
"""
Testnet harness for reverse-engineering Hyperliquid's deterministic state transitions.

Usage:
  export HL_TESTNET_KEY="0x..."
  python3 testnet_harness.py snapshot          # dump current state
  python3 testnet_harness.py compare           # take snapshot, wait for input, take another, diff
  python3 testnet_harness.py order <coin> <side> <sz> <px>  # place an order and show state diff
  python3 testnet_harness.py set-leverage <coin> <leverage> [--isolated]
"""

import json
import os
import sys
import time
import requests
from typing import Optional

TESTNET_INFO = "https://api.hyperliquid-testnet.xyz/info"
TESTNET_EXCHANGE = "https://api.hyperliquid-testnet.xyz/exchange"


def info_request(req_type: str, user: str, **kwargs) -> dict:
    body = {"type": req_type, "user": user, **kwargs}
    r = requests.post(TESTNET_INFO, json=body)
    r.raise_for_status()
    return r.json()


def get_full_state(user: str) -> dict:
    """Get complete user state across all dexes and spot."""
    state = {}

    # Perp clearinghouse state (dex 0)
    ch = info_request("clearinghouseState", user)
    state["dex0"] = ch

    # Try hip-3 dexes
    for dex_name in ["xyz", "flx", "vntl", "hyna", "km", "cash", "para"]:
        try:
            ch_hip3 = info_request("clearinghouseState", user, dex=dex_name)
            if ch_hip3 and ch_hip3.get("assetPositions"):
                state[f"dex_{dex_name}"] = ch_hip3
        except Exception:
            pass

    # Spot clearinghouse state
    try:
        spot = info_request("spotClearinghouseState", user)
        state["spot"] = spot
    except Exception:
        pass

    # Sub-accounts (includes detailed balance info)
    try:
        sub = info_request("subAccounts", user)
        state["subAccounts"] = sub
    except Exception:
        pass

    return state


def extract_key_fields(state: dict) -> dict:
    """Extract the fields we care about for comparison."""
    fields = {}

    for key, ch in state.items():
        if key.startswith("dex") or key.startswith("dex_"):
            ms = ch.get("marginSummary", {})
            cms = ch.get("crossMarginSummary", {})
            fields[f"{key}.accountValue"] = ms.get("accountValue", "0")
            fields[f"{key}.totalRawUsd"] = ms.get("totalRawUsd", "0")
            fields[f"{key}.withdrawable"] = ch.get("withdrawable", "0")
            fields[f"{key}.crossAccountValue"] = cms.get("accountValue", "0")
            fields[f"{key}.crossTotalRawUsd"] = cms.get("totalRawUsd", "0")

            for ap in ch.get("assetPositions", []):
                pos = ap.get("position", {})
                coin = pos.get("coin", "?")
                szi = pos.get("szi", "0")
                entry = pos.get("entryPx", "0")
                lev = pos.get("leverage", {})
                lev_type = lev.get("type", "?")
                lev_val = lev.get("value", 0)
                raw_usd = lev.get("rawUsd", "0")
                cum_fund = pos.get("cumFunding", {})
                fields[f"{key}.{coin}.szi"] = szi
                fields[f"{key}.{coin}.entryPx"] = entry
                fields[f"{key}.{coin}.leverage"] = f"{lev_type}({lev_val})"
                fields[f"{key}.{coin}.rawUsd"] = raw_usd
                fields[f"{key}.{coin}.cumFunding.sinceChange"] = cum_fund.get("sinceChange", "0")
                fields[f"{key}.{coin}.cumFunding.sinceOpen"] = cum_fund.get("sinceOpen", "0")

        elif key == "spot":
            balances = ch.get("balances", [])
            for b in balances:
                coin = b.get("coin", "?")
                total = b.get("total", "0")
                fields[f"spot.{coin}.total"] = total

    return fields


def diff_states(before: dict, after: dict) -> list:
    """Compare two extracted field dicts, return list of changes."""
    all_keys = sorted(set(list(before.keys()) + list(after.keys())))
    diffs = []
    for k in all_keys:
        v1 = before.get(k, "<missing>")
        v2 = after.get(k, "<missing>")
        if v1 != v2:
            try:
                f1 = float(v1) if v1 != "<missing>" else 0.0
                f2 = float(v2) if v2 != "<missing>" else 0.0
                delta = f2 - f1
                diffs.append(f"  {k}: {v1} -> {v2} (delta={delta:+.8f})")
            except ValueError:
                diffs.append(f"  {k}: {v1} -> {v2}")
    return diffs


def print_state(state: dict):
    fields = extract_key_fields(state)
    for k in sorted(fields.keys()):
        print(f"  {k} = {fields[k]}")


def cmd_snapshot(user: str):
    print(f"Fetching state for {user}...")
    state = get_full_state(user)
    print("\n=== Full State ===")
    print_state(state)
    print("\n=== Raw JSON ===")
    print(json.dumps(state, indent=2))


def cmd_compare(user: str):
    print(f"Taking BEFORE snapshot for {user}...")
    before_state = get_full_state(user)
    before_fields = extract_key_fields(before_state)
    print("BEFORE state:")
    print_state(before_state)

    input("\n>>> Press Enter after performing an action on testnet...\n")

    print(f"Taking AFTER snapshot for {user}...")
    after_state = get_full_state(user)
    after_fields = extract_key_fields(after_state)
    print("AFTER state:")
    print_state(after_state)

    print("\n=== DIFFS ===")
    diffs = diff_states(before_fields, after_fields)
    if diffs:
        for d in diffs:
            print(d)
    else:
        print("  (no changes)")


def cmd_order(user: str, private_key: str, coin: str, side: str, sz: str, px: str):
    """Place order and show state diff. Requires hyperliquid-python-sdk."""
    try:
        from hyperliquid.utils import constants
        from hyperliquid.exchange import Exchange
        from hyperliquid.info import Info
        import eth_account
    except ImportError:
        print("Install: pip install hyperliquid-python-sdk eth-account")
        sys.exit(1)

    account = eth_account.Account.from_key(private_key)
    info = Info(constants.TESTNET_API_URL, skip_ws=True)
    exchange = Exchange(account, constants.TESTNET_API_URL)

    print(f"Taking BEFORE snapshot...")
    before_state = get_full_state(user)
    before_fields = extract_key_fields(before_state)

    is_buy = side.lower() in ("buy", "b", "bid")
    print(f"Placing order: {coin} {'BUY' if is_buy else 'SELL'} {sz} @ {px}")

    result = exchange.order(coin, is_buy, float(sz), float(px), {"limit": {"tif": "Ioc"}})
    print(f"Order result: {json.dumps(result, indent=2)}")

    time.sleep(2)  # wait for state to settle

    print(f"Taking AFTER snapshot...")
    after_state = get_full_state(user)
    after_fields = extract_key_fields(after_state)

    print("\n=== DIFFS ===")
    diffs = diff_states(before_fields, after_fields)
    if diffs:
        for d in diffs:
            print(d)
    else:
        print("  (no changes — order may not have filled)")


def main():
    private_key = os.environ.get("HL_TESTNET_KEY", "")
    if not private_key:
        print("Set HL_TESTNET_KEY environment variable")
        sys.exit(1)

    # Derive address from key
    try:
        import eth_account
        account = eth_account.Account.from_key(private_key)
        user = account.address.lower()
    except ImportError:
        print("Install: pip install eth-account")
        sys.exit(1)

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "snapshot":
        cmd_snapshot(user)
    elif cmd == "compare":
        cmd_compare(user)
    elif cmd == "order" and len(sys.argv) >= 6:
        cmd_order(user, private_key, sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
