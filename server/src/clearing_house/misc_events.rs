//! Parser for `misc_events_streaming` data.
//!
//! Misc events contain funding distributions, liquidations, vault operations,
//! rewards claims, and other state changes NOT captured by replica_cmds.
//! Events that ARE already in replica_cmds (deposits, withdrawals, transfers,
//! leverage updates) are skipped here to avoid double-counting.

use crate::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashSet},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use super::LiquidationState;

// ── Serde types ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct MiscEventBatch {
    pub block_number: u64,
    pub events: Vec<MiscEvent>,
}

#[derive(Debug, Deserialize)]
pub struct MiscEvent {
    pub inner: MiscEventInner,
}

#[derive(Debug, Deserialize)]
pub enum MiscEventInner {
    Funding(FundingEvent),
    LedgerUpdate(LedgerUpdateEvent),
    CDeposit(Value),
    CWithdrawal(Value),
    Delegation(Value),
    ValidatorRewards(Value),
}

#[derive(Debug, Deserialize)]
pub struct FundingEvent {
    pub deltas: Vec<FundingDelta>,
}

#[derive(Debug, Deserialize)]
pub struct FundingDelta {
    pub user: String,
    pub coin: String,
    pub funding_amount: String,
    pub szi: String,
    #[serde(default)]
    pub funding_rate: String,
}

#[derive(Debug, Deserialize)]
pub struct LedgerUpdateEvent {
    pub users: Vec<String>,
    pub delta: LedgerDelta,
}

/// We only parse the fields we need. The `type` tag determines which variant.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum LedgerDelta {
    // ── Events NOT in replica (we must process these) ────────────
    #[serde(alias = "liquidation")]
    Liquidation {
        #[serde(default)]
        #[serde(rename = "liquidatedNtlPos")]
        liquidated_ntl_pos: Option<f64>,
        #[serde(default)]
        #[serde(rename = "accountValue")]
        account_value: Option<f64>,
        #[serde(default)]
        #[serde(rename = "leverageType")]
        leverage_type: Option<String>,
        #[serde(default)]
        #[serde(rename = "liquidatedPositions")]
        liquidated_positions: Vec<LiquidatedPosition>,
    },
    #[serde(alias = "vaultDistribution")]
    VaultDistribution { vault: String, usdc: String },
    #[serde(alias = "vaultLeaderCommission")]
    VaultLeaderCommission { vault: String, usdc: String },
    #[serde(alias = "rewardsClaim")]
    RewardsClaim { amount: String },
    #[serde(alias = "deployGasAuction")]
    DeployGasAuction { token: String, amount: String },
    #[serde(alias = "accountActivationGas")]
    AccountActivationGas { amount: String, token: String },

    // ── Events already handled by replica (skip to avoid double-counting) ──
    #[serde(alias = "deposit")]
    Deposit(Value),
    #[serde(alias = "withdraw")]
    Withdraw(Value),
    #[serde(alias = "internalTransfer")]
    InternalTransfer(Value),
    #[serde(alias = "subAccountTransfer")]
    SubAccountTransfer(Value),
    #[serde(alias = "spotTransfer")]
    SpotTransfer(Value),
    #[serde(rename = "send")]
    Send(Value),
    #[serde(alias = "accountClassTransfer")]
    AccountClassTransfer(Value),
    #[serde(alias = "vaultCreate")]
    VaultCreate(Value),
    #[serde(alias = "vaultDeposit")]
    VaultDeposit(Value),
    #[serde(alias = "vaultWithdraw")]
    VaultWithdraw(Value),
    #[serde(alias = "borrowLend")]
    BorrowLend(Value),
    #[serde(alias = "spotGenesis")]
    SpotGenesis(Value),
    #[serde(alias = "cStakingTransfer")]
    CStakingTransfer(Value),
    #[serde(alias = "perpDexClassTransfer")]
    PerpDexClassTransfer(Value),

    // Catch-all for unknown types
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub struct LiquidatedPosition {
    pub coin: String,
    pub szi: f64,
}

pub fn extract_raw_debug_misc_events(
    data_dir: &Path,
    debug_users: &HashSet<String>,
    from_block: u64,
    to_block: u64,
) -> Result<String> {
    if debug_users.is_empty() {
        return Ok(String::new());
    }

    let mut sections: BTreeMap<String, Vec<String>> =
        debug_users.iter().cloned().map(|user| (user, Vec::new())).collect();
    let misc_dir = data_dir.join("misc_events_streaming");

    if misc_dir.exists() {
        let mut files = Vec::new();
        collect_files_recursive(&misc_dir, &mut files);
        files.sort();

        for path in &files {
            let file = match fs::File::open(path) {
                Ok(file) => file,
                Err(_) => continue,
            };

            let reader = BufReader::new(file);
            for (line_idx, line) in reader.lines().enumerate() {
                let line = match line {
                    Ok(line) => line,
                    Err(_) => break,
                };
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let Ok(batch) = serde_json::from_str::<Value>(trimmed) else {
                    continue;
                };
                let Some(block_number) = batch.get("block_number").and_then(Value::as_u64) else {
                    continue;
                };
                if block_number <= from_block || block_number > to_block {
                    continue;
                }

                let Some(events) = batch.get("events").and_then(Value::as_array) else {
                    continue;
                };
                for (event_idx, event) in events.iter().enumerate() {
                    let related_users = related_debug_users_in_value(event, debug_users);
                    if related_users.is_empty() {
                        continue;
                    }

                    let (kind, replay_status) = classify_misc_event(event);
                    if replay_status == "applied" {
                        continue;
                    }
                    let relative_path = path.strip_prefix(data_dir).unwrap_or(path);
                    let pretty = indent_json(event);
                    let section = format!(
                        "  block={block_number} file={} line={} event_idx={} kind={} replay_status={}\n{}",
                        relative_path.display(),
                        line_idx + 1,
                        event_idx,
                        kind,
                        replay_status,
                        pretty
                    );

                    for user in related_users {
                        if let Some(user_sections) = sections.get_mut(&user) {
                            user_sections.push(section.clone());
                        }
                    }
                }
            }
        }
    }

    let mut out = String::from("=== RAW RELATED UNPROCESSED MISC EVENTS ===\n");
    for (user, user_sections) in sections {
        out.push_str(&format!("=== USER {} ===\n", user));
        if user_sections.is_empty() {
            out.push_str("  (no related unprocessed misc events found in replay range)\n");
        } else {
            for section in user_sections {
                out.push_str(&section);
                if !section.ends_with('\n') {
                    out.push('\n');
                }
                out.push('\n');
            }
        }
    }

    Ok(out)
}

// ── Apply misc events to state ───────────────────────────────────────────

pub fn apply_misc_event_batch(state: &mut LiquidationState, batch: &MiscEventBatch) -> (u64, u64) {
    let mut n_funding = 0u64;
    let mut n_ledger = 0u64;

    for event in &batch.events {
        match &event.inner {
            MiscEventInner::Funding(funding) => {
                apply_funding(state, funding);
                n_funding += 1;
            }
            MiscEventInner::LedgerUpdate(ledger) => {
                if apply_ledger_update(state, ledger) {
                    n_ledger += 1;
                }
            }
            // CDeposit/CWithdrawal/Delegation/ValidatorRewards — handled by replica or no perps effect
            _ => {}
        }
    }

    (n_funding, n_ledger)
}

fn apply_funding(state: &mut LiquidationState, funding: &FundingEvent) {
    for delta in &funding.deltas {
        let amt: f64 = delta.funding_amount.parse().unwrap_or(0.0);
        if amt == 0.0 {
            continue;
        }
        let micro = (amt * 1e6).round() as i64;
        let user = delta.user.to_lowercase();

        let coin = &delta.coin;
        if let Some(&(dex_idx, asset_idx)) = state.coin_to_dex_asset.get(coin) {
            let dex = &mut state.dex_states[dex_idx];
            if let Some(user_state) = dex.users.get_mut(&user) {
                if let Some(pos) = user_state.positions.get_mut(&(asset_idx as u32)) {
                    pos.outstanding_funding += micro;
                    match &mut pos.leverage {
                        super::Leverage::Isolated { raw_usd, .. } => {
                            *raw_usd += micro;
                        }
                        super::Leverage::Cross(_) => {
                            user_state.usdc_balance += micro;
                        }
                    }
                } else {
                    user_state.usdc_balance += micro;
                }
                if state.debug_users.contains(&user) {
                    eprintln!("[DEBUG funding] user={} coin={} amt=${:.6} micro={}", user, coin, amt, micro);
                }
            }
        }
    }
}

fn apply_ledger_update(state: &mut LiquidationState, ledger: &LedgerUpdateEvent) -> bool {
    match &ledger.delta {
        LedgerDelta::Liquidation { leverage_type, .. } => {
            // Liquidation zeroes out the user's positions and adjusts balance.
            // The actual position changes come through fills (ADL fills).
            // The balance effect is captured in the liquidation event.
            // However, for isolated liquidations, the raw_usd is returned to cross.
            // We handle this in apply_fill's ADL detection (startPosition=0).
            // So we don't need to do anything extra here — just log it.
            for user in &ledger.users {
                let u = user.to_lowercase();
                if state.debug_users.contains(&u) {
                    eprintln!("[DEBUG liquidation] user={} type={:?}", u, leverage_type);
                }
            }
            true
        }
        LedgerDelta::VaultDistribution { vault, usdc } => {
            let amt: f64 = usdc.parse().unwrap_or(0.0);
            let micro = (amt * 1e6).round() as i64;
            let vault_addr = vault.to_lowercase();
            // Distribution goes from vault to users, but we only see the vault side
            // Vault balance decreases
            state.apply_usd_transfer(&vault_addr, -micro);
            // The user distributions are sent individually — but misc_events
            // shows them as a lump sum. The individual payouts appear as
            // separate ledger updates or are implicit.
            // Actually, the "users" field lists who gets the distribution.
            // For now, distribute evenly (this may not be exact).
            // TODO: The distribution amounts per user aren't in this event.
            // For vault distributions, the vault balance decreases and
            // user balances increase. Since we don't know per-user amounts,
            // skip the user side — the balance will drift by this amount.
            true
        }
        LedgerDelta::VaultLeaderCommission { vault, usdc } => {
            let amt: f64 = usdc.parse().unwrap_or(0.0);
            let micro = (amt * 1e6).round() as i64;
            // Commission goes from vault to leader
            let vault_addr = vault.to_lowercase();
            state.apply_usd_transfer(&vault_addr, -micro);
            // Leader gets the commission — first user in the list is typically the leader
            if let Some(leader) = ledger.users.first() {
                let leader_addr = leader.to_lowercase();
                state.apply_usd_transfer(&leader_addr, micro);
            }
            true
        }
        LedgerDelta::RewardsClaim { amount } => {
            let amt: f64 = amount.parse().unwrap_or(0.0);
            let micro = (amt * 1e6).round() as i64;
            // Rewards go to the user's perps balance
            for user in &ledger.users {
                let u = user.to_lowercase();
                state.apply_usd_transfer(&u, micro);
            }
            true
        }
        LedgerDelta::DeployGasAuction { amount, .. } => {
            let amt: f64 = amount.parse().unwrap_or(0.0);
            let micro = (amt * 1e6).round() as i64;
            for user in &ledger.users {
                let u = user.to_lowercase();
                state.apply_usd_transfer(&u, -micro);
            }
            true
        }
        LedgerDelta::AccountActivationGas { amount, .. } => {
            let amt: f64 = amount.parse().unwrap_or(0.0);
            let micro = (amt * 1e6).round() as i64;
            for user in &ledger.users {
                let u = user.to_lowercase();
                state.apply_usd_transfer(&u, -micro);
            }
            true
        }
        // Send events: most are handled by replica (user's sendAsset action).
        // But bridge deposits routed by the system intermediary (0x6b9e77...)
        // are NOT in replica — they only appear here. Process those.
        LedgerDelta::Send(v) => {
            let sender = v.get("user").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
            let src = v.get("sourceDex").and_then(|v| v.as_str()).unwrap_or("");
            let dst = v.get("destinationDex").and_then(|v| v.as_str()).unwrap_or("_absent_");
            let dest = v.get("destination").and_then(|v| v.as_str()).unwrap_or("");
            let amount_str = v.get("amount").and_then(|v| v.as_str()).unwrap_or("0");

            let is_bridge = sender == "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24";
            let is_perps_dst = dst.is_empty() || state.dex_name_to_pdi.contains_key(dst);

            if is_bridge && !dest.is_empty() {
                let amt: f64 = amount_str.parse().unwrap_or(0.0);
                if amt > 0.0 {
                    let dest_addr = dest.to_lowercase();
                    let token_name = v.get("token").and_then(|v| v.as_str()).unwrap_or("USDC");
                    // Bridge deposits are stablecoins — default to USDC for unknown tokens.
                    let token_id: u32 = match token_name {
                        "USDC" => 0,
                        "USDH" => 360,
                        "USDE" => 235,
                        _ => 0,
                    };
                    if is_perps_dst {
                        // Bridge deposit directly to perps dex.
                        // For Unified users, route to SCL (cross-dex shared balance).
                        let micro = (amt * 1e6).round() as i64;
                        let pdi = state.dex_name_to_pdi.get(dst).copied().unwrap_or(0);
                        state.apply_usd_transfer_on_dex_unified_scl(&dest_addr, micro, pdi);
                    } else {
                        // Bridge deposit to spot (SCL)
                        let delta = (amt * 1e8).round() as i64;
                        state.apply_spot_transfer(&dest_addr, token_id, delta);
                    }
                    return true;
                }
            }
            false
        }
        LedgerDelta::SpotTransfer(v) => {
            // System spot transfers (from/to 0x2000...) are not in replica_cmds.
            // User-initiated spot transfers ARE in replica_cmds (spotSend action),
            // EXCEPT for system-routed flows like accountClassTransfer → spotTransfer chains.
            let sender = v.get("user").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
            let dest = v.get("destination").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
            let is_system_sender = sender.starts_with("0x2000000000000000000000000000000000");
            let is_system_dest = dest.starts_with("0x2000000000000000000000000000000000");
            if is_system_sender || is_system_dest {
                let amount_str = v.get("amount").and_then(|v| v.as_str()).unwrap_or("0");
                let amt: f64 = amount_str.parse().unwrap_or(0.0);
                let token_name = v.get("token").and_then(|v| v.as_str()).unwrap_or("USDC");
                let token_id: Option<u32> = match token_name {
                    "USDC" => Some(0),
                    "USDH" => Some(360),
                    "USDE" => Some(235),
                    _ => None,
                };
                if let Some(token_id) = token_id {
                    if amt > 0.0 {
                        let delta = (amt * 1e8).round() as i64;
                        if is_system_sender && !dest.is_empty() {
                            // Incoming from system → credit destination
                            state.apply_spot_transfer(&dest, token_id, delta);
                            return true;
                        } else if is_system_dest && !sender.is_empty() {
                            // Outgoing to system (bridge withdrawal) → debit sender
                            state.apply_spot_transfer(&sender, token_id, -delta);
                            return true;
                        }
                    }
                }
            }
            false
        }
        LedgerDelta::VaultWithdraw(v) => {
            // Vault withdrawals: replica handles usd>0 cases with exact amount.
            // For usd=0 (equity-based), replica defers here since we have the
            // exact netWithdrawnUsd from the LedgerUpdate.
            let user_addr = v.get("user").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
            let vault_addr = v.get("vault").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
            // Skip if the replica already processed this with an exact amount.
            if state.processed_vault_withdrawals.contains(&(vault_addr.clone(), user_addr.clone())) {
                return false;
            }
            let net_usd_str = v.get("netWithdrawnUsd").and_then(|v| v.as_str()).unwrap_or("0");
            let net_usd: f64 = net_usd_str.parse().unwrap_or(0.0);
            if net_usd != 0.0 && !user_addr.is_empty() {
                let micro = (net_usd * 1e6).round() as i64;
                state.apply_usd_transfer(&vault_addr, -micro);
                state.apply_usd_transfer(&user_addr, micro);
                return true;
            }
            false
        }
        LedgerDelta::AccountClassTransfer(v) => {
            // accountClassTransfer: moves USDC between perps (usdc_balance) and spot (SCL).
            // Sometimes accompanies updateIsolatedMargin and only appears in misc events
            // (no replica UsdClassTransfer counterpart). User comes from outer event.
            let usdc_str = v.get("usdc").and_then(|v| v.as_str()).unwrap_or("0");
            let to_perp = v.get("toPerp").and_then(|v| v.as_bool()).unwrap_or(false);
            let amt: f64 = usdc_str.parse().unwrap_or(0.0);
            if amt > 0.0 {
                if let Some(user_addr) = ledger.users.first() {
                    let user_addr = user_addr.to_lowercase();
                    let micro = (amt * 1e6).round() as i64;
                    let spot_delta = (amt * 1e8).round() as i64;
                    if to_perp {
                        state.apply_usd_transfer_on_dex(&user_addr, micro, 0);
                        state.apply_spot_transfer(&user_addr, 0, -spot_delta);
                    } else {
                        state.apply_usd_transfer_on_dex(&user_addr, -micro, 0);
                        state.apply_spot_transfer(&user_addr, 0, spot_delta);
                    }
                    return true;
                }
            }
            false
        }
        // All these are already handled by replica_cmds — skip
        LedgerDelta::Deposit(_)
        | LedgerDelta::Withdraw(_)
        | LedgerDelta::InternalTransfer(_)
        | LedgerDelta::SubAccountTransfer(_)
        | LedgerDelta::VaultCreate(_)
        | LedgerDelta::VaultDeposit(_)
        | LedgerDelta::BorrowLend(_)
        | LedgerDelta::SpotGenesis(_)
        | LedgerDelta::CStakingTransfer(_)
        | LedgerDelta::PerpDexClassTransfer(_)
        | LedgerDelta::Unknown => false,
    }
}

fn collect_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(&path, out);
        } else if path.is_file() {
            out.push(path);
        }
    }
}

fn related_debug_users_in_value(value: &Value, debug_users: &HashSet<String>) -> Vec<String> {
    let serialized = serde_json::to_string(value).unwrap_or_else(|_| value.to_string()).to_lowercase();
    let mut users: Vec<_> = debug_users.iter().filter(|user| serialized.contains(user.as_str())).cloned().collect();
    users.sort();
    users
}

fn classify_misc_event(event: &Value) -> (String, &'static str) {
    let Some(inner) = event.get("inner").and_then(Value::as_object) else {
        return ("unknown".to_string(), "unknown");
    };
    let Some((kind, payload)) = inner.iter().next() else {
        return ("unknown".to_string(), "unknown");
    };

    match kind.as_str() {
        "Funding" => ("Funding".to_string(), "applied"),
        "LedgerUpdate" => {
            let delta_type =
                payload.get("delta").and_then(|delta| delta.get("type")).and_then(Value::as_str).unwrap_or("unknown");
            (
                format!("LedgerUpdate/{delta_type}"),
                if is_processed_ledger_delta(delta_type) {
                    "applied"
                } else if is_skipped_ledger_delta(delta_type) {
                    "skipped"
                } else {
                    "unknown"
                },
            )
        }
        "CDeposit" | "CWithdrawal" | "Delegation" | "ValidatorRewards" => (kind.clone(), "skipped"),
        _ => (kind.clone(), "unknown"),
    }
}

fn is_processed_ledger_delta(delta_type: &str) -> bool {
    matches!(
        delta_type,
        "liquidation"
            | "vaultDistribution"
            | "vaultLeaderCommission"
            | "rewardsClaim"
            | "deployGasAuction"
            | "accountActivationGas"
    )
}

fn is_skipped_ledger_delta(delta_type: &str) -> bool {
    matches!(
        delta_type,
        "deposit"
            | "withdraw"
            | "internalTransfer"
            | "subAccountTransfer"
            | "spotTransfer"
            | "send"
            | "accountClassTransfer"
            | "vaultCreate"
            | "vaultDeposit"
            | "vaultWithdraw"
            | "borrowLend"
            | "spotGenesis"
            | "cStakingTransfer"
            | "perpDexClassTransfer"
    )
}

fn indent_json(value: &Value) -> String {
    let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    let mut out = String::new();
    for line in pretty.lines() {
        out.push_str("    ");
        out.push_str(line);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{classify_misc_event, related_debug_users_in_value};
    use serde_json::json;
    use std::collections::HashSet;

    #[test]
    fn finds_debug_users_from_serialized_event_json() {
        let debug_users = HashSet::from([String::from("0xabc"), String::from("0xdef")]);
        let event = json!({
            "inner": {
                "CWithdrawal": {
                    "payload": {
                        "user": "0xAbC",
                        "nested": ["ignored", {"account": "0xdEf"}]
                    }
                }
            }
        });

        assert_eq!(
            related_debug_users_in_value(&event, &debug_users),
            vec![String::from("0xabc"), String::from("0xdef")]
        );
    }

    #[test]
    fn classifies_skipped_and_applied_events() {
        let skipped = json!({
            "inner": {
                "LedgerUpdate": {
                    "users": ["0xabc"],
                    "delta": {"type": "borrowLend"}
                }
            }
        });
        let applied = json!({
            "inner": {
                "LedgerUpdate": {
                    "users": ["0xabc"],
                    "delta": {"type": "liquidation"}
                }
            }
        });

        assert_eq!(classify_misc_event(&skipped), (String::from("LedgerUpdate/borrowLend"), "skipped"));
        assert_eq!(classify_misc_event(&applied), (String::from("LedgerUpdate/liquidation"), "applied"));
    }

    #[test]
    fn parses_send_delta_with_extra_fields() {
        let json = r#"{"type": "send", "user": "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24", "destination": "0xtest", "amount": "29999.8", "sourceDex": "spot", "destinationDex": "", "token": "USDC", "fee": "0.0", "nonce": 123, "feeToken": "", "nativeTokenFee": "0.0", "usdcValue": "29999.8"}"#;
        let delta: super::LedgerDelta = serde_json::from_str(json).unwrap();
        match &delta {
            super::LedgerDelta::Send(v) => {
                assert_eq!(v.get("user").unwrap().as_str().unwrap(), "0x6b9e773128f453f5c2c60935ee2de2cbc5390a24");
                assert_eq!(v.get("sourceDex").unwrap().as_str().unwrap(), "spot");
                assert_eq!(v.get("destinationDex").unwrap().as_str().unwrap(), "");
            }
            other => panic!("Expected Send, got {:?}", other),
        }
    }
}
