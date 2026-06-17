//! Parser and watcher for `replica_cmds` ABCI blocks.
//!
//! Each line in a replica_cmds file is a JSON object representing one block.
//! We extract state-mutating actions (leverage changes, transfers, etc.)
//! and apply them to the clearing house state.
//!
//! Unknown action types are ignored so replay remains forward-compatible
//! with newer node action variants.

use log::{info, warn};
use serde::Deserialize;
use std::{
    collections::HashSet,
    io::{BufRead, BufReader},
    path::PathBuf,
};

// ── Serde types ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ReplicaBlock {
    pub abci_block: AbciBlock,
    #[serde(default)]
    pub resps: Option<Resps>,
}

#[derive(Debug, Deserialize)]
pub enum Resps {
    Full(Vec<(String, Vec<ActionResp>)>),
}

#[derive(Debug, Deserialize)]
pub struct ActionResp {
    pub user: Option<String>,
    #[serde(default)]
    pub res: Option<ActionResult>,
}

#[derive(Debug, Deserialize)]
pub struct ActionResult {
    pub status: String,
}

impl ActionResp {
    pub fn is_success(&self) -> bool {
        self.res.as_ref().is_none_or(|r| r.status == "ok")
    }
}

#[derive(Debug, Deserialize)]
pub struct AbciBlock {
    #[allow(dead_code)]
    pub time: Option<String>,
    pub signed_action_bundles: Vec<(String, ActionBundle)>,
}

#[derive(Debug, Deserialize)]
pub struct ActionBundle {
    pub signed_actions: Vec<SignedAction>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignedAction {
    pub vault_address: Option<String>,
    pub action: Action,
}

/// Replica action types. State-mutating variants are parsed in full;
/// safe-to-ignore variants capture remaining fields into a catch-all Value.
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(dead_code)]
pub enum Action {
    // ── State-mutating ──────────────────────────────────────────
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "updateLeverage")]
    UpdateLeverage {
        asset: u32,
        is_cross: bool,
        leverage: u32,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "updateIsolatedMargin")]
    UpdateIsolatedMargin {
        asset: u32,
        is_buy: bool,
        ntli: i64,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "usdSend")]
    UsdSend {
        destination: String,
        amount: String,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "usdClassTransfer")]
    UsdClassTransfer {
        amount: String,
        to_perp: bool,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "subAccountTransfer")]
    SubAccountTransfer {
        sub_account_user: String,
        is_deposit: bool,
        usd: i64,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "withdraw3")]
    Withdraw3 {
        amount: String,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "spotSend")]
    SpotSend {
        destination: String,
        token: String,
        amount: String,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "sendAsset")]
    SendAsset {
        destination: String,
        token: String,
        amount: String,
        #[serde(default)]
        source_dex: Option<String>,
        #[serde(default)]
        destination_dex: Option<String>,
        #[serde(default)]
        from_sub_account: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "vaultTransfer")]
    VaultTransfer {
        vault_address: String,
        is_deposit: bool,
        usd: i64,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "userSetAbstraction")]
    UserSetAbstraction {
        user: String,
        abstraction: String,
    },

    // ── Safe to ignore (no liquidation state effect) ────────────
    #[serde(alias = "order")]
    Order(serde_json::Value),
    #[serde(alias = "cancel")]
    Cancel(serde_json::Value),
    #[serde(alias = "cancelByCloid")]
    CancelByCloid(serde_json::Value),
    #[serde(alias = "batchModify")]
    BatchModify(serde_json::Value),
    #[serde(alias = "modify")]
    Modify(serde_json::Value),
    #[serde(alias = "scheduleCancel")]
    ScheduleCancel(serde_json::Value),
    #[serde(alias = "noop")]
    Noop(serde_json::Value),
    #[serde(alias = "approveAgent")]
    ApproveAgent(serde_json::Value),
    #[serde(alias = "setReferrer")]
    SetReferrer(serde_json::Value),
    #[serde(alias = "registerReferrer")]
    RegisterReferrer(serde_json::Value),
    #[serde(alias = "approveBuilderFee")]
    ApproveBuilderFee(serde_json::Value),
    #[serde(alias = "perpDeploy")]
    PerpDeploy(serde_json::Value),
    #[serde(alias = "agentEnableDexAbstraction")]
    AgentEnableDexAbstraction {},
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "userDexAbstraction")]
    UserDexAbstraction {
        #[serde(default)]
        abstraction: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "agentSetAbstraction")]
    AgentSetAbstraction {
        abstraction: String,
    },
    #[serde(alias = "voteAppHash")]
    VoteAppHash(serde_json::Value),
    #[serde(alias = "claimRewards")]
    ClaimRewards(serde_json::Value),
    #[serde(alias = "spotUser")]
    SpotUser(serde_json::Value),
    #[serde(alias = "SetGlobalAction")]
    SetGlobalAction {
        #[serde(default)]
        pxs: Vec<serde_json::Value>,
    },
    #[serde(rename_all = "camelCase")]
    VoteEthFinalizedWithdrawalAction {
        user: String,
        usd: i64,
        nonce: i64,
    },
    ValidatorSignWithdrawalAction(serde_json::Value),
    #[serde(alias = "evmRawTx")]
    EvmRawTx(serde_json::Value),
    #[serde(alias = "evmUserModify")]
    EvmUserModify(serde_json::Value),
    #[serde(alias = "multiSig")]
    MultiSig(serde_json::Value),
    #[serde(alias = "twapOrder")]
    TwapOrder(serde_json::Value),
    #[serde(alias = "twapCancel")]
    TwapCancel(serde_json::Value),
    NetChildVaultPositionsAction(serde_json::Value),
    #[serde(rename_all = "camelCase")]
    VoteEthDepositAction {
        user: String,
        usd: i64,
        eth_id: serde_json::Value,
    },
    #[serde(alias = "reserveRequestWeight")]
    ReserveRequestWeight(serde_json::Value),
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "cDeposit")]
    CDeposit {
        wei: i64,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "cWithdraw")]
    CWithdraw {
        wei: i64,
    },
    #[serde(alias = "twapModify")]
    TwapModify(serde_json::Value),
    #[serde(alias = "registerReferrerRaw")]
    RegisterReferrerRaw(serde_json::Value),
    #[serde(alias = "createSubAccount")]
    CreateSubAccount(serde_json::Value),
    #[serde(alias = "subAccountModify")]
    SubAccountModify(serde_json::Value),
    #[serde(alias = "sendToEvmWithData")]
    SendToEvmWithData(serde_json::Value),
    #[serde(alias = "borrowLend")]
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "borrowLend")]
    BorrowLend {
        operation: String,
        token: u32,
        amount: Option<String>,
    },
    #[serde(rename_all = "camelCase")]
    #[serde(alias = "tokenDelegate")]
    TokenDelegate {
        wei: i64,
        is_undelegate: bool,
    },
    #[serde(other)]
    Unknown,
}

impl Action {
    pub fn is_ignored(&self) -> bool {
        !matches!(
            self,
            Action::UpdateLeverage { .. }
                | Action::UpdateIsolatedMargin { .. }
                | Action::UsdSend { .. }
                | Action::UsdClassTransfer { .. }
                | Action::SubAccountTransfer { .. }
                | Action::Withdraw3 { .. }
                | Action::SpotSend { .. }
                | Action::SendAsset { .. }
                | Action::VaultTransfer { .. }
                | Action::UserSetAbstraction { .. }
                | Action::VoteEthDepositAction { .. }
                | Action::BorrowLend { .. }
                | Action::CDeposit { .. }
                | Action::CWithdraw { .. }
                | Action::TokenDelegate { .. }
                | Action::SetGlobalAction { .. }
                | Action::AgentSetAbstraction { .. }
                | Action::AgentEnableDexAbstraction { .. }
                | Action::UserDexAbstraction { .. }
        )
    }
}

// ── Apply a single block to state ────────────────────────────────────────

pub fn apply_replica_block(
    state: &mut super::LiquidationState,
    block: &ReplicaBlock,
) -> Result<HashSet<String>, String> {
    let mut affected_users = HashSet::new();

    let resp_bundles: Vec<&Vec<ActionResp>> = match &block.resps {
        Some(Resps::Full(bundles)) => bundles.iter().map(|(_, resps)| resps).collect(),
        None => Vec::new(),
    };

    for (bundle_idx, (_tx_hash, bundle)) in block.abci_block.signed_action_bundles.iter().enumerate() {
        let bundle_resps = resp_bundles.get(bundle_idx).copied();

        for (action_idx, signed_action) in bundle.signed_actions.iter().enumerate() {
            let action = &signed_action.action;

            if action.is_ignored() {
                continue;
            }

            // Skip failed actions — they didn't change state on-chain
            let resp = bundle_resps.and_then(|r| r.get(action_idx));
            if let Some(r) = resp {
                if !r.is_success() {
                    continue;
                }
            }

            let user = signed_action
                .vault_address
                .as_ref()
                .map(|a| a.to_lowercase())
                .or_else(|| resp.and_then(|r| r.user.as_ref()).map(|u| u.to_lowercase()));

            match action {
                Action::UpdateLeverage { asset, is_cross, leverage } => {
                    let Some(user) = &user else { continue };
                    state.apply_leverage_update(user, *asset, *is_cross, *leverage);
                    affected_users.insert(user.clone());
                }
                Action::UpdateIsolatedMargin { asset, is_buy, ntli } => {
                    let Some(user) = &user else { continue };
                    state.apply_isolated_margin_update(user, *asset, *is_buy, *ntli);
                    affected_users.insert(user.clone());
                }
                Action::UsdSend { destination, amount } => {
                    let amt: f64 = amount.parse().unwrap_or(0.0);
                    let micro = (amt * 1e6).round() as i64;
                    if let Some(user) = &user {
                        state.apply_usd_transfer(user, -micro);
                        affected_users.insert(user.clone());
                    }
                    let dest = destination.to_lowercase();
                    state.apply_usd_transfer(&dest, micro);
                    affected_users.insert(dest);
                }
                Action::UsdClassTransfer { amount, to_perp } => {
                    // Moves USDC between spot (SCL) and perps (dex 0)
                    let Some(user) = &user else { continue };
                    let amt: f64 = amount.parse().unwrap_or(0.0);
                    let micro = (amt * 1e6).round() as i64;
                    let spot_delta = (amt * 1e8).round() as i64;
                    if *to_perp {
                        state.apply_usd_transfer_on_dex(user, micro, 0);
                        state.apply_spot_transfer(user, 0, -spot_delta);
                    } else {
                        state.apply_usd_transfer_on_dex(user, -micro, 0);
                        state.apply_spot_transfer(user, 0, spot_delta);
                    }
                    affected_users.insert(user.clone());
                }
                Action::SubAccountTransfer { sub_account_user, is_deposit, usd } => {
                    let Some(user) = &user else { continue };
                    let sub = sub_account_user.to_lowercase();
                    // Sub-account transfers target dex 0 (main perps).
                    if *is_deposit {
                        state.apply_usd_transfer_on_dex(user, -*usd, 0);
                        state.apply_usd_transfer_on_dex(&sub, *usd, 0);
                    } else {
                        state.apply_usd_transfer_on_dex(&sub, -*usd, 0);
                        state.apply_usd_transfer_on_dex(user, *usd, 0);
                    }
                    affected_users.insert(user.clone());
                    affected_users.insert(sub);
                }
                Action::Withdraw3 { amount } => {
                    // Withdrawals deduct from perps usdc (dex 0)
                    let Some(user) = &user else { continue };
                    let amt: f64 = amount.parse().unwrap_or(0.0);
                    let micro = (amt * 1e6).round() as i64;
                    state.apply_usd_transfer_on_dex(user, -micro, 0);
                    affected_users.insert(user.clone());
                }
                Action::SpotSend { destination, token, amount } => {
                    let Some(user) = &user else { continue };
                    let token_name = token.split(':').next().unwrap_or(token);
                    let token_id: u32 = token.parse().unwrap_or_else(|_| match token_name {
                        "USDC" => 0,
                        "USDH" => 360,
                        "USDE" => 235,
                        _ => u32::MAX,
                    });
                    let amt: f64 = amount.parse().unwrap_or(0.0);
                    let delta = (amt * 1e8).round() as i64;
                    let dest = destination.to_lowercase();
                    state.apply_spot_transfer(user, token_id, -delta);
                    state.apply_spot_transfer(&dest, token_id, delta);
                    affected_users.insert(user.clone());
                    affected_users.insert(dest);
                }
                Action::SendAsset { destination, token, amount, source_dex, destination_dex, from_sub_account } => {
                    // Determine the actual sender: fromSubAccount takes precedence
                    let sender = from_sub_account
                        .as_ref()
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_lowercase())
                        .or_else(|| user.clone());
                    let Some(sender) = sender else { continue };
                    let dest = destination.to_lowercase();
                    let amt: f64 = amount.parse().unwrap_or(0.0);
                    let micro = (amt * 1e6).round() as i64;
                    // token can be ID ("0"), name ("USDC"), or "NAME:0xhex" format
                    let token_name = token.split(':').next().unwrap_or(token);
                    let token_id: u32 = token.parse().unwrap_or_else(|_| match token_name {
                        "USDC" => 0,
                        "USDH" => 360,
                        "USDE" => 235,
                        _ => u32::MAX,
                    });

                    // Cross-dex transfer: sourceDex/destinationDex determine what balances change.
                    // Clearing houses: "" (main perps), "xyz", "flx", "vntl", "hyna", "km", "abcd"
                    // Spot exchange: "spot"
                    // External: "HyperEVM", "HyperCore"
                    let src = source_dex.as_deref().unwrap_or("");
                    let dst = destination_dex.as_deref().unwrap_or("");

                    let src_pdi = state.dex_name_to_pdi.get(src).copied();
                    let dst_pdi = state.dex_name_to_pdi.get(dst).copied();
                    let is_spot = |dex: &str| dex == "spot";

                    if source_dex.is_none() && destination_dex.is_none() {
                        // Legacy format: fields absent entirely — pure spot transfer
                        let delta = (amt * 1e8).round() as i64;
                        state.apply_spot_transfer(&sender, token_id, -delta);
                        state.apply_spot_transfer(&dest, token_id, delta);
                    } else {
                        // Source side: sender's mode determines where funds leave from
                        if let Some(pdi) = src_pdi {
                            state.apply_usd_transfer_on_dex(&sender, -micro, pdi);
                        } else if is_spot(src) {
                            let delta = (amt * 1e8).round() as i64;
                            state.apply_spot_transfer(&sender, token_id, -delta);
                        }

                        // Destination side: receiver's account mode determines routing.
                        // - Unified: always to spot (SCL)
                        // - Standard: respects destinationDex literally
                        // - DexAbstraction: USDC to dex 0 if perps dest, non-USDC to spot
                        let dest_mode = state
                            .dex_states
                            .iter()
                            .find_map(|dex| {
                                dex.users
                                    .get(&dest)
                                    .map(|u| u.account_mode)
                                    .or_else(|| dex.users_without_positions.get(&dest).map(|p| p.account_mode))
                            })
                            .unwrap_or(super::AccountMode::Standard);

                        match dest_mode {
                            super::AccountMode::Unified => {
                                // Unified: all incoming funds go to spot (SCL)
                                let delta = (amt * 1e8).round() as i64;
                                state.apply_spot_transfer(&dest, token_id, delta);
                            }
                            super::AccountMode::DexAbstraction => {
                                // DexAbs: USDC to dex 0 if perps, non-USDC to spot
                                if token_id == 0 {
                                    if let Some(pdi) = dst_pdi {
                                        state.apply_usd_transfer_on_dex(&dest, micro, pdi);
                                    } else {
                                        let delta = (amt * 1e8).round() as i64;
                                        state.apply_spot_transfer(&dest, token_id, delta);
                                    }
                                } else {
                                    let delta = (amt * 1e8).round() as i64;
                                    state.apply_spot_transfer(&dest, token_id, delta);
                                }
                            }
                            _ => {
                                // Standard: respect destinationDex
                                if let Some(pdi) = dst_pdi {
                                    state.apply_usd_transfer_on_dex(&dest, micro, pdi);
                                } else if is_spot(dst) {
                                    let delta = (amt * 1e8).round() as i64;
                                    state.apply_spot_transfer(&dest, token_id, delta);
                                }
                            }
                        }
                    }
                    affected_users.insert(sender);
                    affected_users.insert(dest);
                }
                Action::VaultTransfer { vault_address, is_deposit, usd } => {
                    let vault = vault_address.to_lowercase();
                    let Some(user) = &user else { continue };
                    if *usd == 0 && !*is_deposit {
                        // usd=0 withdrawal: deferred to misc_events VaultWithdraw handler
                        // which has the exact netWithdrawnUsd amount from the LedgerUpdate.
                        affected_users.insert(user.clone());
                        affected_users.insert(vault);
                        continue;
                    }
                    let amount = *usd;
                    // Record that this vault withdrawal was handled with exact amount,
                    // so the misc event handler won't double-count it.
                    if !*is_deposit {
                        state.processed_vault_withdrawals.insert((vault.clone(), user.clone()));
                    }
                    if *is_deposit {
                        state.apply_usd_transfer(user, -amount);
                        state.apply_usd_transfer(&vault, amount);
                    } else {
                        state.apply_usd_transfer(&vault, -amount);
                        state.apply_usd_transfer(user, amount);
                    }
                    affected_users.insert(user.clone());
                    affected_users.insert(vault);
                }
                Action::UserSetAbstraction { user: target_user, abstraction } => {
                    let target = target_user.to_lowercase();
                    state.apply_set_abstraction(&target, abstraction);
                    affected_users.insert(target);
                }
                Action::AgentSetAbstraction { abstraction } => {
                    let Some(user) = &user else { continue };
                    state.apply_set_abstraction(user, abstraction);
                    affected_users.insert(user.clone());
                }
                Action::AgentEnableDexAbstraction {} => {
                    let Some(user) = &user else { continue };
                    state.apply_set_abstraction(user, "d");
                    affected_users.insert(user.clone());
                }
                Action::UserDexAbstraction { abstraction } => {
                    let Some(user) = &user else { continue };
                    let mode = abstraction.as_deref().unwrap_or("d");
                    state.apply_set_abstraction(user, mode);
                    affected_users.insert(user.clone());
                }
                Action::VoteEthFinalizedWithdrawalAction { .. } => {
                    // withdraw3 already deducted the balance. This is just validators
                    // confirming the L1 withdrawal went through. No balance effect.
                }
                Action::VoteEthDepositAction { user: target_user, usd, eth_id } => {
                    let target = target_user.to_lowercase();
                    let dedup_key = {
                        use std::collections::hash_map::DefaultHasher;
                        use std::hash::{Hash, Hasher};
                        let mut h = DefaultHasher::new();
                        eth_id.to_string().hash(&mut h);
                        h.finish() as i64
                    };
                    let is_new = state.processed_withdrawal_nonces.insert(dedup_key);
                    if state.debug_users.contains(&target) {
                        eprintln!(
                            "[DEBUG VoteEthDeposit] user={} usd=${:.2} ethId={} is_new={} resp_status={:?}",
                            target,
                            *usd as f64 / 1e6,
                            eth_id.to_string().chars().take(60).collect::<String>(),
                            is_new,
                            resp.map(|r| r.is_success())
                        );
                    }
                    if is_new {
                        // ETH deposits are always USDC — route to dex 0.
                        state.apply_usd_transfer_on_dex(&target, *usd, 0);
                        affected_users.insert(target);
                    }
                }
                Action::BorrowLend { operation, token, amount } => {
                    let Some(user) = &user else { continue };
                    let Some(amount_str) = amount else { continue };
                    let amt: f64 = amount_str.parse().unwrap_or(0.0);
                    if amt == 0.0 {
                        continue;
                    }
                    let delta = (amt * 1e8).round() as i64;
                    let delta = if operation == "supply" { -delta } else { delta };
                    state.apply_spot_transfer(user, *token, delta);
                    affected_users.insert(user.clone());
                }
                Action::CDeposit { wei } => {
                    // Staking HYPE: removes from spot HYPE balance. wei is in HYPE units.
                    // For unified mode users, this reduces spot_collateral if HYPE is the collateral token.
                    // HYPE token ID = 150
                    let Some(user) = &user else { continue };
                    state.apply_spot_transfer(user, 150, -*wei);
                    affected_users.insert(user.clone());
                }
                Action::CWithdraw { wei } => {
                    // Unstaking HYPE: returns to spot HYPE balance.
                    let Some(user) = &user else { continue };
                    state.apply_spot_transfer(user, 150, *wei);
                    affected_users.insert(user.clone());
                }
                Action::SetGlobalAction { pxs } => {
                    // Update mark prices for dex 0 assets.
                    // pxs is array of [mark_px, oracle_px] per asset index.
                    for (asset_idx, px_pair) in pxs.iter().enumerate() {
                        if let Some(arr) = px_pair.as_array() {
                            if let Some(mark_str) = arr.first().and_then(|v| v.as_str()) {
                                if let Ok(mark_px) = mark_str.parse::<f64>() {
                                    state.mark_prices.insert((0, asset_idx as u32), mark_px);
                                }
                            }
                        }
                    }
                }
                // tokenDelegate changes which validator staked tokens go to, no balance effect.
                _ => {}
            }
        }
    }

    // Also check for perpDeploy setOracle in responses (hip-3 oracle prices)
    for (bundle_idx, (_tx_hash, bundle)) in block.abci_block.signed_action_bundles.iter().enumerate() {
        let bundle_resps = resp_bundles.get(bundle_idx).copied();
        for (action_idx, signed_action) in bundle.signed_actions.iter().enumerate() {
            if let Action::PerpDeploy(ref val) = signed_action.action {
                if let Some(set_oracle) = val.get("setOracle") {
                    if let Some(dex_name) = set_oracle.get("dex").and_then(|v| v.as_str()) {
                        if let Some(oracle_pxs) = set_oracle.get("oraclePxs").and_then(|v| v.as_array()) {
                            // Find the dex index for this dex name
                            let dex_idx = state.dex_states.iter().position(|d| {
                                d.universe.first().map(|a| a.name.starts_with(&format!("{dex_name}:"))).unwrap_or(false)
                            });
                            if let Some(di) = dex_idx {
                                for pair in oracle_pxs {
                                    if let Some(arr) = pair.as_array() {
                                        if arr.len() >= 2 {
                                            let coin = arr[0].as_str().unwrap_or("");
                                            let px_str = arr[1].as_str().unwrap_or("");
                                            if let Ok(px) = px_str.parse::<f64>() {
                                                // Find asset index by coin name
                                                if let Some(ai) =
                                                    state.dex_states[di].universe.iter().position(|a| a.name == coin)
                                                {
                                                    state.mark_prices.insert((di, ai as u32), px);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Count actions per user
    for user in &affected_users {
        *state.user_action_counts.entry(user.clone()).or_default() += 1;
    }

    Ok(affected_users)
}

// ── Replay from disk ─────────────────────────────────────────────────────

/// Replay replica_cmds from disk for blocks in `[from_block, to_block)`.
pub fn replay_replica_from_disk(
    home_dir: &std::path::Path,
    state: &mut super::LiquidationState,
    from_block: u64,
    to_block: u64,
) -> u64 {
    let replica_dir = home_dir.join("hl/data/replica_cmds");
    if !replica_dir.exists() {
        info!("replica_cmds directory does not exist: {}. Skipping.", replica_dir.display());
        return 0;
    }

    let mut files = Vec::new();
    collect_replica_files(&replica_dir, &mut files);
    files.sort();

    let mut blocks_applied: u64 = 0;
    let mut seen_unknown_command_types: HashSet<String> = HashSet::new();

    for path in &files {
        // File name is the block height
        let file_block: Option<u64> = path.file_name().and_then(|s| s.to_str()).and_then(|s| s.parse().ok());

        if let Some(fb) = file_block {
            // Replica file named N contains blocks from N onwards (up to ~N+10000).
            // Skip files that are entirely AFTER our range.
            if fb > to_block {
                continue;
            }
            // Skip files that are entirely BEFORE our range.
            // A file at block N could contain blocks up to N+10000.
            if fb + 10_000 <= from_block {
                continue;
            }
        }

        let file = match std::fs::File::open(path) {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to open replica file {}: {e}", path.display());
                continue;
            }
        };
        let reader = BufReader::new(file);
        for (line_num, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    warn!("Failed to read replica line from {}: {e}", path.display());
                    break;
                }
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let raw_block = match serde_json::from_str::<serde_json::Value>(trimmed) {
                Ok(raw) => raw,
                Err(e) => {
                    eprintln!("\n=== FATAL: Replica parse error ===");
                    eprintln!("File: {}", path.display());
                    eprintln!("Line number: {}", line_num + 1);
                    eprintln!("Error: {e}");
                    eprintln!("Raw JSON (first 2000 chars):");
                    eprintln!("{}", &trimmed[..trimmed.len().min(2000)]);
                    std::process::exit(1);
                }
            };

            log_unknown_replica_command_types(&raw_block, &mut seen_unknown_command_types);

            match serde_json::from_value::<ReplicaBlock>(raw_block) {
                Ok(block) => match apply_replica_block(state, &block) {
                    Ok(_) => blocks_applied += 1,
                    Err(e) => warn!("Failed to apply replica block: {e}"),
                },
                Err(e) => {
                    // ABORT: malformed replica payload parse error
                    eprintln!("\n=== FATAL: Replica parse error ===");
                    eprintln!("File: {}", path.display());
                    eprintln!("Line number: {}", line_num + 1);
                    eprintln!("Error: {e}");
                    eprintln!("Raw JSON (first 2000 chars):");
                    eprintln!("{}", &trimmed[..trimmed.len().min(2000)]);

                    // Try to extract the action type for better debugging
                    if let Ok(raw) = serde_json::from_str::<serde_json::Value>(trimmed) {
                        if let Some(bundles) = raw
                            .get("abci_block")
                            .and_then(|b| b.get("signed_action_bundles"))
                            .and_then(|b| b.as_array())
                        {
                            eprintln!("\nAction types in this block:");
                            for bundle in bundles {
                                if let Some(actions) =
                                    bundle.get(1).and_then(|b| b.get("signed_actions")).and_then(|a| a.as_array())
                                {
                                    for action in actions {
                                        if let Some(t) =
                                            action.get("action").and_then(|a| a.get("type")).and_then(|t| t.as_str())
                                        {
                                            eprintln!("  - {t}");
                                        }
                                    }
                                }
                            }
                        }
                    }
                    std::process::exit(1);
                }
            }
        }
    }

    info!("Replayed {blocks_applied} replica blocks from block {from_block} to {to_block}");
    blocks_applied
}

fn collect_replica_files(dir: &std::path::Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_replica_files(&path, out);
        } else if path.is_file() {
            out.push(path);
        }
    }
}

fn log_unknown_replica_command_types(raw_block: &serde_json::Value, seen: &mut HashSet<String>) {
    let Some(bundles) =
        raw_block.get("abci_block").and_then(|b| b.get("signed_action_bundles")).and_then(|b| b.as_array())
    else {
        return;
    };

    for bundle in bundles {
        let Some(actions) = bundle.get(1).and_then(|b| b.get("signed_actions")).and_then(|a| a.as_array()) else {
            continue;
        };

        for signed_action in actions {
            let Some(action_json) = signed_action.get("action") else {
                continue;
            };

            let Ok(action) = serde_json::from_value::<Action>(action_json.clone()) else {
                continue;
            };

            if matches!(action, Action::Unknown) {
                let command_type = action_json.get("type").and_then(|t| t.as_str()).unwrap_or("<missing-type>");
                if seen.insert(command_type.to_string()) {
                    warn!(
                        "Unknown replica command type first seen in this run: {command_type}; action={}",
                        action_json
                    );
                }
            }
        }
    }
}
