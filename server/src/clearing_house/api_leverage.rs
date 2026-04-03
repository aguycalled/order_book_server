//! Query the Hyperliquid API to fix leverage settings for positions
//! where the snapshot didn't have the user's preference.
//!
//! Two-pass approach:
//! 1. First replay discovers which users need leverage data (positions_needing_leverage_fix)
//! 2. Query API for those users
//! 3. Inject leverage into the INITIAL state's leverage_settings
//! 4. Second replay uses correct leverage from the start

use log::info;
use std::collections::{HashMap, HashSet};

use super::{Leverage, LiquidationState};

/// After a first-pass replay, query the API for users with unknown leverage
/// and inject the correct settings into the given initial state (for second-pass replay).
/// Returns the number of leverage settings injected.
pub fn inject_leverage_from_api(
    first_pass_state: &LiquidationState,
    initial_state: &mut LiquidationState,
) -> usize {
    let needs_fix = &first_pass_state.positions_needing_leverage_fix;
    if needs_fix.is_empty() {
        return 0;
    }

    // Deduplicate users
    let unique_users: HashSet<String> = needs_fix.iter().map(|(u, _, _)| u.clone()).collect();
    info!(
        "Querying HL API for {} users ({} positions) with unknown leverage...",
        unique_users.len(),
        needs_fix.len()
    );

    // Build coin name lookup: (dex_idx, asset_idx) → coin_name
    let mut asset_to_coin: HashMap<(usize, u32), String> = HashMap::new();
    for (dex_idx, dex) in initial_state.dex_states.iter().enumerate() {
        for (i, meta) in dex.universe.iter().enumerate() {
            asset_to_coin.insert((dex_idx, i as u32), meta.name.clone());
        }
    }

    // Query API for each user
    let mut api_leverage: HashMap<String, HashMap<String, (bool, u32)>> = HashMap::new();
    let mut api_errors = 0usize;

    let client = ureq::Agent::new();
    for (i, user) in unique_users.iter().enumerate() {
        if i > 0 && i % 100 == 0 {
            info!("  queried {}/{} users...", i, unique_users.len());
        }

        match query_user_leverage(&client, user) {
            Ok(levs) => {
                api_leverage.insert(user.clone(), levs);
            }
            Err(e) => {
                if api_errors < 5 {
                    eprintln!("API error for {}: {}", user, e);
                }
                api_errors += 1;
            }
        }
    }

    if api_errors > 0 {
        info!("Total API errors: {}", api_errors);
    }

    // Inject leverage settings into the initial state
    let mut injected = 0usize;
    for (user, dex_idx, asset_idx) in needs_fix {
        let coin = match asset_to_coin.get(&(*dex_idx, *asset_idx)) {
            Some(c) => c.clone(),
            None => continue,
        };

        let Some(user_levs) = api_leverage.get(user) else { continue };
        let Some(&(is_cross, lev_value)) = user_levs.get(&coin) else { continue };

        let new_lev = if is_cross {
            Leverage::Cross(lev_value)
        } else {
            Leverage::Isolated { leverage: lev_value, raw_usd: 0 }
        };

        let Some(dex) = initial_state.dex_states.get_mut(*dex_idx) else { continue };

        // Inject into users (if they exist there)
        if let Some(user_state) = dex.users.get_mut(user.as_str()) {
            user_state.leverage_settings.entry(*asset_idx).or_insert(new_lev.clone());
            injected += 1;
            continue;
        }
        // Inject into users_without_positions
        if let Some(partial) = dex.users_without_positions.get_mut(user.as_str()) {
            partial.leverage_settings.entry(*asset_idx).or_insert(new_lev.clone());
            injected += 1;
            continue;
        }
        // User doesn't exist in initial state at all — create a partial entry
        // so when their first fill comes, they'll get the correct leverage
        dex.users_without_positions.entry(user.clone()).or_insert_with(|| {
            super::UserStatePartial {
                usdc_balance: 0,
                spot_collateral: 0,
                spot_collateral_decimals: 8,
                account_mode: super::AccountMode::Standard,
                leverage_settings: HashMap::new(),
            }
        }).leverage_settings.entry(*asset_idx).or_insert(new_lev);
        injected += 1;
    }

    info!("Injected {}/{} leverage settings from API ({} errors)", injected, needs_fix.len(), api_errors);
    injected
}

/// Query clearinghouseState for a user and extract per-coin leverage.
fn query_user_leverage(
    client: &ureq::Agent,
    user: &str,
) -> Result<HashMap<String, (bool, u32)>, String> {
    let body = serde_json::json!({
        "type": "clearinghouseState",
        "user": user,
    });

    let resp = client
        .post("https://api.hyperliquid.xyz/info")
        .set("Content-Type", "application/json")
        .send_json(&body)
        .map_err(|e| format!("request failed: {e}"))?;

    let data: serde_json::Value = resp.into_json().map_err(|e| format!("json parse: {e}"))?;

    let mut result = HashMap::new();

    // Extract from assetPositions
    if let Some(positions) = data.get("assetPositions").and_then(|v| v.as_array()) {
        for pos_wrapper in positions {
            let pos = pos_wrapper.get("position").unwrap_or(pos_wrapper);
            let coin = pos.get("coin").and_then(|v| v.as_str()).unwrap_or("");
            if coin.is_empty() {
                continue;
            }
            if let Some(lev) = pos.get("leverage") {
                let lev_type = lev.get("type").and_then(|v| v.as_str()).unwrap_or("cross");
                let lev_value = lev.get("value").and_then(|v| v.as_u64()).unwrap_or(20) as u32;
                let is_cross = lev_type == "cross";
                result.insert(coin.to_string(), (is_cross, lev_value));
            }
        }
    }

    Ok(result)
}
