//! Extract and print user data from an RMP snapshot.
//!
//! Usage: rmp_inspect [--raw] <rmp_file> <user_address> [user_address...]

// Shared workspace deps not directly used by this binary
use axum as _;
use log as _;
use tokio as _;

use server::clearing_house::{self, LiquidationState};
use std::collections::HashSet;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut raw_mode = false;
    let mut structure_mode = false;
    let mut positional = Vec::new();
    for arg in &args[1..] {
        if arg == "--raw" {
            raw_mode = true;
        } else if arg == "--structure" {
            structure_mode = true;
        } else {
            positional.push(arg.clone());
        }
    }

    if structure_mode {
        let path = PathBuf::from(positional.first().cloned().unwrap_or_default());
        if let Err(e) = clearing_house::dump_locus_ftr_keys(&path) {
            eprintln!("dump failed: {e}");
        }
        return;
    }

    if positional.len() < 2 {
        eprintln!("Usage: {} [--raw] <rmp_file> <user_address> [user_address...]", args[0]);
        std::process::exit(1);
    }

    let rmp_path = PathBuf::from(&positional[0]);
    let users: Vec<String> = positional[1..].iter().map(|s| s.to_lowercase()).collect();

    if raw_mode {
        let user_set: HashSet<String> = users.iter().cloned().collect();
        eprintln!("Extracting raw data from {}...", rmp_path.display());
        match clearing_house::extract_raw_debug_users_from_rmp(&rmp_path, &user_set) {
            Ok(output) => {
                // Pretty-print JSON objects found in the output.
                // They appear either as standalone lines or after key= prefixes.
                for line in output.lines() {
                    // Find the first '{' or '[' that starts a JSON value
                    if let Some(json_start) = line.find('{').or_else(|| line.find('[')) {
                        let json_part = &line[json_start..];
                        // Check it ends with matching bracket
                        let valid_json = (json_part.starts_with('{') && json_part.ends_with('}'))
                            || (json_part.starts_with('[') && json_part.ends_with(']'));
                        if valid_json {
                            if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_part) {
                                let prefix = &line[..json_start];
                                let pretty =
                                    serde_json::to_string_pretty(&val).unwrap_or_else(|_| json_part.to_string());
                                // Use original line's leading whitespace + 4 for continuation
                                let base_indent = line.len() - line.trim_start().len();
                                let pad: String = " ".repeat(base_indent + 4);
                                for (i, pline) in pretty.lines().enumerate() {
                                    if i == 0 {
                                        println!("{prefix}{pline}");
                                    } else {
                                        println!("{pad}{pline}");
                                    }
                                }
                                continue;
                            }
                        }
                    }
                    println!("{line}");
                }
            }
            Err(e) => {
                eprintln!("Failed to extract raw data: {e}");
                std::process::exit(1);
            }
        }
        return;
    }

    eprintln!("Parsing {}...", rmp_path.display());
    let state = match LiquidationState::load_from_rmp(&rmp_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to parse: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("Parsed. {} dexes", state.dex_states.len());

    for user in &users {
        println!("=== USER: {} ===", user);
        for (di, dex) in state.dex_states.iter().enumerate() {
            // Collect all asset indices relevant to this user (positions + lev_settings)
            let user_assets: HashSet<u32> = if let Some(us) = dex.users.get(user.as_str()) {
                us.positions.keys().chain(us.leverage_settings.keys()).copied().collect()
            } else if let Some(partial) = dex.users_without_positions.get(user.as_str()) {
                partial.leverage_settings.keys().copied().collect()
            } else {
                continue;
            };

            if let Some(us) = dex.users.get(user.as_str()) {
                println!("  dex[{}] pdi={} (users):", di, dex.pdi);
                println!("    usdc_balance={} (${:.2})", us.usdc_balance, us.usdc_balance as f64 / 1e6);
                println!("    spot_collateral={}", us.spot_collateral);
                println!("    account_mode={:?}", us.account_mode);
                let mut keys: Vec<_> = us.positions.keys().collect();
                keys.sort();
                for &k in &keys {
                    let p = &us.positions[k];
                    let coin = dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                    let mt = dex.universe.get(*k as usize).and_then(|a| dex.margin_tables.get(&a.margin_table_id));
                    let max_lev = mt.and_then(|t| t.first()).map(|t| t.max_leverage);
                    println!(
                        "    pos[{}] {}: szi={} cb={} lev={:?} funding={} max_lev={}",
                        k,
                        coin,
                        p.szi,
                        p.cost_basis,
                        p.leverage,
                        p.outstanding_funding,
                        max_lev.map(|v| v.to_string()).unwrap_or_else(|| "?".to_string())
                    );
                }
                let mut lkeys: Vec<_> = us.leverage_settings.keys().collect();
                lkeys.sort();
                for &k in &lkeys {
                    if !us.positions.contains_key(k) {
                        let lev = &us.leverage_settings[k];
                        let coin = dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                        let mt = dex.universe.get(*k as usize).and_then(|a| dex.margin_tables.get(&a.margin_table_id));
                        let max_lev = mt.and_then(|t| t.first()).map(|t| t.max_leverage);
                        println!(
                            "    lev_setting[{}] {}: {:?} max_lev={}",
                            k,
                            coin,
                            lev,
                            max_lev.map(|v| v.to_string()).unwrap_or_else(|| "?".to_string())
                        );
                    }
                }
            }
            if let Some(partial) = dex.users_without_positions.get(user.as_str()) {
                println!("  dex[{}] pdi={} (partial):", di, dex.pdi);
                println!("    usdc_balance={} (${:.2})", partial.usdc_balance, partial.usdc_balance as f64 / 1e6);
                println!("    spot_collateral={}", partial.spot_collateral);
                println!("    account_mode={:?}", partial.account_mode);
                let mut lkeys: Vec<_> = partial.leverage_settings.keys().collect();
                lkeys.sort();
                for &k in &lkeys {
                    let lev = &partial.leverage_settings[k];
                    let coin = dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                    let mt = dex.universe.get(*k as usize).and_then(|a| dex.margin_tables.get(&a.margin_table_id));
                    let max_lev = mt.and_then(|t| t.first()).map(|t| t.max_leverage);
                    println!(
                        "    lev_setting[{}] {}: {:?} max_lev={}",
                        k,
                        coin,
                        lev,
                        max_lev.map(|v| v.to_string()).unwrap_or_else(|| "?".to_string())
                    );
                }
            }

            // Print margin table info for assets in this user's state
            if !user_assets.is_empty() {
                let mut seen_tables: HashSet<u32> = HashSet::new();
                let mut table_entries: Vec<(u32, &str, u32)> = Vec::new();
                let mut sorted_assets: Vec<u32> = user_assets.into_iter().collect();
                sorted_assets.sort();
                for asset_idx in &sorted_assets {
                    if let Some(meta) = dex.universe.get(*asset_idx as usize) {
                        if seen_tables.insert(meta.margin_table_id) {
                            table_entries.push((*asset_idx, &meta.name, meta.margin_table_id));
                        }
                    }
                }
                if !table_entries.is_empty() {
                    println!("    --- margin tables ---");
                    for (_asset, _coin, table_id) in &table_entries {
                        if let Some(tiers) = dex.margin_tables.get(table_id) {
                            let tier_strs: Vec<String> = tiers
                                .iter()
                                .map(|t| {
                                    format!(
                                        "{{lb={}, max_lev={}, maint_ded={}}}",
                                        t.lower_bound, t.max_leverage, t.maintenance_deduction
                                    )
                                })
                                .collect();
                            println!("    table[{}]: [{}]", table_id, tier_strs.join(", "));
                        }
                    }
                }
            }
        }
        println!();
    }
}
