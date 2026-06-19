//! Trace all state and events for a user between two block heights.
//!
//! Usage: user_trace --user <address> --from-block <N> --to-block <N> [--home-dir <path>]

use axum as _;
use log as _;
use tokio as _;

use clap::Parser;
use server::clearing_house::LiquidationState;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

#[derive(Debug, Parser)]
#[command(about = "Trace all state changes and events for a user between two snapshots")]
struct Args {
    /// User address to trace
    #[arg(long)]
    user: String,

    /// Starting block height (first snapshot)
    #[arg(long)]
    from_block: u64,

    /// Ending block height (second snapshot)
    #[arg(long)]
    to_block: u64,

    /// Home directory containing hl/data/
    #[arg(long, default_value_t = default_home())]
    home_dir: String,

    /// Data directory (defaults to <home_dir>/hl/data)
    #[arg(long)]
    data_dir: Option<String>,
}

fn default_home() -> String {
    dirs::home_dir().map(|p: PathBuf| p.to_string_lossy().to_string()).unwrap_or_else(|| ".".to_string())
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(path)
}

// ── Event representation ─────────────────────────────────────────────────

#[derive(Debug)]
struct Event {
    block: u64,
    source: &'static str,
    summary: String,
    detail: String,
}

fn main() {
    let args = Args::parse();
    let home_dir = expand_tilde(&args.home_dir);
    let data_dir = args.data_dir.as_deref().map(expand_tilde).unwrap_or_else(|| home_dir.join("hl/data"));
    let user = args.user.to_lowercase();

    // ── Step 1: Find and parse snapshots ──────────────────────────────────
    let rmp_a = find_rmp(&home_dir, args.from_block);
    let rmp_b = find_rmp(&home_dir, args.to_block);

    if let Some(ref path) = rmp_a {
        eprintln!("Snapshot A: {} (block {})", path.display(), args.from_block);
        eprintln!("Parsing...");
        match LiquidationState::load_from_rmp(path) {
            Ok(state) => {
                println!("=== SNAPSHOT A (block {}) ===", args.from_block);
                print_user_state(&state, &user);
            }
            Err(e) => eprintln!("Failed to parse snapshot A: {e}"),
        }
    } else {
        eprintln!("Snapshot A not found for block {}", args.from_block);
    }

    if let Some(ref path) = rmp_b {
        eprintln!("Snapshot B: {} (block {})", path.display(), args.to_block);
        eprintln!("Parsing...");
        match LiquidationState::load_from_rmp(path) {
            Ok(state) => {
                println!("\n=== SNAPSHOT B (block {}) ===", args.to_block);
                print_user_state(&state, &user);
            }
            Err(e) => eprintln!("Failed to parse snapshot B: {e}"),
        }
    } else {
        eprintln!("Snapshot B not found for block {}", args.to_block);
    }

    // ── Step 2: Collect all events ───────────────────────────────────────
    let mut events: BTreeMap<u64, Vec<Event>> = BTreeMap::new();

    eprintln!("\nScanning fills...");
    collect_fills(&data_dir, &user, args.from_block, args.to_block, &mut events);

    eprintln!("Scanning replica_cmds...");
    collect_replica(&home_dir, &user, args.from_block, args.to_block, &mut events);

    eprintln!("Scanning misc_events...");
    collect_misc_events(&data_dir, &user, args.from_block, args.to_block, &mut events);

    // ── Step 3: Print chronologically ────────────────────────────────────
    let total: usize = events.values().map(|v| v.len()).sum();
    println!("\n=== EVENTS (blocks {}..{}, {} total) ===", args.from_block, args.to_block, total);

    for (block, block_events) in &events {
        for ev in block_events {
            println!("[block={} src={}] {}", block, ev.source, ev.summary);
            if !ev.detail.is_empty() {
                for line in ev.detail.lines() {
                    println!("  {}", line);
                }
            }
        }
    }

    if total == 0 {
        println!("  (no events found for this user in range)");
    }
}

// ── User state printer ───────────────────────────────────────────────────

fn print_user_state(state: &LiquidationState, user: &str) {
    let mut found = false;
    for (di, dex) in state.dex_states.iter().enumerate() {
        if let Some(us) = dex.users.get(user) {
            found = true;
            println!("  dex[{}] pdi={} (users):", di, dex.pdi);
            println!(
                "    usdc=${:.2} scl={} mode={:?}",
                us.usdc_balance as f64 / 1e6,
                us.spot_collateral,
                us.account_mode
            );
            let mut keys: Vec<_> = us.positions.keys().collect();
            keys.sort();
            for &k in &keys {
                let p = &us.positions[k];
                let coin = dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                println!(
                    "    pos[{}] {}: szi={} cb={} lev={:?} funding={}",
                    k, coin, p.szi, p.cost_basis, p.leverage, p.outstanding_funding
                );
            }
            let mut lkeys: Vec<_> = us.leverage_settings.keys().filter(|k| !us.positions.contains_key(*k)).collect();
            lkeys.sort();
            for &k in &lkeys {
                let lev = &us.leverage_settings[k];
                let coin = dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                println!("    lev[{}] {}: {:?}", k, coin, lev);
            }
        }
        if let Some(partial) = dex.users_without_positions.get(user) {
            found = true;
            println!("  dex[{}] pdi={} (partial):", di, dex.pdi);
            println!(
                "    usdc=${:.2} scl={} mode={:?}",
                partial.usdc_balance as f64 / 1e6,
                partial.spot_collateral,
                partial.account_mode
            );
            let mut lkeys: Vec<_> = partial.leverage_settings.keys().collect();
            lkeys.sort();
            for &k in &lkeys {
                let lev = &partial.leverage_settings[k];
                let coin = dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                println!("    lev[{}] {}: {:?}", k, coin, lev);
            }
        }
    }
    if !found {
        println!("  (user not found in any dex)");
    }
}

// ── Fill collector ───────────────────────────────────────────────────────

fn collect_fills(data_dir: &Path, user: &str, from_block: u64, to_block: u64, events: &mut BTreeMap<u64, Vec<Event>>) {
    let fills_dir = data_dir.join("node_fills_streaming");
    if !fills_dir.exists() {
        return;
    }

    let mut files = Vec::new();
    collect_files_recursive(&fills_dir, &mut files);
    files.sort();

    for path in &files {
        let Ok(file) = std::fs::File::open(path) else { continue };
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let Ok(line) = line else { break };
            if !line.contains(user) {
                continue;
            }
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            let bn = val.get("block_number").and_then(|v| v.as_u64()).unwrap_or(0);
            if bn <= from_block || bn > to_block {
                continue;
            }

            let Some(evts) = val.get("events").and_then(|v| v.as_array()) else { continue };
            for ev in evts {
                let Some(arr) = ev.as_array() else { continue };
                if arr.len() < 2 {
                    continue;
                }
                let ev_user = arr[0].as_str().unwrap_or("").to_lowercase();
                if ev_user != user {
                    continue;
                }

                let fill = &arr[1];
                let coin = fill.get("coin").and_then(|v| v.as_str()).unwrap_or("?");
                let side = fill.get("side").and_then(|v| v.as_str()).unwrap_or("?");
                let sz = fill.get("sz").and_then(|v| v.as_str()).unwrap_or("?");
                let px = fill.get("px").and_then(|v| v.as_str()).unwrap_or("?");
                let fee = fill.get("fee").and_then(|v| v.as_str()).unwrap_or("?");
                let start_pos = fill.get("startPosition").and_then(|v| v.as_str()).unwrap_or("?");
                let dir = fill.get("dir").and_then(|v| v.as_str()).unwrap_or("?");
                let closed_pnl = fill.get("closedPnl").and_then(|v| v.as_str()).unwrap_or("0");

                events.entry(bn).or_default().push(Event {
                    block: bn,
                    source: "fill",
                    summary: format!(
                        "{} {} {} sz={} px={} startPos={} fee={} dir={} closedPnl={}",
                        coin,
                        side_name(side),
                        dir,
                        sz,
                        px,
                        start_pos,
                        fee,
                        dir,
                        closed_pnl
                    ),
                    detail: String::new(),
                });
            }
        }
    }
}

fn side_name(s: &str) -> &str {
    match s {
        "A" => "Ask/Sell",
        "B" => "Bid/Buy",
        _ => s,
    }
}

// ── Replica collector ────────────────────────────────────────────────────

fn collect_replica(
    home_dir: &Path,
    user: &str,
    from_block: u64,
    to_block: u64,
    events: &mut BTreeMap<u64, Vec<Event>>,
) {
    let replica_dir = home_dir.join("hl/data/replica_cmds");
    if !replica_dir.exists() {
        return;
    }

    let mut files = Vec::new();
    collect_files_recursive(&replica_dir, &mut files);
    files.sort();

    for path in &files {
        // Filter by filename (block number)
        let file_block: Option<u64> = path.file_name().and_then(|s| s.to_str()).and_then(|s| s.parse().ok());
        if let Some(fb) = file_block {
            if fb > to_block || fb + 10_000 <= from_block {
                continue;
            }
        }

        let Ok(file) = std::fs::File::open(path) else { continue };
        let reader = BufReader::new(file);
        let base_block = file_block.unwrap_or(0);

        for (line_idx, line) in reader.lines().enumerate() {
            let Ok(line) = line else { break };
            if !line.to_lowercase().contains(user) {
                continue;
            }
            let block_num = base_block + line_idx as u64;
            if block_num <= from_block || block_num > to_block {
                continue;
            }

            let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) else { continue };

            // Check signed_action_bundles
            let bundles = val.pointer("/abci_block/signed_action_bundles").and_then(|v| v.as_array());
            let resps = val.get("resps");

            let resp_bundles: Vec<Option<&Vec<serde_json::Value>>> = match resps {
                Some(serde_json::Value::Object(obj)) => {
                    if let Some(serde_json::Value::Array(full)) = obj.get("Full") {
                        full.iter()
                            .map(|entry| {
                                entry.as_array().and_then(|a| if a.len() >= 2 { a[1].as_array() } else { None })
                            })
                            .collect()
                    } else {
                        Vec::new()
                    }
                }
                _ => Vec::new(),
            };

            if let Some(bundles) = bundles {
                for (bi, bundle) in bundles.iter().enumerate() {
                    let Some(arr) = bundle.as_array() else { continue };
                    if arr.len() < 2 {
                        continue;
                    }
                    let signer = arr[0].as_str().unwrap_or("").to_lowercase();

                    let Some(action_bundle) = arr[1].as_object() else { continue };
                    let Some(signed_actions) = action_bundle.get("signed_actions").and_then(|v| v.as_array()) else {
                        continue;
                    };

                    let bundle_resps = resp_bundles.get(bi).and_then(|r| r.as_ref());

                    for (si, sa) in signed_actions.iter().enumerate() {
                        let action = sa.get("action").unwrap_or(sa);
                        let vault_addr = sa.get("vaultAddress").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
                        let action_str = serde_json::to_string(action).unwrap_or_default().to_lowercase();
                        let atype = action.get("type").and_then(|v| v.as_str()).unwrap_or("?");

                        // Check if user is signer, vaultAddress, or referenced in action
                        let is_signer = signer == user;
                        let is_vault = vault_addr == user;
                        let is_in_action = action_str.contains(user);

                        // Check if user is in resp
                        let resp_user = bundle_resps
                            .and_then(|resps| resps.get(si))
                            .and_then(|r| r.get("user"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_lowercase());
                        let is_in_resp = resp_user.as_deref() == Some(user);

                        if !is_signer && !is_vault && !is_in_action && !is_in_resp {
                            continue;
                        }

                        // Skip pure order/cancel unless user is signer
                        if matches!(atype, "order" | "cancel" | "cancelByCloid" | "batchModify") && !is_signer {
                            continue;
                        }

                        let resp_status = bundle_resps
                            .and_then(|resps| resps.get(si))
                            .and_then(|r| r.get("res"))
                            .and_then(|r| r.get("status"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("ok");

                        let role = if is_signer {
                            "signer"
                        } else if is_vault {
                            "vaultAddr"
                        } else if is_in_resp {
                            "resp_user"
                        } else {
                            "referenced"
                        };

                        let detail = serde_json::to_string_pretty(action).unwrap_or_default();

                        events.entry(block_num).or_default().push(Event {
                            block: block_num,
                            source: "replica",
                            summary: format!(
                                "type={} role={} signer={}.. status={}",
                                atype,
                                role,
                                &signer[..14.min(signer.len())],
                                resp_status
                            ),
                            detail,
                        });
                    }
                }
            }
        }
    }
}

// ── Misc events collector ────────────────────────────────────────────────

fn collect_misc_events(
    data_dir: &Path,
    user: &str,
    from_block: u64,
    to_block: u64,
    events: &mut BTreeMap<u64, Vec<Event>>,
) {
    let misc_dir = data_dir.join("misc_events_streaming");
    if !misc_dir.exists() {
        return;
    }

    let mut files = Vec::new();
    collect_files_recursive(&misc_dir, &mut files);
    files.sort();

    for path in &files {
        let Ok(file) = std::fs::File::open(path) else { continue };
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let Ok(line) = line else { break };
            if !line.to_lowercase().contains(user) {
                continue;
            }
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&line) else { continue };
            let bn = val.get("block_number").and_then(|v| v.as_u64()).unwrap_or(0);
            if bn <= from_block || bn > to_block {
                continue;
            }

            let Some(evts) = val.get("events").and_then(|v| v.as_array()) else { continue };
            for ev in evts {
                let ev_str = serde_json::to_string(ev).unwrap_or_default().to_lowercase();
                if !ev_str.contains(user) {
                    continue;
                }

                let Some(inner) = ev.get("inner").and_then(|v| v.as_object()) else { continue };
                let Some((kind, payload)) = inner.iter().next() else { continue };

                match kind.as_str() {
                    "Funding" => {
                        if let Some(deltas) = payload.get("deltas").and_then(|v| v.as_array()) {
                            for d in deltas {
                                let d_user = d.get("user").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
                                if d_user != user {
                                    continue;
                                }
                                let coin = d.get("coin").and_then(|v| v.as_str()).unwrap_or("?");
                                let amt = d.get("funding_amount").and_then(|v| v.as_str()).unwrap_or("?");
                                let szi = d.get("szi").and_then(|v| v.as_str()).unwrap_or("?");
                                let rate = d.get("fundingRate").and_then(|v| v.as_str()).unwrap_or("?");
                                events.entry(bn).or_default().push(Event {
                                    block: bn,
                                    source: "misc",
                                    summary: format!("Funding coin={} amt={} szi={} rate={}", coin, amt, szi, rate),
                                    detail: String::new(),
                                });
                            }
                        }
                    }
                    "LedgerUpdate" => {
                        let delta = payload.get("delta").unwrap_or(payload);
                        let dtype = delta.get("type").and_then(|v| v.as_str()).unwrap_or("?");
                        let detail = serde_json::to_string_pretty(delta).unwrap_or_default();
                        events.entry(bn).or_default().push(Event {
                            block: bn,
                            source: "misc",
                            summary: format!("LedgerUpdate type={}", dtype),
                            detail,
                        });
                    }
                    _ => {
                        let detail = serde_json::to_string_pretty(payload).unwrap_or_default();
                        events.entry(bn).or_default().push(Event {
                            block: bn,
                            source: "misc",
                            summary: format!("{}", kind),
                            detail,
                        });
                    }
                }
            }
        }
    }
}

// ── RMP file finder ──────────────────────────────────────────────────────

fn find_rmp(home_dir: &Path, block: u64) -> Option<PathBuf> {
    // Check replay_analysis first
    let analysis_dir = home_dir.join("replay_analysis");
    if analysis_dir.exists() {
        for entry in std::fs::read_dir(&analysis_dir).ok()?.flatten() {
            let rmp = entry.path().join(format!("{block}.rmp"));
            if rmp.exists() {
                return Some(rmp);
            }
        }
    }

    // Check periodic_abci_states
    let abci_dir = home_dir.join("hl/data/periodic_abci_states");
    if abci_dir.exists() {
        let mut dirs: Vec<_> = std::fs::read_dir(&abci_dir).ok()?.flatten().filter(|e| e.path().is_dir()).collect();
        dirs.sort_by_key(|e| e.file_name());
        for dir in dirs {
            let rmp = dir.path().join(format!("{block}.rmp"));
            if rmp.exists() {
                return Some(rmp);
            }
        }
    }

    None
}

fn collect_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files_recursive(&path, out);
        } else if path.is_file() {
            out.push(path);
        }
    }
}
