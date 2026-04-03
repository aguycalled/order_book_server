//! Replay verification binary.
//!
//! Parses a selected ABCI snapshot, replays fills and replica_cmds until the
//! next snapshot, then compares the resulting state against that next snapshot
//! as ground truth.

// Shared workspace deps not directly used by this binary
use axum as _;
use log as _;
use tokio as _;

use clap::Parser;
use server::clearing_house::{
    LiquidationState, block_height_from_rmp, compare_states, extract_raw_debug_misc_events,
    extract_raw_debug_users_from_rmp, find_all_rmp_files, replay_interleaved,
};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Parser)]
#[command(author, version, about = "Verify clearing house state replay between two ABCI snapshots")]
struct Args {
    /// Home directory containing hl/data/
    #[arg(long, default_value_t = default_home())]
    home_dir: String,

    /// Data directory containing node_fills_streaming/ (defaults to <home_dir>/hl/data)
    #[arg(long)]
    data_dir: Option<String>,

    /// Starting snapshot block height. If omitted, uses the second-to-last snapshot.
    #[arg(long)]
    from_block: Option<u64>,

    /// Debug specific user addresses (comma-separated, lowercase).
    /// Traces all state changes for these users.
    #[arg(long)]
    debug_users: Option<String>,

    /// Directory to write analysis artifacts (results, RMP copies, translated JSON).
    /// Default: ~/replay_analysis
    #[arg(long)]
    analysis_dir: Option<String>,

    /// Path to hl-node binary for translating RMP to JSON.
    /// Default: hl-node (assumes in PATH)
    #[arg(long, default_value = "~/hl-node")]
    hlnode_binary: String,

    /// Query the HL API to fix leverage for new positions with unknown settings.
    #[arg(long)]
    api_leverage: bool,

    /// Write per-user event traces for all drifting users to traces/ subfolder.
    #[arg(long)]
    trace_drifters: bool,

    /// Self-test: parse the first snapshot twice and compare. Zero drifts = parsing is lossless.
    #[arg(long)]
    self_test: bool,
}

fn default_home() -> String {
    dirs::home_dir().map(|p: PathBuf| p.to_string_lossy().to_string()).unwrap_or_else(|| ".".to_string())
}

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let home_dir = expand_tilde_path(&args.home_dir);
    let data_dir = args.data_dir.as_deref().map(expand_tilde_path).unwrap_or_else(|| home_dir.join("hl/data"));

    println!("=== Replay Verification ===");
    println!("Home dir: {}", home_dir.display());
    println!("Data dir: {}", data_dir.display());
    println!();

    // Step 1: Find RMP files
    let rmp_files = match find_all_rmp_files(&home_dir) {
        Ok(files) => files,
        Err(e) => {
            eprintln!("Failed to find RMP files: {e}");
            std::process::exit(1);
        }
    };

    if rmp_files.len() < 2 {
        eprintln!("Need at least 2 RMP files, found {}. Available:", rmp_files.len());
        for f in &rmp_files {
            eprintln!("  {}", f.display());
        }
        std::process::exit(1);
    }

    let (first_idx, second_idx) = match select_snapshot_pair(&rmp_files, args.from_block) {
        Ok(pair) => pair,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    let first = &rmp_files[first_idx];
    let second = &rmp_files[second_idx];
    let block_a = block_height_from_rmp(first).unwrap_or(0);
    let block_b = block_height_from_rmp(second).unwrap_or(0);

    println!("First RMP: {} (block {})", first.display(), block_a);
    println!("Second RMP: {} (block {})", second.display(), block_b);
    println!();

    // Parse debug users
    let debug_users: std::collections::HashSet<String> = args
        .debug_users
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_lowercase())
        .collect();
    if !debug_users.is_empty() {
        println!("Debug users: {:?}", debug_users);
    }
    let mut debug_output = String::new();
    if !debug_users.is_empty() {
        match extract_raw_debug_users_from_rmp(first, &debug_users) {
            Ok(raw) if !raw.is_empty() => {
                eprintln!("{raw}");
                debug_output.push_str(&raw);
                if !raw.ends_with('\n') {
                    debug_output.push('\n');
                }
            }
            Ok(_) => {}
            Err(e) => {
                let line = format!("[DEBUG raw_init] failed to extract raw snapshot data: {e}");
                eprintln!("{line}");
                debug_output.push_str(&line);
                debug_output.push('\n');
            }
        }

        match extract_raw_debug_misc_events(&data_dir, &debug_users, block_a, block_b) {
            Ok(raw) if !raw.is_empty() => {
                eprintln!("{raw}");
                debug_output.push_str(&raw);
                if !raw.ends_with('\n') {
                    debug_output.push('\n');
                }
            }
            Ok(_) => {}
            Err(e) => {
                let line = format!("[DEBUG raw_misc] failed to extract related misc events: {e}");
                eprintln!("{line}");
                debug_output.push_str(&line);
                debug_output.push('\n');
            }
        }
    }

    let mut append_debug_snapshot = |label: &str, state: &LiquidationState| {
        if debug_users.is_empty() {
            return;
        }

        debug_output.push_str(&format!("=== {label} ===\n"));
        for user in &debug_users {
            for (di, dex) in state.dex_states.iter().enumerate() {
                if let Some(us) = dex.users.get(user) {
                    let positions = {
                        let mut keys: Vec<_> = us.positions.keys().copied().collect();
                        keys.sort_unstable();
                        keys.into_iter()
                            .map(|k| {
                                let p = &us.positions[&k];
                                let coin = dex.universe.get(k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                                format!(
                                    "{k}:{coin}(szi={}, cb={}, lev={:?}, funding={})",
                                    p.szi, p.cost_basis, p.leverage, p.outstanding_funding
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    let lev_settings = {
                        let mut keys: Vec<_> = us.leverage_settings.keys().copied().collect();
                        keys.sort_unstable();
                        keys.into_iter()
                            .map(|k| {
                                let lev = &us.leverage_settings[&k];
                                let coin = dex.universe.get(k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                                format!("{k}:{coin}={lev:?}")
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    let mode = format!("{:?}", us.account_mode);
                    let line = format!(
                        "[DEBUG {label}] user={} dex={} pdi={} mode={} collateral_token={} usdc=${:.2} scl=${:.2} positions=[{}] lev_settings=[{}]",
                        user,
                        di,
                        dex.pdi,
                        mode,
                        dex.collateral_token,
                        us.usdc_balance as f64 / 1e6,
                        us.spot_collateral as f64 / 1e8,
                        positions,
                        lev_settings
                    );
                    eprintln!("{line}");
                    debug_output.push_str(&line);
                    debug_output.push('\n');
                }
                if let Some(partial) = dex.users_without_positions.get(user) {
                    let lev_settings = {
                        let mut keys: Vec<_> = partial.leverage_settings.keys().copied().collect();
                        keys.sort_unstable();
                        keys.into_iter()
                            .map(|k| {
                                let lev = &partial.leverage_settings[&k];
                                let coin = dex.universe.get(k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                                format!("{k}:{coin}={lev:?}")
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    };
                    let mode = format!("{:?}", partial.account_mode);
                    let line = format!(
                        "[DEBUG {label}] user={} dex={} pdi={} (partial) mode={} collateral_token={} usdc=${:.2} scl=${:.2} positions=[] lev_settings=[{}]",
                        user,
                        di,
                        dex.pdi,
                        mode,
                        dex.collateral_token,
                        partial.usdc_balance as f64 / 1e6,
                        partial.spot_collateral as f64 / 1e8,
                        lev_settings
                    );
                    eprintln!("{line}");
                    debug_output.push_str(&line);
                    debug_output.push('\n');
                }
            }
        }
        debug_output.push('\n');
    };

    // Step 2: Parse first snapshot
    println!("Parsing first RMP...");
    let t = Instant::now();
    let mut state_a = match LiquidationState::load_from_rmp(first) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to parse {}: {e}", first.display());
            std::process::exit(1);
        }
    };
    state_a.debug_users = debug_users.clone();
    let parse_a_time = t.elapsed();
    print_state_summary("State A (before replay)", &state_a);
    append_debug_snapshot("init", &state_a);
    println!("  Parsed in {:.1}s", parse_a_time.as_secs_f64());
    println!();

    // Self-test: parse the same snapshot twice, compare — any diff means parsing is lossy
    if args.self_test {
        println!("=== SELF-TEST: parsing {} twice and comparing ===", first.display());
        let state_b = match LiquidationState::load_from_rmp(first) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to re-parse {}: {e}", first.display());
                std::process::exit(1);
            }
        };
        let results = compare_states(&state_a, &state_b);
        let mut total_drifts = 0;
        let mut total_lev = 0;
        let mut total_szi = 0;
        for r in &results {
            total_drifts += r.balance_drifts.len();
            total_lev += r.leverage_mismatches.len();
            total_szi += r.szi_mismatches.len();
            let dex_name = state_a.dex_states.get(r.dex_idx)
                .map(|d| format!("pdi={}", d.pdi))
                .unwrap_or_else(|| "?".to_string());
            if !r.balance_drifts.is_empty() || !r.leverage_mismatches.is_empty() || !r.szi_mismatches.is_empty() {
                println!("  Dex {} ({}): {} balance drifts, {} leverage mismatches, {} szi mismatches",
                    r.dex_idx, dex_name, r.balance_drifts.len(), r.leverage_mismatches.len(), r.szi_mismatches.len());
                for d in r.balance_drifts.iter().take(5) {
                    println!("    {} replay=${:.2} truth=${:.2} diff=${:.2}",
                        d.user, d.replay_balance as f64 / 1e6, d.truth_balance as f64 / 1e6, d.diff_usd);
                }
                for m in r.leverage_mismatches.iter().take(5) {
                    println!("    {} asset={} replay={} truth={}", m.user, m.asset_idx, m.replay_lev, m.truth_lev);
                }
            }
        }
        if total_drifts == 0 && total_lev == 0 && total_szi == 0 {
            println!("  PASS: parsing is lossless — zero diffs across all {} dexes", results.len());
        } else {
            println!("  FAIL: {} balance drifts, {} leverage mismatches, {} szi mismatches", total_drifts, total_lev, total_szi);
        }
        println!();
        if args.from_block.is_none() {
            // If only self-testing (no specific block), exit here
            std::process::exit(if total_drifts + total_lev + total_szi == 0 { 0 } else { 1 });
        }
    }

    // Check if backed-up data exists in analysis dir (from a previous run)
    // and use it if the original data has been purged.
    let analysis_dir_for_data = args
        .analysis_dir
        .as_deref()
        .map(expand_tilde_path)
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")).join("replay_analysis"));
    let backup_data_dir = analysis_dir_for_data.join(format!("{block_a}_{block_b}")).join("data");

    let effective_data_dir;
    let effective_home_dir;
    if backup_data_dir.exists() {
        // Use backed-up data as additional source.
        // Create a temp dir that merges backup + live data by symlinking.
        // Simpler: just use the backup as data_dir if it has the needed subdirs.
        let backup_fills = backup_data_dir.join("node_fills_streaming");
        let backup_replica = backup_data_dir.join("replica_cmds");
        let backup_misc = backup_data_dir.join("misc_events_streaming");

        // Check if live data has replica_cmds for our range
        let live_replica = home_dir.join("hl/data/replica_cmds");
        let has_live_replica = live_replica.exists() && {
            let mut found = false;
            if let Ok(entries) = std::fs::read_dir(&live_replica) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        if let Ok(sub) = std::fs::read_dir(&path) {
                            for sub_entry in sub.flatten() {
                                if let Some(name) = sub_entry.file_name().to_str() {
                                    if let Ok(bn) = name.parse::<u64>() {
                                        if bn <= block_b && bn + 10_000 > block_a {
                                            found = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if found { break; }
                }
            }
            found
        };

        if !has_live_replica && backup_replica.exists() {
            println!("Live replica_cmds purged — using backed-up data from {}", backup_data_dir.display());
            // Point home_dir to a fake path where hl/data/replica_cmds is the backup
            // Create symlink: backup_data_dir/hl/data/replica_cmds -> backup_data_dir/replica_cmds
            let fake_hl = backup_data_dir.join("hl").join("data").join("replica_cmds");
            if !fake_hl.exists() {
                let _ = std::fs::create_dir_all(fake_hl.parent().unwrap());
                let _ = std::os::unix::fs::symlink(&backup_replica, &fake_hl);
            }
            effective_data_dir = backup_data_dir.clone();
            effective_home_dir = backup_data_dir.clone();
        } else {
            effective_data_dir = data_dir.clone();
            effective_home_dir = home_dir.clone();
        }
    } else {
        effective_data_dir = data_dir.clone();
        effective_home_dir = home_dir.clone();
    }

    // Step 3: First-pass replay
    if args.trace_drifters {
        state_a.enable_event_log();
    }
    println!("Pass 1: Replaying fills + replica from block {} to {}...", block_a, block_b);
    let t = Instant::now();
    let (n_fills, n_replica) = replay_interleaved(&effective_data_dir, &effective_home_dir, &mut state_a, block_a, block_b);
    let replay_time = t.elapsed();
    println!("  {} fills, {} replica blocks in {:.1}s", n_fills, n_replica, replay_time.as_secs_f64());
    let needs_api_fix = state_a.positions_needing_leverage_fix.len();
    println!("  {} positions need leverage fix from API", needs_api_fix);
    if !state_a.mark_prices.is_empty() {
        println!("  {} mark prices indexed from replica_cmds", state_a.mark_prices.len());
    }
    println!();

    // Step 4: If --api-leverage, do two-pass: query API, re-parse, re-replay
    if args.api_leverage && needs_api_fix > 0 {
        println!("Querying HL API for leverage data...");
        let t = Instant::now();

        // Re-parse the initial snapshot
        let mut state_a_fresh = match LiquidationState::load_from_rmp(first) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Failed to re-parse {}: {e}", first.display());
                std::process::exit(1);
            }
        };
        state_a_fresh.debug_users = debug_users.clone();

        // Inject API leverage into the fresh initial state
        let injected = server::clearing_house::api_leverage::inject_leverage_from_api(&state_a, &mut state_a_fresh);
        println!("  Injected {} leverage settings in {:.1}s", injected, t.elapsed().as_secs_f64());

        // Second-pass replay with correct leverage
        println!("Pass 2: Replaying with corrected leverage...");
        let t = Instant::now();
        let (n_fills2, n_replica2) = replay_interleaved(&effective_data_dir, &effective_home_dir, &mut state_a_fresh, block_a, block_b);
        let replay2_time = t.elapsed();
        let remaining = state_a_fresh.positions_needing_leverage_fix.len();
        println!("  {} fills, {} replica blocks in {:.1}s", n_fills2, n_replica2, replay2_time.as_secs_f64());
        println!("  {} positions still need fix (API didn't have data)", remaining);
        println!();

        // Use the second-pass result
        state_a = state_a_fresh;
    }

    append_debug_snapshot("replay", &state_a);

    // Step 5: Parse second snapshot (ground truth)
    println!("Parsing second RMP (ground truth)...");
    let t = Instant::now();
    let state_b = match LiquidationState::load_from_rmp(second) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to parse {}: {e}", second.display());
            std::process::exit(1);
        }
    };
    let parse_b_time = t.elapsed();
    print_state_summary("State B (ground truth)", &state_b);
    append_debug_snapshot("truth", &state_b);
    println!("  Parsed in {:.1}s", parse_b_time.as_secs_f64());
    println!();

    // Step 6: Compare
    println!("=== Comparison Results ===");
    println!();
    let results = compare_states(&state_a, &state_b);

    for r in &results {
        // Skip empty dexes
        if r.users_in_truth == 0 && r.users_in_replay == 0 {
            continue;
        }

        let dex_name =
            state_b.dex_states.get(r.dex_idx).map(|d| format!("pdi={}", d.pdi)).unwrap_or_else(|| "?".to_string());

        println!("--- Dex {} ({}) ---", r.dex_idx, dex_name);
        println!("  Users in truth:     {}", r.users_in_truth);
        println!("  Users in replay:    {}", r.users_in_replay);
        println!("  Position szi match: {}", r.szi_matches);
        println!("  Position szi mismatches: {}", r.szi_mismatches.len());
        println!("  Balance drifts (>$1): {}", r.balance_drifts.len());
        println!("  Leverage mismatches: {}", r.leverage_mismatches.len());
        println!("  Cost basis drifts: {}", r.cost_basis_drifts.len());
        println!("  Raw USD drifts: {}", r.raw_usd_drifts.len());
        println!("  Funding drifts: {}", r.funding_drifts.len());
        println!("  SCL drifts (>$1): {}", r.scl_drifts.len());
        println!("  Missing after replay: {}", r.missing_after_replay.len());
        println!("  Extra after replay: {}", r.extra_after_replay.len());

        // Print first N details for each category
        let max_detail = 20;

        if !r.szi_mismatches.is_empty() {
            println!("\n  Top szi mismatches:");
            for m in r.szi_mismatches.iter().take(max_detail) {
                println!(
                    "    {} asset={} coin={}: replay={} truth={}",
                    &m.user, m.asset_idx, m.coin, m.replay_szi, m.truth_szi,
                );
            }
            if r.szi_mismatches.len() > max_detail {
                println!("    ... and {} more", r.szi_mismatches.len() - max_detail);
            }
        }

        if !r.balance_drifts.is_empty() {
            println!("\n  Top balance drifts:");
            let mut sorted: Vec<_> = r.balance_drifts.iter().collect();
            sorted.sort_by(|a, b| b.diff_usd.abs().partial_cmp(&a.diff_usd.abs()).unwrap_or(std::cmp::Ordering::Equal));
            for d in sorted.iter().take(max_detail) {
                let actions = state_a.user_action_counts.get(&d.user).copied().unwrap_or(0);
                println!(
                    "    {} replay=${:.2} truth=${:.2} diff=${:.2} ({:.4}%) actions={}",
                    &d.user,
                    d.replay_balance as f64 / 1e6,
                    d.truth_balance as f64 / 1e6,
                    d.diff_usd,
                    d.pct,
                    actions,
                );
            }
            if r.balance_drifts.len() > max_detail {
                println!("    ... and {} more", r.balance_drifts.len() - max_detail);
            }
        }

        if !r.leverage_mismatches.is_empty() {
            println!("\n  Top leverage mismatches:");
            for m in r.leverage_mismatches.iter().take(max_detail) {
                println!(
                    "    {} asset={} coin={}: replay={} truth={}",
                    &m.user, m.asset_idx, m.coin, m.replay_lev, m.truth_lev,
                );
            }
        }

        if !r.missing_after_replay.is_empty() {
            println!(
                "\n  Missing users (first {}): {:?}",
                max_detail.min(r.missing_after_replay.len()),
                &r.missing_after_replay[..max_detail.min(r.missing_after_replay.len())]
            );
        }

        if !r.extra_after_replay.is_empty() {
            println!(
                "\n  Extra users (first {}): {:?}",
                max_detail.min(r.extra_after_replay.len()),
                &r.extra_after_replay[..max_detail.min(r.extra_after_replay.len())]
            );
        }

        println!();
    }

    // Step 7: Write analysis artifacts
    let analysis_dir = args
        .analysis_dir
        .as_deref()
        .map(expand_tilde_path)
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")).join("replay_analysis"));
    let run_dir = analysis_dir.join(format!("{block_a}_{block_b}"));
    std::fs::create_dir_all(&run_dir).unwrap_or_else(|e| {
        eprintln!("Failed to create analysis dir {}: {e}", run_dir.display());
    });

    // Write comparison results to file
    let results_path = run_dir.join("results.txt");
    if let Ok(mut f) = std::fs::File::create(&results_path) {
        use std::io::Write;
        for r in &results {
            if r.users_in_truth == 0 && r.users_in_replay == 0 {
                continue;
            }
            let dex_name =
                state_b.dex_states.get(r.dex_idx).map(|d| format!("pdi={}", d.pdi)).unwrap_or_else(|| "?".to_string());
            writeln!(f, "--- Dex {} ({}) ---", r.dex_idx, dex_name).ok();
            writeln!(f, "  Users in truth: {}", r.users_in_truth).ok();
            writeln!(f, "  Users in replay: {}", r.users_in_replay).ok();
            writeln!(f, "  Position szi match: {}", r.szi_matches).ok();
            writeln!(f, "  Position szi mismatches: {}", r.szi_mismatches.len()).ok();
            writeln!(f, "  Balance drifts (>$1): {}", r.balance_drifts.len()).ok();
            writeln!(f, "  Leverage mismatches: {}", r.leverage_mismatches.len()).ok();
            writeln!(f, "  Cost basis drifts: {}", r.cost_basis_drifts.len()).ok();
            writeln!(f, "  Raw USD drifts: {}", r.raw_usd_drifts.len()).ok();
            writeln!(f, "  Funding drifts: {}", r.funding_drifts.len()).ok();
            writeln!(f, "  SCL drifts (>$1): {}", r.scl_drifts.len()).ok();
            writeln!(f, "  Missing after replay: {}", r.missing_after_replay.len()).ok();
            writeln!(f, "  Extra after replay: {}", r.extra_after_replay.len()).ok();

            // Write ALL balance drifts sorted by magnitude
            let mut sorted: Vec<_> = r.balance_drifts.iter().collect();
            sorted.sort_by(|a, b| b.diff_usd.abs().partial_cmp(&a.diff_usd.abs()).unwrap_or(std::cmp::Ordering::Equal));
            writeln!(f, "\n  Balance drifts:").ok();
            for d in &sorted {
                let actions = state_a.user_action_counts.get(&d.user).copied().unwrap_or(0);
                writeln!(
                    f,
                    "    {} replay=${:.2} truth=${:.2} diff=${:.2} ({:.4}%) actions={}",
                    d.user,
                    d.replay_balance as f64 / 1e6,
                    d.truth_balance as f64 / 1e6,
                    d.diff_usd,
                    d.pct,
                    actions,
                )
                .ok();
            }

            // Write ALL leverage mismatches
            writeln!(f, "\n  Leverage mismatches:").ok();
            for m in &r.leverage_mismatches {
                writeln!(
                    f,
                    "    {} asset={} coin={}: replay={} truth={}",
                    m.user, m.asset_idx, m.coin, m.replay_lev, m.truth_lev,
                )
                .ok();
            }

            // Write ALL szi mismatches
            if !r.szi_mismatches.is_empty() {
                writeln!(f, "\n  Szi mismatches:").ok();
                for m in &r.szi_mismatches {
                    writeln!(
                        f,
                        "    {} asset={} coin={}: replay={} truth={}",
                        m.user, m.asset_idx, m.coin, m.replay_szi, m.truth_szi,
                    )
                    .ok();
                }
            }

            writeln!(f, "\n  Extra after replay: {}", r.extra_after_replay.len()).ok();
            writeln!(f, "  Missing after replay: {}", r.missing_after_replay.len()).ok();
            writeln!(f).ok();
        }
        println!("Results written to {}", results_path.display());
    }

    if !debug_output.is_empty() {
        let debug_path = run_dir.join("debug_users.txt");
        match std::fs::write(&debug_path, debug_output.as_bytes()) {
            Ok(()) => println!("Debug user output written to {}", debug_path.display()),
            Err(e) => eprintln!("Failed to write debug user output {}: {e}", debug_path.display()),
        }
    }

    // Copy RMP files
    let rmp_a_dest = run_dir.join(format!("{block_a}.rmp"));
    let rmp_b_dest = run_dir.join(format!("{block_b}.rmp"));
    if !rmp_a_dest.exists() {
        println!("Copying {} ...", first.display());
        if let Err(e) = std::fs::copy(first, &rmp_a_dest) {
            eprintln!("  Failed: {e}");
        }
    }
    if !rmp_b_dest.exists() {
        println!("Copying {} ...", second.display());
        if let Err(e) = std::fs::copy(second, &rmp_b_dest) {
            eprintln!("  Failed: {e}");
        }
    }

    // Backup replica_cmds and fill/misc_events files for future replication
    {
        let backup_dir = run_dir.join("data");
        let fills_dir = data_dir.join("node_fills_streaming");
        let replica_dir = home_dir.join("hl/data/replica_cmds");
        let misc_dir = data_dir.join("misc_events_streaming");

        /// Check if a streaming file (fills/misc) overlaps the block range
        /// by reading the block_number from the first and last lines.
        fn file_overlaps_range(path: &std::path::Path, from_block: u64, to_block: u64) -> bool {
            use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
            let Ok(file) = std::fs::File::open(path) else { return false };
            let mut reader = BufReader::new(file);
            let mut first_block = None;
            let mut line = String::new();
            if reader.read_line(&mut line).is_ok() {
                first_block = extract_block_number(&line);
            }
            // Read last 32KB as bytes, convert to lossy string, find last block_number
            let mut last_block = None;
            if let Ok(mut file) = std::fs::File::open(path) {
                let fsize = file.seek(SeekFrom::End(0)).unwrap_or(0);
                let chunk = 32768u64;
                let start = if fsize > chunk { fsize - chunk } else { 0 };
                let _ = file.seek(SeekFrom::Start(start));
                let mut buf = vec![0u8; (fsize - start) as usize];
                let _ = file.read(&mut buf);
                let text = String::from_utf8_lossy(&buf);
                for line in text.lines().rev() {
                    // Skip partial first line from mid-file seek
                    if !line.starts_with('{') {
                        continue;
                    }
                    if let Some(bn) = extract_block_number(line) {
                        last_block = Some(bn);
                        break;
                    }
                }
            }
            let first = first_block.unwrap_or(0);
            let last = last_block.unwrap_or(u64::MAX);
            last > from_block && first <= to_block
        }

        fn extract_block_number(line: &str) -> Option<u64> {
            let needle = "\"block_number\":";
            let pos = line.find(needle)?;
            let start = pos + needle.len();
            let rest = &line[start..];
            let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
            if end == 0 { return None; }
            rest[..end].parse().ok()
        }

        let copy_matching_files = |src_dir: &std::path::Path, dest_subdir: &str, label: &str, check_block_range: bool, filter_by_filename: bool| {
            if !src_dir.exists() {
                return 0u64;
            }
            let dest = backup_dir.join(dest_subdir);
            let mut files = Vec::new();
            fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
                let Ok(entries) = std::fs::read_dir(dir) else { return };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() { collect(&path, out); } else { out.push(path); }
                }
            }
            collect(src_dir, &mut files);
            files.sort();
            let mut copied = 0u64;
            for path in &files {
                // For replica_cmds: filename is the base block number
                if filter_by_filename {
                    if let Some(file_block) = path.file_name()
                        .and_then(|s| s.to_str())
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        if file_block > block_b || file_block + 10_000 <= block_a {
                            continue;
                        }
                    }
                }
                // For fills/misc: check block range overlap via first/last line
                if check_block_range && !file_overlaps_range(path, block_a, block_b) {
                    continue;
                }
                let rel = path.strip_prefix(src_dir).unwrap_or(path);
                let dest_path = dest.join(rel);
                if dest_path.exists() {
                    continue;
                }
                if let Some(parent) = dest_path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                match std::fs::copy(path, &dest_path) {
                    Ok(_) => copied += 1,
                    Err(e) => eprintln!("  Failed to copy {}: {e}", path.display()),
                }
            }
            if copied > 0 {
                println!("Backed up {} {} files to {}", copied, label, dest.display());
            }
            copied
        };

        // Replica_cmds: filter by filename (block number)
        copy_matching_files(&replica_dir, "replica_cmds", "replica_cmds", false, true);
        // Fills and misc: filter by first/last block_number in file content
        copy_matching_files(&fills_dir, "node_fills_streaming", "fills", true, false);
        copy_matching_files(&misc_dir, "misc_events_streaming", "misc_events", true, false);
    }

    // Translate RMP to JSON using hl-node
    let hlnode = expand_tilde_path(&args.hlnode_binary);
    for (rmp_path, block) in [(&rmp_a_dest, block_a), (&rmp_b_dest, block_b)] {
        let json_path = run_dir.join(format!("{block}.json"));
        if json_path.exists() {
            println!("{} already exists, skipping translation", json_path.display());
            continue;
        }
        println!("Translating {} to JSON...", rmp_path.display());
        let output = std::process::Command::new(&hlnode)
            .args(["--chain", "Mainnet", "translate-abci-state"])
            .arg(rmp_path)
            .arg(&json_path)
            .output();
        match output {
            Ok(o) if o.status.success() => {
                println!("  Written to {}", json_path.display());
            }
            Ok(o) => {
                eprintln!("  hl-node failed: {}", String::from_utf8_lossy(&o.stderr));
            }
            Err(e) => {
                eprintln!("  Failed to run {}: {e}", hlnode.display());
            }
        }
    }

    // Step 8: Diagnostic dump for top drift users
    // Collect top drift users across all dexes
    let mut all_drift_users: std::collections::HashSet<String> = std::collections::HashSet::new();
    for r in &results {
        let mut sorted: Vec<_> = r.balance_drifts.iter().collect();
        sorted.sort_by(|a, b| b.diff_usd.abs().partial_cmp(&a.diff_usd.abs()).unwrap_or(std::cmp::Ordering::Equal));
        for d in sorted.iter().take(30) {
            all_drift_users.insert(d.user.clone());
        }
    }

    let diag_path = run_dir.join("diagnostics.txt");
    if let Ok(mut f) = std::fs::File::create(&diag_path) {
        use std::io::Write;

        // For each top drift user, show state in EVERY dex (both replay and truth)
        let mut sorted_users: Vec<_> = all_drift_users.iter().cloned().collect();
        sorted_users.sort();
        for user in &sorted_users {
            writeln!(f, "=== USER: {} ===", user).ok();

            for (di, (replay_dex, truth_dex)) in state_a.dex_states.iter().zip(state_b.dex_states.iter()).enumerate() {
                let r_user = replay_dex.users.get(user.as_str());
                let t_user = truth_dex.users.get(user.as_str());
                let r_partial = replay_dex.users_without_positions.get(user.as_str());
                let t_partial = truth_dex.users_without_positions.get(user.as_str());

                if r_user.is_none() && t_user.is_none() && r_partial.is_none() && t_partial.is_none() {
                    continue;
                }

                writeln!(f, "  --- dex[{}] pdi={} ---", di, replay_dex.pdi).ok();

                if let Some(ru) = r_user {
                    writeln!(
                        f,
                        "    REPLAY: usdc=${:.2} scl={} unified={}",
                        ru.usdc_balance as f64 / 1e6,
                        ru.spot_collateral,
                        ru.account_mode.is_shared_usdc()
                    )
                    .ok();
                    let mut pos_keys: Vec<_> = ru.positions.keys().collect();
                    pos_keys.sort();
                    for &k in &pos_keys {
                        let p = &ru.positions[k];
                        let coin = replay_dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                        writeln!(
                            f,
                            "      pos[{}] {}: szi={} cb={} lev={:?} funding={}",
                            k, coin, p.szi, p.cost_basis, p.leverage, p.outstanding_funding
                        )
                        .ok();
                    }
                } else if let Some(rp) = r_partial {
                    writeln!(
                        f,
                        "    REPLAY (partial): usdc=${:.2} scl={} unified={}",
                        rp.usdc_balance as f64 / 1e6,
                        rp.spot_collateral,
                        rp.account_mode.is_shared_usdc()
                    )
                    .ok();
                } else {
                    writeln!(f, "    REPLAY: NOT PRESENT").ok();
                }

                if let Some(tu) = t_user {
                    writeln!(
                        f,
                        "    TRUTH:  usdc=${:.2} scl={} unified={}",
                        tu.usdc_balance as f64 / 1e6,
                        tu.spot_collateral,
                        tu.account_mode.is_shared_usdc()
                    )
                    .ok();
                    let mut pos_keys: Vec<_> = tu.positions.keys().collect();
                    pos_keys.sort();
                    for &k in &pos_keys {
                        let p = &tu.positions[k];
                        let coin = truth_dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                        writeln!(
                            f,
                            "      pos[{}] {}: szi={} cb={} lev={:?} funding={}",
                            k, coin, p.szi, p.cost_basis, p.leverage, p.outstanding_funding
                        )
                        .ok();
                    }
                } else if let Some(tp) = t_partial {
                    writeln!(
                        f,
                        "    TRUTH (partial):  usdc=${:.2} scl={} unified={}",
                        tp.usdc_balance as f64 / 1e6,
                        tp.spot_collateral,
                        tp.account_mode.is_shared_usdc()
                    )
                    .ok();
                } else {
                    writeln!(f, "    TRUTH:  NOT PRESENT").ok();
                }
            }
            writeln!(f).ok();
        }
        println!("Diagnostics written to {}", diag_path.display());
    }

    // Write per-user traces for drifting users
    if args.trace_drifters {
        if let Some(ref event_log) = state_a.event_log {
            let traces_dir = run_dir.join("traces");
            std::fs::create_dir_all(&traces_dir).ok();

            // Collect all drifting users
            let mut drifting_users: std::collections::HashSet<String> = std::collections::HashSet::new();
            for r in &results {
                for d in &r.balance_drifts {
                    drifting_users.insert(d.user.clone());
                }
            }

            let mut written = 0usize;
            for user in &drifting_users {
                let mut trace = String::new();

                // Snapshot A state
                trace.push_str(&format!("=== SNAPSHOT A (block {}) ===\n", block_a));
                write_user_state_to_string(&state_a, user, &mut trace);

                // Snapshot B state
                trace.push_str(&format!("\n=== SNAPSHOT B (block {}) ===\n", block_b));
                write_user_state_to_string(&state_b, user, &mut trace);

                // Events
                if let Some(events) = event_log.get(user) {
                    trace.push_str(&format!("\n=== EVENTS ({} total) ===\n", events.len()));
                    for (bn, desc) in events {
                        trace.push_str(&format!("[block={}] {}\n", bn, desc));
                    }
                } else {
                    trace.push_str("\n=== EVENTS (0 total) ===\n");
                }

                // Write to file
                let filename = format!("{}.txt", user);
                if std::fs::write(traces_dir.join(&filename), trace.as_bytes()).is_ok() {
                    written += 1;
                }
            }
            println!("{} user traces written to {}", written, traces_dir.display());
        }
    }

    println!("\n=== Analysis artifacts in {} ===", run_dir.display());
    println!("=== Done ===");
}

fn write_user_state_to_string(state: &LiquidationState, user: &str, out: &mut String) {
    let mut found = false;
    for (di, dex) in state.dex_states.iter().enumerate() {
        if let Some(us) = dex.users.get(user) {
            found = true;
            out.push_str(&format!("  dex[{}] pdi={} (users):\n", di, dex.pdi));
            out.push_str(&format!("    usdc=${:.2} scl={} mode={:?}\n", us.usdc_balance as f64 / 1e6, us.spot_collateral, us.account_mode));
            let mut keys: Vec<_> = us.positions.keys().collect();
            keys.sort();
            for &k in &keys {
                let p = &us.positions[k];
                let coin = dex.universe.get(*k as usize).map(|a| a.name.as_str()).unwrap_or("?");
                out.push_str(&format!("    pos[{}] {}: szi={} cb={} lev={:?} funding={}\n", k, coin, p.szi, p.cost_basis, p.leverage, p.outstanding_funding));
            }
        }
        if let Some(partial) = dex.users_without_positions.get(user) {
            found = true;
            out.push_str(&format!("  dex[{}] pdi={} (partial):\n", di, dex.pdi));
            out.push_str(&format!("    usdc=${:.2} scl={} mode={:?}\n", partial.usdc_balance as f64 / 1e6, partial.spot_collateral, partial.account_mode));
        }
    }
    if !found {
        out.push_str("  (user not found)\n");
    }
}

fn print_state_summary(label: &str, state: &LiquidationState) {
    println!("  {}:", label);
    for (i, dex) in state.dex_states.iter().enumerate() {
        if dex.users.is_empty() {
            continue;
        }
        let total_positions: usize = dex.users.values().map(|u| u.positions.len()).sum();
        println!(
            "    dex[{}] pdi={}: {} users, {} positions, {} assets",
            i,
            dex.pdi,
            dex.users.len(),
            total_positions,
            dex.universe.len(),
        );
    }
    if !state.vault_states.is_empty() {
        println!("    {} vaults parsed", state.vault_states.len());
    }
    if !state.portfolio_margin_users.is_empty() {
        println!("    {} portfolio margin users", state.portfolio_margin_users.len());
    }
    if !state.mark_prices.is_empty() {
        println!("    {} mark prices indexed", state.mark_prices.len());
    }
}

fn select_snapshot_pair(rmp_files: &[PathBuf], from_block: Option<u64>) -> Result<(usize, usize), String> {
    match from_block {
        None => Ok((rmp_files.len() - 2, rmp_files.len() - 1)),
        Some(block) => {
            let Some(first_idx) = rmp_files.iter().position(|path| block_height_from_rmp(path) == Some(block)) else {
                return Err(format!("Snapshot block {block} not found in periodic_abci_states"));
            };

            if first_idx + 1 >= rmp_files.len() {
                return Err(format!(
                    "Snapshot block {block} is the last available snapshot and has no following snapshot to compare against"
                ));
            }

            Ok((first_idx, first_idx + 1))
        }
    }
}

fn expand_tilde_path(path: &str) -> PathBuf {
    if path == "~" {
        return dirs::home_dir().unwrap_or_else(|| PathBuf::from(path));
    }

    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    }

    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::{expand_tilde_path, select_snapshot_pair};
    use std::path::PathBuf;

    fn paths(blocks: &[u64]) -> Vec<PathBuf> {
        blocks.iter().map(|block| PathBuf::from(format!("/tmp/{block}.rmp"))).collect()
    }

    #[test]
    fn defaults_to_last_two_snapshots() {
        let files = paths(&[100, 200, 300]);
        assert_eq!(select_snapshot_pair(&files, None).unwrap(), (1, 2));
    }

    #[test]
    fn selects_requested_start_snapshot_and_next_snapshot() {
        let files = paths(&[100, 200, 300]);
        assert_eq!(select_snapshot_pair(&files, Some(100)).unwrap(), (0, 1));
        assert_eq!(select_snapshot_pair(&files, Some(200)).unwrap(), (1, 2));
    }

    #[test]
    fn rejects_missing_or_terminal_snapshot() {
        let files = paths(&[100, 200, 300]);
        assert!(select_snapshot_pair(&files, Some(150)).is_err());
        assert!(select_snapshot_pair(&files, Some(300)).is_err());
    }

    #[test]
    fn expands_tilde_prefixed_paths() {
        if let Some(home) = dirs::home_dir() {
            assert_eq!(expand_tilde_path("~"), home);
            assert_eq!(expand_tilde_path("~/hl-node"), home.join("hl-node"));
        }
        assert_eq!(expand_tilde_path("hl-node"), PathBuf::from("hl-node"));
    }
}
