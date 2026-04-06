pub mod api_leverage;
pub mod misc_events;
pub mod replica;
pub mod rmp_streaming;
pub mod state;

use crate::prelude::*;
use crate::types::node_data::{Batch, NodeDataFill, NodeDataOrderStatus};
use log::{info, warn};
use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

// ── Core types ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountMode {
    /// Separate perp and spot balances, separate DEX balances.
    Standard,
    /// Single balance per asset, shared across all dexes via scl.
    Unified,
    /// USDC defaults to perps, other collateral to spot. Cross-dex USDC sharing.
    DexAbstraction,
    /// Like standard but with auto-borrowing against collateral.
    PortfolioMargin,
}

impl AccountMode {
    /// Whether this mode shares USDC across dexes (for balance comparison).
    pub fn is_shared_usdc(&self) -> bool {
        matches!(self, AccountMode::Unified | AccountMode::DexAbstraction)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarginMode {
    /// Cross and Isolated both allowed, default is Cross.
    Normal,
    /// Only Isolated allowed (no cross margin).
    NoCross,
    /// Only Isolated, margin cannot be removed.
    StrictIsolated,
}

#[derive(Debug, Clone)]
pub struct AssetMeta {
    pub name: String,
    pub sz_decimals: u32,
    pub margin_table_id: u32,
    pub margin_mode: MarginMode,
}

#[derive(Debug, Clone)]
pub struct MarginTier {
    pub lower_bound: i64,
    pub max_leverage: u32,
    pub maintenance_deduction: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Position {
    pub szi: i64,
    pub cost_basis: i64,
    pub leverage: Leverage,
    pub margin_table_id: u32,
    /// Outstanding funding not yet settled into usdc_balance.
    /// Settles hourly when funding distributes.
    pub outstanding_funding: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Leverage {
    Cross(u32),
    Isolated { leverage: u32, raw_usd: i64 },
}

#[derive(Debug, Clone)]
pub struct UserState {
    pub usdc_balance: i64,
    pub spot_collateral: i64,
    pub spot_collateral_decimals: u32,
    pub account_mode: AccountMode,
    pub positions: HashMap<u32, Position>,
    /// Leverage settings per asset, persisted even when positions are closed.
    /// When a fill opens a new position, this is used instead of a default.
    pub leverage_settings: HashMap<u32, Leverage>,
}

#[derive(Debug, Clone)]
pub struct DexState {
    pub pdi: u32,
    pub universe: Vec<AssetMeta>,
    pub margin_tables: HashMap<u32, Vec<MarginTier>>,
    pub oracle_prices: Vec<i64>,
    pub users: HashMap<String, UserState>,
    pub collateral_token: u32,
    /// Partial state for users without positions at snapshot time.
    /// When a fill or replica action touches such a user, their state is
    /// initialized from here instead of defaults.
    pub users_without_positions: HashMap<String, UserStatePartial>,
}

#[derive(Debug, Clone)]
pub struct UserStatePartial {
    pub usdc_balance: i64,
    pub spot_collateral: i64,
    pub spot_collateral_decimals: u32,
    pub account_mode: AccountMode,
    pub leverage_settings: HashMap<u32, Leverage>,
}

impl UserStatePartial {
    pub fn into_user_state(self) -> UserState {
        UserState {
            usdc_balance: self.usdc_balance,
            spot_collateral: self.spot_collateral,
            spot_collateral_decimals: self.spot_collateral_decimals,
            account_mode: self.account_mode,
            positions: HashMap::new(),
            leverage_settings: self.leverage_settings,
        }
    }
}

pub struct LiquidationState {
    pub dex_states: Vec<DexState>,
    pub coin_to_dex_asset: HashMap<String, (usize, usize)>,
    /// Track processed withdrawal nonces to avoid double-counting
    /// (multiple validators vote on the same withdrawal).
    pub processed_withdrawal_nonces: std::collections::HashSet<i64>,
    /// Users to trace for debugging. All state changes are logged.
    pub debug_users: std::collections::HashSet<String>,
    /// Users who opened positions with default leverage (no snapshot data).
    /// Set of (user_addr, dex_idx, asset_idx) that need API-based leverage fix.
    pub positions_needing_leverage_fix: Vec<(String, usize, u32)>,
    /// Per-user event log for tracing. Enabled with `trace_all = true`.
    /// Maps user_addr → list of (block_number, event_description).
    pub event_log: Option<HashMap<String, Vec<(u64, String)>>>,
    /// Shared unified balance per (user, collateral_token).
    /// For unified mode users, spot_collateral is shared across all dexes
    /// with the same collateral token. All mutations go through this map.
    /// Stored in 8-decimal (weiDecimals) units.
    pub unified_balances: HashMap<(String, u32), i64>,
    /// Per-user action counter: fills + replica actions applied.
    pub user_action_counts: HashMap<String, u32>,
    /// Per-user borrow/supply state from locus.blp, keyed by (user, token_id).
    /// Amounts in 8-decimal (weiDecimals) units.
    pub borrow_lend_states: HashMap<(String, u32), BorrowLendState>,
    /// Users in portfolio margin mode ("a"="p").
    pub portfolio_margin_users: std::collections::HashSet<String>,
    /// Vault state: vault_addr → per-user ownership fractions.
    pub vault_states: HashMap<String, VaultState>,
    /// Per-asset mark prices (in USD string format) from SetGlobalAction.
    /// Key = (dex_idx, asset_idx), value = mark_price as f64 in USD.
    pub mark_prices: HashMap<(usize, u32), f64>,
    /// (Reserved for future per-order tracking if needed.)
    pub order_holds: HashMap<u64, (String, i64)>,
    /// Spot pair metadata: coin name (e.g. "@150") → (base_token_id, quote_token_id, sz_decimals).
    /// Used to process spot fills that change SCL balances.
    pub spot_pairs: HashMap<String, (u32, u32, u32)>,
    /// Dex name → pdi mapping (e.g. "xyz" → 1, "cash" → 7).
    /// Populated from coin name prefixes in the snapshot.
    pub dex_name_to_pdi: HashMap<String, u32>,
}

/// Per-vault ownership data parsed from locus.vlt.
#[derive(Debug, Clone, Default)]
pub struct VaultState {
    /// Map of user_addr → ownership fraction (0.0 to 1.0).
    pub user_ownership: HashMap<String, f64>,
}

#[derive(Debug, Clone, Default)]
pub struct BorrowLendState {
    pub borrowed: i64,
    pub borrow_shares: i64,
    pub supplied: i64,
    pub supply_shares: i64,
}

impl LiquidationState {
    pub fn load_from_rmp(path: &Path) -> Result<Self> {
        rmp_streaming::load_from_rmp(path)
    }

    /// Mutate the unified balance for a user. `delta` is in 6-decimal micro-USD.
    /// Converts to 8-decimal and updates the shared balance.
    /// Returns the new balance (8 decimals).
    pub fn unified_balance_add(&mut self, user: &str, collateral_token: u32, delta_micro_usd: i64) -> i64 {
        let key = (user.to_lowercase(), collateral_token);
        let bal = self.unified_balances.entry(key).or_default();
        *bal += delta_micro_usd * 100; // 6→8 decimal conversion
        *bal
    }

    /// Get the unified balance for a user (8 decimals).
    pub fn unified_balance_get(&self, user: &str, collateral_token: u32) -> i64 {
        self.unified_balances.get(&(user.to_lowercase(), collateral_token)).copied().unwrap_or(0)
    }

    /// For portfolio margin users: auto-repay borrows when receiving funds,
    /// auto-borrow when spending beyond available balance.
    /// `delta_micro` is in 6-decimal units. Positive = receiving, negative = spending.
    pub fn pm_auto_borrow_repay(&mut self, user: &str, collateral_token: u32, delta_micro: i64) {
        if !self.portfolio_margin_users.contains(&user.to_lowercase()) {
            return;
        }
        let key = (user.to_lowercase(), collateral_token);
        let bls = self.borrow_lend_states.entry(key).or_default();
        let delta_8dec = delta_micro * 100; // 6→8 decimal

        if delta_micro > 0 && bls.borrowed > 0 {
            // Receiving funds — auto-repay borrow first.
            // Repay amount is deducted from usdc_balance (the funds go to repay debt).
            let repay = delta_8dec.min(bls.borrowed);
            bls.borrowed -= repay;
            let repay_micro = repay / 100; // 8→6 dec
            // Deduct repayment from the user's usdc_balance on the first dex where they exist
            for dex in &mut self.dex_states {
                if let Some(user_state) = dex.users.get_mut(&user.to_lowercase()) {
                    user_state.usdc_balance -= repay_micro;
                    break;
                }
            }
            if self.debug_users.contains(&user.to_lowercase()) {
                eprintln!(
                    "  → PM AUTO-REPAY: ${:.2} repaid from usdc, borrow remaining=${:.2}",
                    repay_micro as f64 / 1e6, bls.borrowed as f64 / 1e8
                );
            }
        } else if delta_micro < 0 {
            // Spending funds — check if usdc went negative, auto-borrow the deficit
            let mut usdc_total: i64 = 0;
            for dex in &self.dex_states {
                if let Some(user_state) = dex.users.get(&user.to_lowercase()) {
                    usdc_total += user_state.usdc_balance;
                }
            }
            if usdc_total < 0 {
                let borrow_amount = (-usdc_total * 100).min(-delta_8dec); // 6→8 dec
                bls.borrowed += borrow_amount;
                let borrow_micro = borrow_amount / 100;
                // Add borrowed funds back to usdc_balance
                for dex in &mut self.dex_states {
                    if let Some(user_state) = dex.users.get_mut(&user.to_lowercase()) {
                        user_state.usdc_balance += borrow_micro;
                        break;
                    }
                }
                if self.debug_users.contains(&user.to_lowercase()) {
                    eprintln!(
                        "  → PM AUTO-BORROW: ${:.2} borrowed to usdc, total borrow=${:.2}",
                        borrow_micro as f64 / 1e6, bls.borrowed as f64 / 1e8
                    );
                }
            }
        }
    }

    /// Compute vault withdrawal amount when `usd=0` (withdraw all).
    /// Returns the user's share of the vault's usdc_balance in 6-decimal micro USD.
    pub fn compute_vault_withdrawal(&self, vault_addr: &str, user_addr: &str) -> i64 {
        let vault = vault_addr.to_lowercase();
        let user = user_addr.to_lowercase();

        // Look up ownership fraction
        let fraction = self.vault_states.get(&vault)
            .and_then(|vs| vs.user_ownership.get(&user))
            .copied()
            .unwrap_or(0.0);
        if fraction == 0.0 {
            return 0;
        }

        // TODO: Full vault equity requires oracle prices (format TBD).
        // For now, use usdc_balance only — this underestimates for vaults with positions.
        let mut vault_usdc: i64 = 0;
        for dex in &self.dex_states {
            if let Some(vs) = dex.users.get(&vault) {
                vault_usdc += vs.usdc_balance;
            }
        }

        let withdrawal = (vault_usdc as f64 * fraction).round() as i64;

        if self.debug_users.contains(&vault) || self.debug_users.contains(&user) {
            eprintln!(
                "[DEBUG vault_withdrawal] vault={} user={} fraction={:.6} vault_usdc=${:.2} withdrawal=${:.2}",
                vault, user, fraction, vault_usdc as f64 / 1e6, withdrawal as f64 / 1e6
            );
        }

        withdrawal
    }

    /// Enable per-user event tracing.
    pub fn enable_event_log(&mut self) {
        self.event_log = Some(HashMap::new());
    }

    /// Log an event for a user. No-op if event_log is disabled.
    pub fn log_event(&mut self, user: &str, block: u64, desc: String) {
        if let Some(ref mut log) = self.event_log {
            log.entry(user.to_lowercase()).or_default().push((block, desc));
        }
    }

    /// Settle all outstanding funding into usdc_balance for all users.
    /// Called when a funding distribution event occurs (hourly boundary).
    /// For cross positions: outstanding → usdc_balance.
    /// For isolated positions: outstanding → raw_usd.
    pub fn settle_funding(&mut self) {
        for dex in &mut self.dex_states {
            for (addr, user_state) in &mut dex.users {
                let mut total_settled = 0i64;
                for (_, pos) in &mut user_state.positions {
                    let funding = pos.outstanding_funding;
                    if funding != 0 {
                        match &mut pos.leverage {
                            Leverage::Isolated { raw_usd, .. } => {
                                *raw_usd += funding;
                            }
                            Leverage::Cross(_) => {
                                total_settled += funding;
                            }
                        }
                        pos.outstanding_funding = 0;
                    }
                }
                if total_settled != 0 {
                    if self.debug_users.contains(addr) {
                        eprintln!("[DEBUG settle_funding] user={} settled=${:.2}", addr, total_settled as f64 / 1e6);
                    }
                    user_state.usdc_balance += total_settled;
                }
            }
        }
    }
}

pub fn extract_raw_debug_users_from_rmp(
    path: &Path,
    debug_users: &std::collections::HashSet<String>,
) -> Result<String> {
    rmp_streaming::extract_raw_debug_users_from_rmp(path, debug_users)
}

pub fn extract_raw_debug_misc_events(
    data_dir: &Path,
    debug_users: &std::collections::HashSet<String>,
    from_block: u64,
    to_block: u64,
) -> Result<String> {
    misc_events::extract_raw_debug_misc_events(data_dir, debug_users, from_block, to_block)
}

// ── RMP file discovery ──────────────────────────────────────────────────

/// Extract block height from an RMP filename like "932460000.rmp"
pub fn block_height_from_rmp(path: &Path) -> Option<u64> {
    path.file_stem().and_then(|s| s.to_str()).and_then(|s| s.parse::<u64>().ok())
}

/// Find all .rmp files sorted by block height.
pub fn find_all_rmp_files(base_dir: &Path) -> Result<Vec<PathBuf>> {
    let abci_dir = base_dir.join("hl/data/periodic_abci_states");
    if !abci_dir.exists() {
        return Err(format!("ABCI states directory not found: {}", abci_dir.display()).into());
    }

    let mut all_rmp: Vec<PathBuf> = Vec::new();

    let mut date_dirs: Vec<_> = fs::read_dir(&abci_dir)?.filter_map(|e| e.ok()).filter(|e| e.path().is_dir()).collect();
    date_dirs.sort_by_key(|e| e.file_name());

    for date_dir in &date_dirs {
        let mut rmp_files: Vec<_> = fs::read_dir(date_dir.path())?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "rmp"))
            .map(|e| e.path())
            .collect();
        rmp_files.sort();
        all_rmp.extend(rmp_files);
    }

    Ok(all_rmp)
}

// ── Fills replay (from streaming hourly files) ──────────────────────────

/// Replay fills from node_fills_streaming/ for blocks in `[from_block, to_block)`.
pub fn replay_fills_from_streaming(
    data_dir: &Path,
    state: &mut LiquidationState,
    from_block: u64,
    to_block: u64,
) -> u64 {
    let fills_dir = data_dir.join("node_fills_streaming");
    if !fills_dir.exists() {
        info!("Fills streaming directory does not exist: {}. Skipping.", fills_dir.display());
        return 0;
    }

    let mut files = Vec::new();
    collect_files_recursive(&fills_dir, &mut files);
    files.sort();

    let mut total_fills: u64 = 0;
    let mut total_batches: u64 = 0;

    for path in &files {
        let file = match fs::File::open(path) {
            Ok(f) => f,
            Err(e) => {
                warn!("Failed to open fill file {}: {e}", path.display());
                continue;
            }
        };
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    warn!("Failed to read line from {}: {e}", path.display());
                    break;
                }
            };
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let batch: Batch<NodeDataFill> = match serde_json::from_str(trimmed) {
                Ok(b) => b,
                Err(e) => {
                    warn!("Fill batch parse error in {}: {e}", path.display());
                    continue;
                }
            };
            let block_num = batch.block_number();
            // Snapshot at block N includes state after block N is processed.
            // So replay range is (from_block, to_block] — exclusive start, inclusive end.
            if block_num <= from_block || block_num > to_block {
                continue;
            }
            let fills = batch.events();
            for fill in &fills {
                let user_addr = format!("{}", fill.0).to_lowercase();
                state.apply_fill(&user_addr, &fill.1);
            }
            total_fills += fills.len() as u64;
            total_batches += 1;
        }
    }

    info!("Replayed {total_fills} fills in {total_batches} batches (blocks {from_block}..{to_block})");
    total_fills
}

// ── Interleaved replay ──────────────────────────────────────────────────

/// Replay fills and replica_cmds interleaved by block number.
/// This ensures leverage changes from replica are applied before fills
/// at the same or later blocks.
pub fn replay_interleaved(
    data_dir: &Path,
    home_dir: &Path,
    state: &mut LiquidationState,
    from_block: u64,
    to_block: u64,
) -> (u64, u64) {
    // Collect fill events keyed by block number
    let fills_dir = data_dir.join("node_fills_streaming");
    let mut fill_batches: Vec<(u64, String)> = Vec::new(); // (block, json_line)

    if fills_dir.exists() {
        let mut files = Vec::new();
        collect_files_recursive(&fills_dir, &mut files);
        files.sort();
        for path in &files {
            let file = match fs::File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    warn!("Failed to open fill file {}: {e}", path.display());
                    continue;
                }
            };
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let Ok(line) = line else { break };
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                // Quick extract of block_number without full parse
                if let Ok(batch) = serde_json::from_str::<Batch<NodeDataFill>>(trimmed) {
                    let bn = batch.block_number();
                    if bn > from_block && bn <= to_block {
                        fill_batches.push((bn, line));
                    }
                }
            }
        }
    }

    // Collect replica blocks keyed by block number (from filename)
    let replica_dir = home_dir.join("hl/data/replica_cmds");
    let mut replica_blocks: Vec<(u64, String)> = Vec::new(); // (block, json_line)

    if replica_dir.exists() {
        let mut files = Vec::new();
        collect_files_recursive(&replica_dir, &mut files);
        files.sort();
        for path in &files {
            let file_block: Option<u64> = path.file_name().and_then(|s| s.to_str()).and_then(|s| s.parse().ok());
            if let Some(fb) = file_block {
                // Replica file named N contains blocks from N onwards (~N+10000).
                if fb > to_block || fb + 10_000 <= from_block {
                    continue;
                }
            }
            let file = match fs::File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    warn!("Failed to open replica file {}: {e}", path.display());
                    continue;
                }
            };
            let reader = BufReader::new(file);
            let base_block = file_block.unwrap_or(0);
            for (line_idx, line) in reader.lines().enumerate() {
                let Ok(line) = line else { break };
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                // Each line in the file is one block: file_block + line_index
                let block_num = base_block + line_idx as u64;
                if block_num > from_block && block_num <= to_block {
                    replica_blocks.push((block_num, line));
                }
            }
        }
    }

    // Collect misc_events keyed by block number
    let misc_dir = data_dir.join("misc_events_streaming");
    let mut misc_batches: Vec<(u64, String)> = Vec::new();

    if misc_dir.exists() {
        let mut files = Vec::new();
        collect_files_recursive(&misc_dir, &mut files);
        files.sort();
        for path in &files {
            let file = match fs::File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    warn!("Failed to open misc_events file {}: {e}", path.display());
                    continue;
                }
            };
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let Ok(line) = line else { break };
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                // Quick extract of block_number
                if let Ok(batch) = serde_json::from_str::<misc_events::MiscEventBatch>(trimmed) {
                    if batch.block_number > from_block && batch.block_number <= to_block {
                        misc_batches.push((batch.block_number, line));
                    }
                }
            }
        }
    }

    // // Collect order status events keyed by block number.
    // // These files are very large (10s of GB), so we only read the specific
    // // hourly file that covers our block range, using the same directory layout
    // // as fills (node_order_statuses_streaming/hourly/YYYYMMDD/HH).
    // let mut order_status_batches: Vec<(u64, String)> = Vec::new();
    // {
    //     // Determine which hourly file(s) to read by looking at the fills directory
    //     // for available date/hour paths, then using the same paths for order statuses.
    //     // Order statuses live alongside fills. Try multiple base dirs:
    //     // 1. backed-up data_dir, 2. home_dir/hl/data, 3. real home ~/hl/data
    //     let candidate_dirs = [
    //         data_dir.join("node_order_statuses_streaming"),
    //         home_dir.join("hl/data/node_order_statuses_streaming"),
    //         dirs::home_dir()
    //             .unwrap_or_default()
    //             .join("hl/data/node_order_statuses_streaming"),
    //     ];
    //     let os_base_dir = candidate_dirs
    //         .iter()
    //         .find(|d| d.exists())
    //         .cloned();

    //     // Discover the specific hourly file(s) to read by matching fills paths
    //     let mut os_files = Vec::new();
    //     if let Some(ref os_base) = os_base_dir {
    //         // Find which hourly files the fills came from
    //         let fills_dir = data_dir.join("node_fills_streaming");
    //         let mut fill_files = Vec::new();
    //         if fills_dir.exists() {
    //             collect_files_recursive(&fills_dir, &mut fill_files);
    //         }
    //         for fill_path in &fill_files {
    //             // Extract the relative path after "node_fills_streaming/"
    //             let fill_str = fill_path.to_string_lossy();
    //             if let Some(pos) = fill_str.find("node_fills_streaming/") {
    //                 let rel = &fill_str[pos + "node_fills_streaming/".len()..];
    //                 let os_path = os_base.join(rel);
    //                 if os_path.exists() {
    //                     os_files.push(os_path);
    //                 }
    //             }
    //         }
    //     }

    //     for path in &os_files {
    //         let file = match fs::File::open(path) {
    //             Ok(f) => f,
    //             Err(e) => {
    //                 warn!("Failed to open order_status file {}: {e}", path.display());
    //                 continue;
    //             }
    //         };
    //         info!("Reading order statuses from {}", path.display());
    //         let reader = BufReader::new(file);
    //         for line in reader.lines() {
    //             let Ok(line) = line else { break };
    //             let trimmed = line.trim();
    //             if trimmed.is_empty() {
    //                 continue;
    //             }
    //             // Quick pre-filter: extract block_number with string search
    //             if let Some(pos) = trimmed.find("\"block_number\":") {
    //                 let num_start = pos + 15;
    //                 let num_end = trimmed[num_start..]
    //                     .find(|c: char| !c.is_ascii_digit())
    //                     .map(|i| num_start + i)
    //                     .unwrap_or(trimmed.len());
    //                 if let Ok(bn) = trimmed[num_start..num_end].parse::<u64>() {
    //                     if bn <= from_block || bn > to_block {
    //                         continue;
    //                     }
    //                 } else {
    //                     continue;
    //                 }
    //             } else {
    //                 continue;
    //             }
    //             if let Ok(batch) = serde_json::from_str::<Batch<NodeDataOrderStatus>>(trimmed) {
    //                 let bn = batch.block_number();
    //                 if bn > from_block && bn <= to_block {
    //                     order_status_batches.push((bn, line));
    //                 }
    //             }
    //         }
    //     }
    // }
    // info!(
    //     "Collected {} order status batches",
    //     order_status_batches.len()
    // );

    // Collect HIP-3 oracle updates for mark prices
    let hip3_local = data_dir.join("hip3_oracle_updates_streaming");
    let hip3_home = home_dir.join("hl/data/hip3_oracle_updates_streaming");
    let hip3_real = dirs::home_dir().unwrap_or_default().join("hl/data/hip3_oracle_updates_streaming");
    let hip3_oracle_dir = [hip3_local, hip3_home, hip3_real]
        .into_iter()
        .find(|d| d.exists())
        .unwrap_or_default();
    let mut hip3_oracle_batches: Vec<(u64, String)> = Vec::new();
    if hip3_oracle_dir.exists() {
        // Use same hourly file matching as fills
        let fills_dir = data_dir.join("node_fills_streaming");
        let mut fill_files = Vec::new();
        if fills_dir.exists() {
            collect_files_recursive(&fills_dir, &mut fill_files);
        }
        for fill_path in &fill_files {
            let fill_str = fill_path.to_string_lossy();
            if let Some(pos) = fill_str.find("node_fills_streaming/") {
                let rel = &fill_str[pos + "node_fills_streaming/".len()..];
                let oracle_path = hip3_oracle_dir.join(rel);
                if oracle_path.exists() {
                    let file = match fs::File::open(&oracle_path) {
                        Ok(f) => f,
                        Err(_) => continue,
                    };
                    info!("Reading HIP-3 oracle updates from {}", oracle_path.display());
                    let reader = BufReader::new(file);
                    for line in reader.lines() {
                        let Ok(line) = line else { break };
                        let trimmed = line.trim();
                        if trimmed.is_empty() { continue; }
                        if let Some(pos) = trimmed.find("\"block_number\":") {
                            let num_start = pos + 15;
                            let num_end = trimmed[num_start..]
                                .find(|c: char| !c.is_ascii_digit())
                                .map(|i| num_start + i)
                                .unwrap_or(trimmed.len());
                            if let Ok(bn) = trimmed[num_start..num_end].parse::<u64>() {
                                if bn > from_block && bn <= to_block {
                                    hip3_oracle_batches.push((bn, line));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if !hip3_oracle_batches.is_empty() {
        info!("Collected {} HIP-3 oracle batches", hip3_oracle_batches.len());
    }

    // Merge and sort by block number. Order within same block:
    // 0. HIP-3 oracle updates (mark prices — before everything so margin uses correct price)
    // 1. Replica (leverage changes, transfers)
    // 2. OrderStatus (margin hold changes — before fills so holds are set up)
    // 3. MiscEvents (funding, liquidations)
    // 4. Fills (position changes)
    #[derive(PartialEq, Eq, PartialOrd, Ord)]
    enum EventType {
        Hip3Oracle = 0,
        Replica = 1,
        OrderStatus = 2,
        MiscEvent = 3,
        Fill = 4,
    }
    let mut events: Vec<(u64, EventType, String)> = Vec::new();
    for (bn, line) in fill_batches {
        events.push((bn, EventType::Fill, line));
    }
    for (bn, line) in replica_blocks {
        events.push((bn, EventType::Replica, line));
    }
    for (bn, line) in misc_batches {
        events.push((bn, EventType::MiscEvent, line));
    }
    for (bn, line) in hip3_oracle_batches {
        events.push((bn, EventType::Hip3Oracle, line));
    }
    // for (bn, line) in order_status_batches {
    //     events.push((bn, EventType::OrderStatus, line));
    // }
    events.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    let mut total_fills: u64 = 0;
    let mut total_replica: u64 = 0;
    let mut total_misc: u64 = 0;
    let mut total_order_statuses: u64 = 0;
    let mut total_hip3_oracle: u64 = 0;

    for (block_num, event_type, line) in &events {
        match event_type {
            EventType::Hip3Oracle => {
                // Parse HIP-3 oracle updates to set mark prices.
                // Format: {"block_number":N, "events":[{"oracle_pxs":{"coin_to_mark_px":[["coin",{"px":"123.45",...}],...]}}]}
                if let Ok(batch) = serde_json::from_str::<serde_json::Value>(line) {
                    if let Some(events_arr) = batch.get("events").and_then(|e| e.as_array()) {
                        for ev in events_arr {
                            if let Some(pairs) = ev.pointer("/oracle_pxs/coin_to_mark_px").and_then(|v| v.as_array()) {
                                for pair in pairs {
                                    if let Some(arr) = pair.as_array() {
                                        if arr.len() >= 2 {
                                            let coin = arr[0].as_str().unwrap_or("");
                                            let px_str = arr[1].get("px").and_then(|p| p.as_str()).unwrap_or("0");
                                            let px: f64 = px_str.parse().unwrap_or(0.0);
                                            if px > 0.0 {
                                                if let Some(&(dex_idx, asset_idx)) = state.coin_to_dex_asset.get(coin) {
                                                    state.mark_prices.insert((dex_idx, asset_idx as u32), px);
                                                    total_hip3_oracle += 1;
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
            EventType::OrderStatus => {
                if let Ok(batch) = serde_json::from_str::<Batch<NodeDataOrderStatus>>(line) {
                    for os in batch.events() {
                        let user_addr = format!("{}", os.user).to_lowercase();
                        let is_open = os.status == "open";
                        let is_cancel = os.status == "canceled";
                        if !is_open && !is_cancel {
                            continue;
                        }
                        let limit_px: f64 = os.order.limit_px.parse().unwrap_or(0.0);
                        let sz: f64 = os.order.sz.parse().unwrap_or(0.0);
                        let is_ioc = os.order.tif.as_deref() == Some("Ioc");
                        state.apply_order_status(
                            &user_addr,
                            &os.order.coin,
                            os.order.side == crate::order_book::types::Side::Bid,
                            limit_px,
                            sz,
                            os.order.oid,
                            is_open,
                            os.order.is_trigger,
                            is_ioc,
                        );
                        total_order_statuses += 1;
                    }
                }
            }
            EventType::Fill => {
                if let Ok(batch) = serde_json::from_str::<Batch<NodeDataFill>>(line) {
                    let bn = batch.block_number();
                    let fills = batch.events();
                    for fill in &fills {
                        let user_addr = format!("{}", fill.0).to_lowercase();
                        // Log before applying
                        if state.event_log.is_some() {
                            let f = &fill.1;
                            let side = &f.side;
                            let desc = format!(
                                "fill {} {:?} sz={} px={} startPos={} fee={} dir={} closedPnl={}",
                                f.coin, side, f.sz, f.px, f.start_position, f.fee, f.dir, f.closed_pnl
                            );
                            state.log_event(&user_addr, bn, desc);
                        }
                        state.apply_fill(&user_addr, &fill.1);
                    }
                    total_fills += fills.len() as u64;
                }
            }
            EventType::Replica => {
                match serde_json::from_str::<replica::ReplicaBlock>(line) {
                Ok(block) => {
                    // Log state-mutating replica actions
                    if state.event_log.is_some() {
                        for (_, bundle) in &block.abci_block.signed_action_bundles {
                            for sa in &bundle.signed_actions {
                                if !sa.action.is_ignored() {
                                    let desc = format!("replica {:?}", sa.action);
                                    // Best effort: get user from vault_address or action
                                    if let Some(ref va) = sa.vault_address {
                                        state.log_event(&va.to_lowercase(), *block_num, desc);
                                    }
                                }
                            }
                        }
                    }
                    if replica::apply_replica_block(state, &block).is_ok() {
                        total_replica += 1;
                    }
                }
                Err(e) => {
                    // Log first few failures
                    if total_replica < 3 {
                        warn!("Failed to deserialize replica block: {e}");
                        warn!("  line (first 300 chars): {}", &line[..line.len().min(300)]);
                    }
                }
                }
            }
            EventType::MiscEvent => {
                if let Ok(batch) = serde_json::from_str::<misc_events::MiscEventBatch>(line) {
                    // Log misc events
                    if state.event_log.is_some() {
                        for ev in &batch.events {
                            match &ev.inner {
                                misc_events::MiscEventInner::Funding(funding) => {
                                    for d in &funding.deltas {
                                        let desc = format!("funding coin={} amt={}", d.coin, d.funding_amount);
                                        state.log_event(&d.user.to_lowercase(), batch.block_number, desc);
                                    }
                                }
                                misc_events::MiscEventInner::LedgerUpdate(lu) => {
                                    let desc = format!("ledger_update {:?}", lu.delta);
                                    for u in &lu.users {
                                        state.log_event(&u.to_lowercase(), batch.block_number, desc.clone());
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    let (nf, nl) = misc_events::apply_misc_event_batch(state, &batch);
                    total_misc += nf + nl;
                }
            }
        }
    }

    info!(
        "Interleaved replay: {total_fills} fills, {total_replica} replica blocks, {total_misc} misc events, {total_order_statuses} order statuses, {total_hip3_oracle} hip3 oracle updates (blocks {from_block}..{to_block})"
    );
    (total_fills, total_replica)
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

// ── State comparison ────────────────────────────────────────────────────

pub struct ComparisonResult {
    pub dex_idx: usize,
    pub users_in_truth: usize,
    pub users_in_replay: usize,
    pub szi_matches: usize,
    pub szi_mismatches: Vec<SziMismatch>,
    pub balance_drifts: Vec<BalanceDrift>,
    pub leverage_mismatches: Vec<LeverageMismatch>,
    pub cost_basis_drifts: Vec<PositionFieldDrift>,
    pub raw_usd_drifts: Vec<PositionFieldDrift>,
    pub funding_drifts: Vec<PositionFieldDrift>,
    pub scl_drifts: Vec<BalanceDrift>,
    pub missing_after_replay: Vec<String>,
    pub extra_after_replay: Vec<String>,
}

pub struct SziMismatch {
    pub user: String,
    pub asset_idx: u32,
    pub coin: String,
    pub replay_szi: i64,
    pub truth_szi: i64,
}

pub struct BalanceDrift {
    pub user: String,
    pub replay_balance: i64,
    pub truth_balance: i64,
    pub diff_usd: f64,
    pub pct: f64,
}

pub struct PositionFieldDrift {
    pub user: String,
    pub asset_idx: u32,
    pub coin: String,
    pub replay_val: i64,
    pub truth_val: i64,
}

pub struct LeverageMismatch {
    pub user: String,
    pub asset_idx: u32,
    pub coin: String,
    pub replay_lev: String,
    pub truth_lev: String,
}


/// Compare leverage type and value, ignoring raw_usd drift (which comes from funding).
fn leverage_type_matches(a: &Leverage, b: &Leverage) -> bool {
    match (a, b) {
        (Leverage::Cross(la), Leverage::Cross(lb)) => la == lb,
        (Leverage::Isolated { leverage: la, .. }, Leverage::Isolated { leverage: lb, .. }) => la == lb,
        _ => false,
    }
}

fn format_leverage(lev: &Leverage) -> String {
    match lev {
        Leverage::Cross(l) => format!("Cross({l})"),
        Leverage::Isolated { leverage, raw_usd } => format!("Isolated(lev={leverage}, usd={raw_usd})"),
    }
}

/// Compare replayed state against ground truth (parsed second snapshot).
/// Per-dex, per-field comparison. For Unified/DexAbstraction users, balance comparison
/// sums usdc across all dexes with the same collateral token + SCL, since the protocol
/// silently rebalances between them.
pub fn compare_states(replay: &LiquidationState, truth: &LiquidationState) -> Vec<ComparisonResult> {
    let mut results = Vec::new();

    // Pre-compute cross-dex totals for shared-usdc users (Unified + DexAbstraction).
    // Key = (user_addr, collateral_token), Value = sum of usdc_balance across all dexes + scl.
    let mut replay_cross_dex: HashMap<(String, u32), i64> = HashMap::new();
    let mut truth_cross_dex: HashMap<(String, u32), i64> = HashMap::new();

    // Track which (user, token) has already had SCL counted to avoid double-counting.
    // SCL is shared across dexes, so spot_collateral is the same value on each dex
    // with the same collateral token — count it only once.
    // SCL is shared across dexes, so spot_collateral is the same value on each dex
    // with the same collateral token — count it only once.
    let mut replay_scl_counted: std::collections::HashSet<(String, u32)> = std::collections::HashSet::new();
    for dex in &replay.dex_states {
        let token = dex.collateral_token;
        for (addr, user) in &dex.users {
            if user.account_mode.is_shared_usdc() {
                let key = (addr.clone(), token);
                let scl = if replay_scl_counted.insert(key.clone()) { user.spot_collateral / 100 } else { 0 };
                *replay_cross_dex.entry(key).or_default() += user.usdc_balance + scl;
            }
        }
        for (addr, partial) in &dex.users_without_positions {
            if partial.account_mode.is_shared_usdc() {
                let key = (addr.clone(), token);
                let scl = if replay_scl_counted.insert(key.clone()) { partial.spot_collateral / 100 } else { 0 };
                *replay_cross_dex.entry(key).or_default() += partial.usdc_balance + scl;
            }
        }
    }
    let mut truth_scl_counted: std::collections::HashSet<(String, u32)> = std::collections::HashSet::new();
    for dex in &truth.dex_states {
        let token = dex.collateral_token;
        for (addr, user) in &dex.users {
            if user.account_mode.is_shared_usdc() {
                let key = (addr.clone(), token);
                let scl = if truth_scl_counted.insert(key.clone()) { user.spot_collateral / 100 } else { 0 };
                *truth_cross_dex.entry(key).or_default() += user.usdc_balance + scl;
            }
        }
        for (addr, partial) in &dex.users_without_positions {
            if partial.account_mode.is_shared_usdc() {
                let key = (addr.clone(), token);
                let scl = if truth_scl_counted.insert(key.clone()) { partial.spot_collateral / 100 } else { 0 };
                *truth_cross_dex.entry(key).or_default() += partial.usdc_balance + scl;
            }
        }
    }
    // Track which shared-usdc users have already been compared (to avoid double-counting)
    let mut compared_shared_usdc: std::collections::HashSet<(String, u32)> = std::collections::HashSet::new();

    // Per-dex comparison
    for (dex_idx, truth_dex) in truth.dex_states.iter().enumerate() {
        let replay_dex = replay.dex_states.get(dex_idx);

        let mut result = ComparisonResult {
            dex_idx,
            users_in_truth: truth_dex.users.len(),
            users_in_replay: replay_dex.map_or(0, |d| d.users.len()),
            szi_matches: 0,
            szi_mismatches: Vec::new(),
            balance_drifts: Vec::new(),
            leverage_mismatches: Vec::new(),
            cost_basis_drifts: Vec::new(),
            raw_usd_drifts: Vec::new(),
            funding_drifts: Vec::new(),
            scl_drifts: Vec::new(),
            missing_after_replay: Vec::new(),
            extra_after_replay: Vec::new(),
        };

        let Some(replay_dex) = replay_dex else {
            result.missing_after_replay = truth_dex.users.keys().cloned().collect();
            results.push(result);
            continue;
        };

        // Build asset_idx → coin name map
        let asset_to_coin: HashMap<u32, &str> =
            truth_dex.universe.iter().enumerate().map(|(i, a)| (i as u32, a.name.as_str())).collect();

        // Check each user in truth
        for (addr, truth_user) in &truth_dex.users {
            // Skip portfolio margin users — borrow mechanics not yet replicated
            if truth.portfolio_margin_users.contains(addr) {
                continue;
            }
            // Skip vault addresses — equity-based withdrawals not yet replicated
            if truth.vault_states.contains_key(addr) {
                continue;
            }
            let Some(replay_user) = replay_dex.users.get(addr) else {
                result.missing_after_replay.push(addr.clone());
                continue;
            };

            {
                // For shared-usdc users (Unified + DexAbstraction): compare cross-dex
                // total equity (sum of usdc + scl across all dexes with same collateral token).
                // Only report once per (user, token) — on the first dex encountered.
                // For other modes: compare usdc_balance directly per dex.
                if replay_user.account_mode.is_shared_usdc() {
                    let token = replay_dex.collateral_token;
                    let key = (addr.clone(), token);
                    if !compared_shared_usdc.contains(&key) {
                        compared_shared_usdc.insert(key.clone());
                        let replay_total = replay_cross_dex.get(&key).copied().unwrap_or(0);
                        let truth_total = truth_cross_dex.get(&key).copied().unwrap_or(0);
                        let diff = replay_total - truth_total;
                        let diff_usd = diff as f64 / 1e6;
                        let truth_usd = truth_total as f64 / 1e6;
                        let pct = if truth_usd.abs() > 0.01 { (diff_usd / truth_usd) * 100.0 } else { 0.0 };
                        if diff.abs() > 1_000_000 {
                            result.balance_drifts.push(BalanceDrift {
                                user: addr.clone(),
                                replay_balance: replay_total,
                                truth_balance: truth_total,
                                diff_usd,
                                pct,
                            });
                        }
                    }
                } else {
                    let diff = replay_user.usdc_balance - truth_user.usdc_balance;
                    let diff_usd = diff as f64 / 1e6;
                    let truth_usd = truth_user.usdc_balance as f64 / 1e6;
                    let pct = if truth_usd.abs() > 0.01 { (diff_usd / truth_usd) * 100.0 } else { 0.0 };
                    if diff.abs() > 1_000_000 {
                        result.balance_drifts.push(BalanceDrift {
                            user: addr.clone(),
                            replay_balance: replay_user.usdc_balance,
                            truth_balance: truth_user.usdc_balance,
                            diff_usd,
                            pct,
                        });
                    }
                }
            }

            // Compare positions
            for (asset_idx, truth_pos) in &truth_user.positions {
                let coin = asset_to_coin.get(asset_idx).unwrap_or(&"?");
                let Some(replay_pos) = replay_user.positions.get(asset_idx) else {
                    result.szi_mismatches.push(SziMismatch {
                        user: addr.clone(),
                        asset_idx: *asset_idx,
                        coin: coin.to_string(),
                        replay_szi: 0,
                        truth_szi: truth_pos.szi,
                    });
                    continue;
                };

                if replay_pos.szi == truth_pos.szi {
                    result.szi_matches += 1;
                } else {
                    result.szi_mismatches.push(SziMismatch {
                        user: addr.clone(),
                        asset_idx: *asset_idx,
                        coin: coin.to_string(),
                        replay_szi: replay_pos.szi,
                        truth_szi: truth_pos.szi,
                    });
                }

                // Compare leverage type + value
                if !leverage_type_matches(&replay_pos.leverage, &truth_pos.leverage) {
                    result.leverage_mismatches.push(LeverageMismatch {
                        user: addr.clone(),
                        asset_idx: *asset_idx,
                        coin: coin.to_string(),
                        replay_lev: format_leverage(&replay_pos.leverage),
                        truth_lev: format_leverage(&truth_pos.leverage),
                    });
                }

                // Compare cost_basis
                if replay_pos.cost_basis != truth_pos.cost_basis {
                    result.cost_basis_drifts.push(PositionFieldDrift {
                        user: addr.clone(),
                        asset_idx: *asset_idx,
                        coin: coin.to_string(),
                        replay_val: replay_pos.cost_basis,
                        truth_val: truth_pos.cost_basis,
                    });
                }

                // Compare raw_usd (isolated positions)
                if let (Leverage::Isolated { raw_usd: r_raw, .. }, Leverage::Isolated { raw_usd: t_raw, .. })
                    = (&replay_pos.leverage, &truth_pos.leverage)
                {
                    if r_raw != t_raw {
                        result.raw_usd_drifts.push(PositionFieldDrift {
                            user: addr.clone(),
                            asset_idx: *asset_idx,
                            coin: coin.to_string(),
                            replay_val: *r_raw,
                            truth_val: *t_raw,
                        });
                    }
                }

                // Compare outstanding_funding
                if replay_pos.outstanding_funding != truth_pos.outstanding_funding {
                    result.funding_drifts.push(PositionFieldDrift {
                        user: addr.clone(),
                        asset_idx: *asset_idx,
                        coin: coin.to_string(),
                        replay_val: replay_pos.outstanding_funding,
                        truth_val: truth_pos.outstanding_funding,
                    });
                }
            }

            // Compare spot_collateral — skip for shared-usdc users since it's
            // already included in the cross-dex total equity comparison above.
            if !replay_user.account_mode.is_shared_usdc() {
                if replay_user.spot_collateral != truth_user.spot_collateral {
                    let diff = (replay_user.spot_collateral - truth_user.spot_collateral) / 100; // 8→6 dec
                    let diff_usd = diff as f64 / 1e6;
                    let truth_usd = (truth_user.spot_collateral / 100) as f64 / 1e6;
                    let pct = if truth_usd.abs() > 0.01 { (diff_usd / truth_usd) * 100.0 } else { 0.0 };
                    if diff.abs() > 1_000_000 {
                        result.scl_drifts.push(BalanceDrift {
                            user: addr.clone(),
                            replay_balance: replay_user.spot_collateral / 100,
                            truth_balance: truth_user.spot_collateral / 100,
                            diff_usd,
                            pct,
                        });
                    }
                }
            }

            // Check for extra positions in replay
            for (asset_idx, replay_pos) in &replay_user.positions {
                if !truth_user.positions.contains_key(asset_idx) {
                    let coin = asset_to_coin.get(asset_idx).unwrap_or(&"?");
                    result.szi_mismatches.push(SziMismatch {
                        user: addr.clone(),
                        asset_idx: *asset_idx,
                        coin: coin.to_string(),
                        replay_szi: replay_pos.szi,
                        truth_szi: 0,
                    });
                }
            }
        }

        // Check for extra users in replay
        for addr in replay_dex.users.keys() {
            if !truth_dex.users.contains_key(addr) {
                result.extra_after_replay.push(addr.clone());
            }
        }

        results.push(result);
    }

    results
}
