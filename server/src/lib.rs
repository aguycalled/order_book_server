#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
pub mod clearing_house;
pub mod history;
mod listeners;
pub mod metrics;
mod order_book;
mod prelude;
pub mod referral;
mod servers;
pub mod strategy;
mod types;

use std::path::PathBuf;

use clap::ValueEnum;

pub use prelude::Result;
pub use servers::websocket_server::run_websocket_server;

/// Open a RocksDB, transparently repairing it if the first open fails.
///
/// orderbook_server owns several RocksDBs but can be killed without a clean
/// shutdown (SIGTERM/SIGKILL/OOM/crash, or a supervisor restarting it around
/// an hl-node upgrade), which leaves the MANIFEST/WAL in a state the next
/// `DB::open` rejects (`Corruption: ... wal_dir contains existing log file`,
/// or a truncated MANIFEST referencing a compacted-away `.sst`). `DB::repair`
/// rebuilds the MANIFEST from the on-disk SST + WAL files, recovering the data.
/// A fresh/empty path opens cleanly on the first try, so repair only runs on a
/// genuinely broken store.
pub fn open_db_with_repair(
    opts: &rocksdb::Options,
    path: &std::path::Path,
) -> std::result::Result<rocksdb::DB, rocksdb::Error> {
    match rocksdb::DB::open(opts, path) {
        Ok(db) => Ok(db),
        Err(first_err) => {
            log::warn!("RocksDB open failed at {} ({first_err}); attempting repair", path.display());
            rocksdb::DB::repair(opts, path)?;
            let db = rocksdb::DB::open(opts, path)?;
            log::warn!("RocksDB repaired and reopened at {}", path.display());
            Ok(db)
        }
    }
}

/// Snapshot fetching mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum SnapshotMode {
    /// Use docker exec to call hl-node inside container
    #[default]
    Docker,
    /// Call hl-node directly (for systemctl/bare metal setups)
    Direct,
}

/// Server configuration passed from CLI arguments
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Full address string (e.g., "0.0.0.0:8000")
    pub address: String,
    /// WebSocket compression level (0-9)
    pub compression_level: u32,
    /// Optional base directory for hlnode data
    pub data_dir: Option<PathBuf>,
    /// Include perpetual futures markets
    pub include_perps: bool,
    /// Include spot markets (@ coins, PURR/USDC)
    pub include_spot: bool,
    /// Include HIP-3 markets
    pub include_hip3: bool,
    /// Snapshot fetching mode (docker or direct)
    pub snapshot_mode: SnapshotMode,
    /// Docker container name for exec commands (docker mode only)
    pub docker_container: String,
    /// Path to hl-node binary (direct mode only)
    pub hlnode_binary: String,
    /// Path to abci_state.rmp file (direct mode only, has default)
    pub abci_state_path: Option<PathBuf>,
    /// Path where snapshot will be written (direct mode only, has default)
    pub snapshot_output_path: Option<PathBuf>,
    /// Path to visor_abci_state.json (optional)
    pub visor_state_path: Option<PathBuf>,
    /// Port for Prometheus metrics endpoint (0 to disable)
    pub metrics_port: u16,
    /// BBO-only mode: lightweight mode that only tracks best bid/ask per coin
    /// Disables L2/L4/Trades subscriptions but uses ~100MB RAM instead of 2-3GB
    pub bbo_only: bool,
    /// Path to the L2 history RocksDB database (optional override)
    pub history_db_path: Option<PathBuf>,
    /// Build and serve a liquidation map via WebSocket subscriptions.
    /// Loads clearing house state from RMP snapshots and tracks fills.
    pub build_liquidation_map: bool,
    /// Path to the referral-stats RocksDB database (optional override).
    pub referral_stats_db_path: Option<PathBuf>,
    /// Path to the per-strategy-stats RocksDB database (optional override).
    pub strategy_stats_db_path: Option<PathBuf>,
    /// Referral code whose users we track (e.g. "HYBRIDGE"). Case-insensitive.
    pub track_referral_code: String,
    /// Builder address whose fills we track (0x-prefixed hex).
    pub track_builder_address: String,
    /// Fraction of a referee's gross fee paid out to us as referral reward
    /// (e.g. 0.10 = 10%). Confirm against current Hyperliquid referral terms.
    pub referral_reward_rate: f64,
}
