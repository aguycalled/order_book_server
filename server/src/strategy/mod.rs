//! Per-strategy fill-volume attribution for Borsa orders.
//!
//! Every Borsa live order carries a 16-byte client order id (cloid) of the
//! form `0x<32 hex>`:
//!   - bytes[0..1]  = magic `0xB05A` ("this is a Borsa order")
//!   - byte[2]      = version `0x01`
//!   - bytes[3..10] = stratKey (8 bytes = 16 hex) — the strategy id
//!   - bytes[11..15]= nonce (5 bytes)
//!
//! A fill is attributed to a strategy when it is BOTH a Borsa cloid AND already
//! Borsa-attributed by the referral module's existing logic (filling user is in
//! the tracked referral set, OR the order carries our builder address). The
//! `stratKey` equals the first 16 hex of the strategy's marketplace
//! `versionHash` digest, so it is matchable from public data — no mapping table.

pub mod aggregator;
pub mod handlers;
pub mod stats_db;

pub use aggregator::accumulate_strategy_fill;
pub use handlers::{stats_strategy_batch_handler, stats_strategy_handler};
pub use stats_db::{StrategyStats, StrategyStatsDb, decode_strategy_cloid};
