//! Fill-volume and fee tracking for users with a specific referral code
//! (e.g. HYBRIDGE) or for fills tagged with a specific builder address.
//!
//! A fill is counted if ANY of:
//!   - the filling user is in the tracked referral set, OR
//!   - the fill carries our target builder address (read directly from the
//!     fill's `builder` field, along with the exact `builderFee`).
//!
//! Aggregates are kept in RocksDB keyed by user address; HTTP endpoints
//! expose per-user lookup and top-N queries.

pub mod aggregator;
pub mod handlers;
pub mod referrer_tracker;
pub mod stats_db;

pub use aggregator::spawn_referral_consumer;
pub use handlers::{
    stats_growth_handler, stats_referral_accrual_handler, stats_top_handler,
    stats_user_daily_handler, stats_user_handler,
};
pub use referrer_tracker::{ReferrerTracker, spawn_growth_recorder, spawn_referrer_tailer};
pub use stats_db::{ReferralStatsDb, UserStats};
