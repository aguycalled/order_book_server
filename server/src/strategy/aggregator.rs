//! Per-strategy fill bucketing.
//!
//! This does NOT subscribe to the fill stream independently — it hooks into the
//! referral aggregator's existing per-fill loop (`referral::aggregator`), which
//! already determines whether a fill is "ours" (filling user in the tracked
//! referral set, OR order carries our builder address). For every such
//! attributed fill we additionally decode the order's Borsa cloid and, when it
//! is one, bucket the fill's notional by stratKey. Reusing that single
//! determination keeps strategy attribution and referral attribution in lock
//! step — a fill counts for a strategy iff it already counts for Borsa.

use std::collections::HashMap;

use crate::strategy::stats_db::{StrategyStats, StrategyStatsDb, cum_key, day_key, decode_strategy_cloid};

const MS_PER_DAY: u64 = 86_400_000;

/// Volume scale (e8) — kept in lock step with `referral::aggregator`.
const VOLUME_SCALE: f64 = 1e8;

/// Accumulate one already-Borsa-attributed fill into the per-strategy batch.
///
/// Call this from the referral aggregator's fill loop, passing the same `cloid`,
/// `px`, `sz` and the exact `builder_fee_e8` it read off the fill (the
/// `builderFee` field, e8-scaled; 0 when the order isn't ours). No-op when the
/// cloid is absent or is not a Borsa cloid. `by_key` collects deltas so the
/// caller writes each row once.
pub fn accumulate_strategy_fill(
    by_key: &mut HashMap<Vec<u8>, StrategyStats>,
    db: &StrategyStatsDb,
    cloid: Option<&str>,
    px: &str,
    sz: &str,
    user_lower: &str,
    block_ms: u64,
    builder_fee_e8: u128,
) {
    let Some(cloid) = cloid else { return };
    let Some(strat_key) = decode_strategy_cloid(cloid) else { return };

    let px: f64 = px.parse().unwrap_or(0.0);
    let sz: f64 = sz.parse().unwrap_or(0.0);
    let notional = px * sz;
    let mut notional_e8: u128 = 0;
    if notional.is_finite() && notional > 0.0 {
        let scaled = (notional * VOLUME_SCALE).round();
        if scaled >= 0.0 && scaled < u128::MAX as f64 {
            notional_e8 = scaled as u128;
        }
    }
    let day = (block_ms / MS_PER_DAY) as u32;
    // Cumulative row keeps the unique-user set; daily rows stay compact.
    for (key, track_users) in [(cum_key(&strat_key).to_vec(), true), (day_key(&strat_key, day).to_vec(), false)] {
        let entry = by_key.entry(key).or_insert_with_key(|k| db.get_raw(k).unwrap_or_default());
        entry.accumulate(notional_e8, builder_fee_e8, user_lower, block_ms, track_users);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_borsa_cloid_into_cum_and_daily() {
        let dir = tempfile::tempdir().unwrap();
        let db = StrategyStatsDb::open(dir.path().join("t.rocksdb")).unwrap();
        let mut by_key: HashMap<Vec<u8>, StrategyStats> = HashMap::new();
        let cloid = "0xb05a010011223344556677008899aabb";
        // px*sz = 2*3 = 6 USD; builder fee passed in exact, e8-scaled (60_000).
        accumulate_strategy_fill(&mut by_key, &db, Some(cloid), "2", "3", "0xabc", 1_700_000_000_000, 60_000);
        // One cumulative + one daily row.
        assert_eq!(by_key.len(), 2);
        let strat_key = decode_strategy_cloid(cloid).unwrap();
        let cum = by_key.get(&cum_key(&strat_key).to_vec()).unwrap();
        assert_eq!(cum.volume_quote_e8, 600_000_000); // 6 * 1e8
        assert_eq!(cum.fill_count, 1);
        assert_eq!(cum.unique_users.len(), 1);
        assert_eq!(cum.builder_fees_quote_e8, 60_000);
    }

    #[test]
    fn ignores_non_borsa_and_missing_cloid() {
        let dir = tempfile::tempdir().unwrap();
        let db = StrategyStatsDb::open(dir.path().join("t.rocksdb")).unwrap();
        let mut by_key: HashMap<Vec<u8>, StrategyStats> = HashMap::new();
        accumulate_strategy_fill(&mut by_key, &db, None, "2", "3", "0xabc", 1_700_000_000_000, 0);
        accumulate_strategy_fill(
            &mut by_key,
            &db,
            Some("0xdead010011223344556677008899aabb"),
            "2",
            "3",
            "0xabc",
            1_700_000_000_000,
            0,
        );
        assert!(by_key.is_empty());
    }
}
