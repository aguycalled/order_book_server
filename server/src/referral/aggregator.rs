//! Subscribes to the HFT fill stream, attributes each fill to the tracked
//! referral set and/or our builder address, and writes running aggregates to
//! `ReferralStatsDb` — one cumulative row plus one per-UTC-day row per user.
//!
//! Builder attribution reads the `builder` and `builderFee` fields carried
//! directly on each fill (HIP-3 / builder-code orders include them). This is
//! exact and race-free. Earlier code resolved the builder via a separate
//! order-status cache keyed by oid, which silently dropped immediate-fill
//! (taker / IOC) orders whose `open` status landed in the same block as the
//! fill — the fill was attributed before the status reached the cache.

use std::{collections::HashMap, sync::Arc};

use alloy::primitives::Address;
use log::warn;

use crate::listeners::order_book::HftMessage;
use crate::referral::{
    ReferralStatsDb, ReferrerTracker, UserStats,
    stats_db::{cum_key, day_key},
};
use crate::strategy::{StrategyStats, StrategyStatsDb, accumulate_strategy_fill};
use crate::types::{Fill, node_data::NodeDataFill};

const FLAG_REFERRAL: u8 = 1;
const FLAG_BUILDER: u8 = 2;

/// Scale factor: a quote-USD float → u128 scaled by 1e8.
const VOLUME_SCALE: f64 = 1e8;

const MS_PER_DAY: u64 = 86_400_000;

/// Single consumer of the HFT fill stream for builder/referral attribution.
/// Order-status events are no longer needed — the builder is on the fill.
pub fn spawn_referral_consumer(
    db: Arc<ReferralStatsDb>,
    strategy_db: Arc<StrategyStatsDb>,
    tracker: Arc<ReferrerTracker>,
    target_builder: Address,
    referral_reward_rate: f64,
    mut hft_rx: tokio::sync::broadcast::Receiver<Arc<HftMessage>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match hft_rx.recv().await {
                Ok(msg) => {
                    if let HftMessage::Fills { batch } = msg.as_ref() {
                        apply_batch(&db, &strategy_db, &tracker, &target_builder, referral_reward_rate, batch);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!("referral_consumer: lagged {n} messages");
                }
                Err(_) => {
                    log::error!("referral_consumer: hft channel closed");
                    return;
                }
            }
        }
    })
}

/// Parse a quote-denominated decimal fee string (e.g. "0.003537") into an
/// e8-scaled u128. Returns 0 on missing / unparseable / non-positive input.
fn fee_str_to_e8(s: Option<&str>) -> u128 {
    let Some(v) = s.and_then(|s| s.parse::<f64>().ok()) else { return 0 };
    if !v.is_finite() || v <= 0.0 {
        return 0;
    }
    let scaled = (v * VOLUME_SCALE).round();
    if scaled >= 0.0 && scaled < u128::MAX as f64 { scaled as u128 } else { 0 }
}

fn apply_batch(
    db: &ReferralStatsDb,
    strategy_db: &StrategyStatsDb,
    tracker: &ReferrerTracker,
    target_builder: &Address,
    referral_reward_rate: f64,
    batch: &crate::types::node_data::Batch<NodeDataFill>,
) {
    // Group deltas by row key (cumulative and daily) so we only do one DB
    // round-trip per row per batch.
    let mut by_key: HashMap<Vec<u8>, UserStats> = HashMap::new();
    // Same idea for the per-strategy aggregates, keyed by stratKey row.
    let mut strat_by_key: HashMap<Vec<u8>, StrategyStats> = HashMap::new();
    let block_ms = batch.block_time();
    let day = (block_ms / MS_PER_DAY) as u32;

    for fill in batch.events_ref() {
        let addr = fill.0;
        let user_lower = format!("{addr:#x}");
        let referral_hit = tracker.is_tracked(&user_lower);

        // Builder attribution straight off the fill: does it carry OUR builder
        // address? If so, `builderFee` is the exact fee we earned on it.
        let is_our_builder = fill
            .1
            .builder
            .as_deref()
            .and_then(|b| b.parse::<Address>().ok())
            .map(|b| b == *target_builder)
            .unwrap_or(false);
        let builder_fee_e8 = if is_our_builder { fee_str_to_e8(fill.1.builder_fee.as_deref()) } else { 0 };

        if !referral_hit && !is_our_builder {
            continue;
        }

        // This fill is Borsa-attributed (referral hit or our builder code).
        // Reuse that exact determination to also bucket it by strategy: a fill
        // counts for a strategy iff it already counts for Borsa here.
        accumulate_strategy_fill(
            &mut strat_by_key,
            strategy_db,
            fill.1.cloid.as_deref(),
            &fill.1.px,
            &fill.1.sz,
            &user_lower,
            block_ms,
            builder_fee_e8,
        );

        let flags = (if referral_hit { FLAG_REFERRAL } else { 0 })
            | (if is_our_builder { FLAG_BUILDER } else { 0 });

        for key in [cum_key(&addr).to_vec(), day_key(&addr, day).to_vec()] {
            let entry = by_key.entry(key).or_insert_with_key(|k| db.get_raw(k).unwrap_or_default());
            accumulate(entry, &fill.1, block_ms, referral_hit, referral_reward_rate, builder_fee_e8, is_our_builder);
            entry.match_flags |= flags;
        }
    }

    if !strat_by_key.is_empty() {
        let strat_updates: Vec<(Vec<u8>, StrategyStats)> = strat_by_key.into_iter().collect();
        strategy_db.write_batch_raw(&strat_updates);
    }

    if by_key.is_empty() {
        return;
    }
    let updates: Vec<(Vec<u8>, UserStats)> = by_key.into_iter().collect();
    db.write_batch_raw(&updates);
}

fn accumulate(
    stats: &mut UserStats,
    fill: &Fill,
    block_ms: u64,
    referral_hit: bool,
    referral_reward_rate: f64,
    builder_fee_e8: u128,
    is_our_builder: bool,
) {
    let px: f64 = fill.px.parse().unwrap_or(0.0);
    let sz: f64 = fill.sz.parse().unwrap_or(0.0);
    let notional = px * sz;
    if notional.is_finite() && notional > 0.0 {
        let scaled = (notional * VOLUME_SCALE).round();
        if scaled >= 0.0 && scaled < u128::MAX as f64 {
            let n = scaled as u128;
            stats.volume_quote_e8 = stats.volume_quote_e8.saturating_add(n);
            // Earning volume: only fills routed through our builder.
            if is_our_builder {
                stats.builder_volume_quote_e8 = stats.builder_volume_quote_e8.saturating_add(n);
            }
        }
    }

    let fee: f64 = fill.fee.parse().unwrap_or(0.0);
    if fee.is_finite() && fee != 0.0 {
        let scaled = (fee * VOLUME_SCALE).round() as i128;
        add_to_token_map(&mut stats.fees_by_token, &fill.fee_token, scaled);
        if referral_hit {
            let reward = (fee * referral_reward_rate * VOLUME_SCALE).round() as i128;
            if reward != 0 {
                add_to_token_map(&mut stats.referral_fees_by_token, &fill.fee_token, reward);
            }
        }
    }

    // Exact builder fee from the fill (0 when not our builder).
    stats.builder_fees_quote_e8 = stats.builder_fees_quote_e8.saturating_add(builder_fee_e8);

    stats.fill_count = stats.fill_count.saturating_add(1);
    stats.last_update_ms = block_ms;
}

fn add_to_token_map(map: &mut std::collections::BTreeMap<String, String>, token: &str, delta: i128) {
    let entry = map.entry(token.to_string()).or_insert_with(|| "0".to_string());
    let prev: i128 = entry.parse().unwrap_or(0);
    *entry = prev.saturating_add(delta).to_string();
}
