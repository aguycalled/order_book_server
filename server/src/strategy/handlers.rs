//! Axum handlers for /stats/strategy/:strat_key and /stats/strategy/batch.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};

use crate::strategy::stats_db::{StrategyStatsDb, decode_strat_key_hex, e8_to_f64};

/// Largest batch the POST endpoint will service in one request.
const MAX_BATCH: usize = 500;

/// Cap on the number of user addresses returned by the single-key endpoint.
const MAX_USERS: usize = 500;

/// Public per-strategy view. Zeros for an unknown strategy.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StrategyView {
    strat_key: String,
    volume_usd: f64,
    fills: u64,
    unique_users: u64,
    builder_fees_usd: f64,
    vol_24h: f64,
    vol_7d: f64,
    vol_30d: f64,
    first_ts: u64,
    last_ts: u64,
    /// Capped list of distinct user addresses (lowercase `0x…`). Populated only
    /// by the single-key GET endpoint; omitted on the batch endpoint to keep the
    /// marketplace listing lightweight.
    #[serde(skip_serializing_if = "Option::is_none")]
    users: Option<Vec<String>>,
}

impl StrategyView {
    fn zero(strat_key: String, with_users: bool) -> Self {
        Self {
            strat_key,
            volume_usd: 0.0,
            fills: 0,
            unique_users: 0,
            builder_fees_usd: 0.0,
            vol_24h: 0.0,
            vol_7d: 0.0,
            vol_30d: 0.0,
            first_ts: 0,
            last_ts: 0,
            users: with_users.then(Vec::new),
        }
    }
}

fn today_epoch_day() -> u32 {
    (chrono::Utc::now().timestamp() / 86_400) as u32
}

/// Build the public view for one stratKey, summing daily rows for the rolling
/// 24h/7d/30d windows. Returns the zero view when the strategy is unknown.
///
/// `with_users` adds a capped list of the distinct user addresses; it is set
/// only for the single-key GET endpoint and left off for the batch listing.
fn build_view(db: &StrategyStatsDb, hex: &str, key: &[u8; 8], today: u32, with_users: bool) -> StrategyView {
    let Some(stats) = db.get(key) else {
        return StrategyView::zero(hex.to_string(), with_users);
    };
    let window = |days: u32| -> f64 {
        let from = today.saturating_sub(days.saturating_sub(1));
        db.strategy_daily(key, from, today).into_iter().map(|(_, s)| e8_to_f64(s.volume_quote_e8)).sum()
    };
    StrategyView {
        strat_key: hex.to_string(),
        volume_usd: e8_to_f64(stats.volume_quote_e8),
        fills: stats.fill_count,
        unique_users: stats.unique_users.len() as u64,
        builder_fees_usd: e8_to_f64(stats.builder_fees_quote_e8),
        vol_24h: window(1),
        vol_7d: window(7),
        vol_30d: window(30),
        first_ts: stats.first_update_ms,
        last_ts: stats.last_update_ms,
        // The stored set is a BTreeSet (address-ordered, not recency-ordered);
        // we cannot cheaply order by recency, so return up to MAX_USERS from it.
        users: with_users.then(|| stats.unique_users.iter().take(MAX_USERS).cloned().collect()),
    }
}

pub async fn stats_strategy_handler(
    Path(strat_key): Path<String>,
    State(db): State<Arc<StrategyStatsDb>>,
) -> impl IntoResponse {
    let hex = strat_key.to_ascii_lowercase();
    let key = match decode_strat_key_hex(&hex) {
        Some(k) => k,
        None => return (StatusCode::BAD_REQUEST, "invalid stratKey, want 16 hex chars").into_response(),
    };
    let view = build_view(&db, &hex, &key, today_epoch_day(), true);
    axum::response::Json(view).into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchRequest {
    strat_keys: Vec<String>,
}

pub async fn stats_strategy_batch_handler(
    State(db): State<Arc<StrategyStatsDb>>,
    axum::Json(req): axum::Json<BatchRequest>,
) -> impl IntoResponse {
    if req.strat_keys.len() > MAX_BATCH {
        return (StatusCode::BAD_REQUEST, format!("too many stratKeys (max {MAX_BATCH})")).into_response();
    }
    let today = today_epoch_day();
    let mut out: std::collections::HashMap<String, StrategyView> = std::collections::HashMap::new();
    for raw in &req.strat_keys {
        let hex = raw.to_ascii_lowercase();
        let Some(key) = decode_strat_key_hex(&hex) else {
            continue; // skip malformed keys rather than failing the whole batch
        };
        out.insert(hex.clone(), build_view(&db, &hex, &key, today, false));
    }
    axum::response::Json(out).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategy::stats_db::{StrategyStats, cum_key};

    fn db_with_strategy(dir: &tempfile::TempDir, key: &[u8; 8]) -> StrategyStatsDb {
        let db = StrategyStatsDb::open(dir.path().join("t.rocksdb")).unwrap();
        let mut s = StrategyStats::default();
        s.volume_quote_e8 = 600_000_000;
        s.fill_count = 2;
        s.last_update_ms = 1_700_000_000_000;
        s.unique_users.insert("0xaaa".to_string());
        s.unique_users.insert("0xbbb".to_string());
        db.write_batch_raw(&[(cum_key(key).to_vec(), s)]);
        db
    }

    #[test]
    fn single_key_view_includes_users_array() {
        let key: [u8; 8] = [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
        let dir = tempfile::tempdir().unwrap();
        let db = db_with_strategy(&dir, &key);
        let view = build_view(&db, "0011223344556677", &key, today_epoch_day(), true);
        let json = serde_json::to_value(&view).unwrap();
        // `users` is present and lists the stored lowercase addresses.
        assert_eq!(json["uniqueUsers"], 2);
        let users = json["users"].as_array().expect("users array present");
        let addrs: Vec<&str> = users.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(addrs.contains(&"0xaaa") && addrs.contains(&"0xbbb"));
        assert!(addrs.len() <= MAX_USERS);
    }

    #[test]
    fn batch_view_omits_users_array() {
        let key: [u8; 8] = [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77];
        let dir = tempfile::tempdir().unwrap();
        let db = db_with_strategy(&dir, &key);
        let view = build_view(&db, "0011223344556677", &key, today_epoch_day(), false);
        let json = serde_json::to_value(&view).unwrap();
        // Batch keeps the count but skips the address list entirely.
        assert_eq!(json["uniqueUsers"], 2);
        assert!(json.get("users").is_none());
    }

    #[test]
    fn unknown_strategy_zero_view_respects_with_users() {
        let key: [u8; 8] = [9; 8];
        let dir = tempfile::tempdir().unwrap();
        let db = StrategyStatsDb::open(dir.path().join("t.rocksdb")).unwrap();
        let with = serde_json::to_value(build_view(&db, "0909090909090909", &key, today_epoch_day(), true)).unwrap();
        let without =
            serde_json::to_value(build_view(&db, "0909090909090909", &key, today_epoch_day(), false)).unwrap();
        // GET returns an empty array for an unknown strategy; batch omits the field.
        assert_eq!(with["users"].as_array().unwrap().len(), 0);
        assert!(without.get("users").is_none());
    }
}
