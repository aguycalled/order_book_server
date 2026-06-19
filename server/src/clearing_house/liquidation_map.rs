//! Liquidation map: queries /info for users with positions, builds per-coin
//! heatmaps of liquidation prices using the protocol's own calculations.

use std::collections::HashMap;

use log::{debug, warn};
use serde::Deserialize;

use crate::types::{L4LiquidationEntry, L4LiquidationMapData, LiquidationLevel, LiquidationMapData};

// ── /info response types ────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClearinghouseResponse {
    pub asset_positions: Vec<AssetPositionWrapper>,
    pub margin_summary: Option<MarginSummary>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AssetPositionWrapper {
    pub position: PositionInfo,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PositionInfo {
    pub coin: String,
    pub szi: String,
    pub leverage: LeverageInfo,
    pub entry_px: String,
    pub liquidation_px: Option<String>,
    #[serde(default)]
    pub position_value: String,
    #[serde(default)]
    pub unrealized_pnl: String,
    #[serde(default)]
    pub max_leverage: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LeverageInfo {
    #[serde(rename = "type")]
    pub lev_type: String,
    pub value: u32,
    #[serde(default)]
    pub raw_usd: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarginSummary {
    pub account_value: String,
    pub total_margin_used: String,
}

// ── Query /info ─────────────────────────────────────────────────────────

/// Query local node /info endpoint for a user's clearinghouse state.
///
/// `dex` is the HIP-3 perp dex short name (e.g. `"xyz"`). Pass an empty string
/// for the primary perp dex (dex 0). Omitting the `dex` field on the request
/// is equivalent to passing dex 0, so HIP-3 positions only come back when the
/// specific dex name is supplied.
pub async fn query_user_state(
    client: &reqwest::Client,
    node_url: &str,
    user: &str,
    dex: &str,
) -> Option<ClearinghouseResponse> {
    let body = if dex.is_empty() {
        serde_json::json!({
            "type": "clearinghouseState",
            "user": user,
        })
    } else {
        serde_json::json!({
            "type": "clearinghouseState",
            "user": user,
            "dex": dex,
        })
    };
    let resp = client.post(node_url).json(&body).send().await.ok()?;
    resp.json().await.ok()
}

/// Query /info for multiple users concurrently on a specific dex (limited concurrency).
pub async fn query_users_batch(
    client: &reqwest::Client,
    node_url: &str,
    users: &[String],
    dex: &str,
    max_concurrent: usize,
) -> Vec<(String, ClearinghouseResponse)> {
    use futures_util::stream::{self, StreamExt};

    let results: Vec<_> = stream::iter(users.iter().cloned())
        .map(|user| {
            let client = client.clone();
            let url = node_url.to_string();
            let dex = dex.to_string();
            async move {
                let resp = query_user_state(&client, &url, &user, &dex).await;
                (user, resp)
            }
        })
        .buffer_unordered(max_concurrent)
        .filter_map(|(user, resp)| async move { resp.map(|r| (user, r)) })
        .collect()
        .await;

    results
}

// ── Build maps from /info data ──────────────────────────────────────────

struct LiqEntry {
    user: String,
    coin: String,
    is_long: bool,
    sz_abs: f64,
    entry_px: f64,
    liq_px: f64,
    leverage: u32,
    margin_type: String,
}

/// Extract liquidation entries from /info responses.
fn extract_entries(responses: &[(String, ClearinghouseResponse)]) -> Vec<LiqEntry> {
    let mut entries = Vec::new();
    for (user, resp) in responses {
        for wrapper in &resp.asset_positions {
            let pos = &wrapper.position;
            let szi: f64 = pos.szi.parse().unwrap_or(0.0);
            if szi == 0.0 {
                continue;
            }
            let liq_px: f64 = match &pos.liquidation_px {
                Some(s) => s.parse().unwrap_or(0.0),
                None => continue,
            };
            if liq_px <= 0.0 {
                continue;
            }
            let entry_px: f64 = pos.entry_px.parse().unwrap_or(0.0);
            entries.push(LiqEntry {
                user: user.clone(),
                coin: pos.coin.clone(),
                is_long: szi > 0.0,
                sz_abs: szi.abs(),
                entry_px,
                liq_px,
                leverage: pos.leverage.value,
                margin_type: pos.leverage.lev_type.clone(),
            });
        }
    }
    entries
}

/// Build L2 aggregated liquidation map for a specific coin from /info data.
pub fn build_liquidation_map_from_entries(entries: &[LiqEntry], coin: &str, bucket_size: f64) -> LiquidationMapData {
    let time = now_millis();

    let mut long_buckets: HashMap<i64, (f64, f64, usize)> = HashMap::new();
    let mut short_buckets: HashMap<i64, (f64, f64, usize)> = HashMap::new();

    for e in entries {
        if e.coin != coin {
            continue;
        }
        let bucket_key = (e.liq_px / bucket_size).floor() as i64;
        let ntl = e.sz_abs * e.liq_px;
        let map = if e.is_long { &mut long_buckets } else { &mut short_buckets };
        let entry = map.entry(bucket_key).or_insert((0.0, 0.0, 0));
        entry.0 += e.sz_abs;
        entry.1 += ntl;
        entry.2 += 1;
    }

    LiquidationMapData {
        coin: coin.to_string(),
        time,
        levels: [to_levels(long_buckets, bucket_size), to_levels(short_buckets, bucket_size)],
    }
}

/// Build L4 per-user liquidation map for a specific coin from /info data.
pub fn build_l4_liquidation_map_from_entries(entries: &[LiqEntry], coin: &str) -> L4LiquidationMapData {
    let time = now_millis();

    let positions: Vec<L4LiquidationEntry> = entries
        .iter()
        .filter(|e| e.coin == coin)
        .map(|e| L4LiquidationEntry {
            user: e.user.clone(),
            side: if e.is_long { "long".to_string() } else { "short".to_string() },
            sz: format!("{:.6}", e.sz_abs),
            entry_px: format!("{:.2}", e.entry_px),
            liq_px: format!("{:.2}", e.liq_px),
            leverage: format!("{}", e.leverage),
            margin_type: e.margin_type.clone(),
        })
        .collect();

    L4LiquidationMapData { coin: coin.to_string(), time, positions }
}

/// Median of `entry_px` across a coin's entries, used as a stand-in mark price
/// for markets that don't yet have one in state (e.g. HIP-3 markets created
/// after the most recent RMP snapshot).
fn estimate_mark_from_entries(entries: &[LiqEntry], coin: &str) -> f64 {
    let mut pxs: Vec<f64> = entries.iter().filter(|e| e.coin == coin && e.entry_px > 0.0).map(|e| e.entry_px).collect();
    if pxs.is_empty() {
        return 0.0;
    }
    pxs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    pxs[pxs.len() / 2]
}

/// Determine bucket size for a given mark price (~1% resolution).
pub fn auto_bucket_size(mark_px: f64) -> f64 {
    if mark_px <= 0.0 {
        return 1.0;
    }
    let magnitude = 10f64.powf((mark_px.log10()).floor() - 2.0);
    if magnitude < 0.001 { 0.001 } else { magnitude }
}

/// Full pipeline: query users on the primary perp dex, extract, build maps.
/// Kept for tools/tests; production builder queries each dex separately so it
/// also captures HIP-3 positions.
pub async fn build_all_maps(
    client: &reqwest::Client,
    node_url: &str,
    users: &[String],
    mark_prices: &HashMap<String, f64>,
    max_concurrent: usize,
) -> (Vec<LiquidationMapData>, Vec<L4LiquidationMapData>) {
    let responses = query_users_batch(client, node_url, users, "", max_concurrent).await;
    debug!("Liquidation map: queried {}/{} users", responses.len(), users.len());
    build_maps_from_responses(&responses, mark_prices)
}

/// Build maps from already-collected /info responses.
pub fn build_maps_from_responses(
    responses: &[(String, ClearinghouseResponse)],
    mark_prices: &HashMap<String, f64>,
) -> (Vec<LiquidationMapData>, Vec<L4LiquidationMapData>) {
    let entries = extract_entries(responses);

    let mut coins: Vec<String> = entries.iter().map(|e| e.coin.clone()).collect();
    coins.sort();
    coins.dedup();

    let mut maps = Vec::new();
    let mut l4_maps = Vec::new();

    for coin in &coins {
        let mark_px = mark_prices
            .get(coin.as_str())
            .copied()
            .filter(|&p| p > 0.0)
            .unwrap_or_else(|| estimate_mark_from_entries(&entries, coin));
        let bucket_size = if mark_px > 0.0 { auto_bucket_size(mark_px) } else { 1.0 };
        maps.push(build_liquidation_map_from_entries(&entries, coin, bucket_size));
        l4_maps.push(build_l4_liquidation_map_from_entries(&entries, coin));
    }

    (maps, l4_maps)
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn now_millis() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn to_levels(buckets: HashMap<i64, (f64, f64, usize)>, bucket_size: f64) -> Vec<LiquidationLevel> {
    let mut levels: Vec<_> = buckets
        .into_iter()
        .map(|(k, (coin_sz, ntl_sz, n))| {
            let px = k as f64 * bucket_size;
            LiquidationLevel {
                px: format!("{:.1}", px),
                coin_sz: format!("{:.4}", coin_sz),
                ntl_sz: format!("{:.2}", ntl_sz),
                n,
            }
        })
        .collect();
    levels.sort_by(|a, b| a.px.parse::<f64>().unwrap_or(0.0).partial_cmp(&b.px.parse::<f64>().unwrap_or(0.0)).unwrap());
    levels
}
