//! Per-subscription state reducers.
//!
//! The relay keeps one upstream subscription per unique (type+coin+params) and
//! reconstructs that channel's current state in memory, so a newly-joining
//! client can be handed a full snapshot and then ride the shared live stream.
//!
//! Forwarding is zero-copy-ish: the raw upstream frame text is broadcast as-is
//! to every group member (already serialized once upstream). The reducer only
//! exists to synthesize the *bootstrap snapshot* for late joiners.
//!
//! Coverage:
//!   - l2Book / triggerBook : full level-book reconstruction (delta merge)
//!   - bbo / liquidationMap / l4LiquidationMap : last-full-frame cache
//!   - allPrices : price-map merge
//!   - trades / orderUpdates / l4Book / l4TriggerBook : live passthrough
//!     (new peers join the live stream; no historical snapshot in v1)

use serde_json::{json, Value};
use std::collections::HashMap;

pub enum Reducer {
    /// l2Book / triggerBook — reconstruct [bids, asks] keyed by price string.
    Levels {
        channel: String,
        coin: String,
        // params echoed back in the snapshot frame
        n_sig_figs: Option<Value>,
        n_levels: Option<usize>,
        mantissa: Option<Value>,
        // px -> level object {px, sz, n}
        bids: HashMap<String, Value>,
        asks: HashMap<String, Value>,
        time: u64,
    },
    /// bbo / liquidationMap / l4LiquidationMap — cache the last full frame text.
    LastFrame(Option<String>),
    /// allPrices — merge coin->price map.
    PriceMap { prices: HashMap<String, String> },
    /// trades / orderUpdates / l4Book / l4TriggerBook — no bootstrap snapshot.
    Passthrough,
}

impl Reducer {
    /// Build a reducer for the given channel + parsed subscription params.
    pub fn new(channel: &str, sub: &Value) -> Self {
        match channel {
            "l2Book" | "triggerBook" => Reducer::Levels {
                channel: channel.to_string(),
                coin: sub.get("coin").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                n_sig_figs: sub.get("nSigFigs").filter(|v| !v.is_null()).cloned(),
                n_levels: sub.get("nLevels").and_then(|v| v.as_u64()).map(|n| n as usize),
                mantissa: sub.get("mantissa").filter(|v| !v.is_null()).cloned(),
                bids: HashMap::new(),
                asks: HashMap::new(),
                time: 0,
            },
            "bbo" | "liquidationMap" | "l4LiquidationMap" => Reducer::LastFrame(None),
            "allPrices" => Reducer::PriceMap { prices: HashMap::new() },
            _ => Reducer::Passthrough,
        }
    }

    /// Apply an upstream frame (already split into channel + data) to the state.
    pub fn apply(&mut self, raw: &str, channel: &str, data: &Value) {
        match self {
            Reducer::Levels { bids, asks, time, .. } => {
                if let Some(t) = data.get("time").and_then(|v| v.as_u64()) {
                    *time = t;
                }
                if let Some(levels) = data.get("levels").and_then(|v| v.as_array()) {
                    if let Some(side) = levels.first().and_then(|v| v.as_array()) {
                        apply_side(bids, side);
                    }
                    if let Some(side) = levels.get(1).and_then(|v| v.as_array()) {
                        apply_side(asks, side);
                    }
                }
            }
            Reducer::LastFrame(slot) => {
                let _ = channel;
                *slot = Some(raw.to_string());
            }
            Reducer::PriceMap { prices } => {
                if let Some(map) = data.as_object() {
                    for (coin, px) in map {
                        if let Some(px) = px.as_str() {
                            prices.insert(coin.clone(), px.to_string());
                        }
                    }
                }
            }
            Reducer::Passthrough => {}
        }
    }

    /// Reset state (used on upstream reconnect, before the fresh snapshot).
    pub fn reset(&mut self) {
        match self {
            Reducer::Levels { bids, asks, time, .. } => {
                bids.clear();
                asks.clear();
                *time = 0;
            }
            Reducer::LastFrame(slot) => *slot = None,
            Reducer::PriceMap { prices } => prices.clear(),
            Reducer::Passthrough => {}
        }
    }

    /// Serialize a full-state frame for a newly-joined client, or None if this
    /// channel has no bootstrap snapshot (client just joins the live stream).
    pub fn snapshot_frame(&self) -> Option<String> {
        match self {
            Reducer::Levels { channel, coin, n_sig_figs, n_levels, mantissa, bids, asks, time } => {
                // No state yet (first subscriber of a fresh group, before the
                // upstream snapshot arrived): skip the bootstrap; the forwarded
                // full frame serves as the snapshot instead.
                if bids.is_empty() && asks.is_empty() {
                    return None;
                }
                let mut bid_vec = sorted_levels(bids, true);
                let mut ask_vec = sorted_levels(asks, false);
                if let Some(n) = n_levels {
                    bid_vec.truncate(*n);
                    ask_vec.truncate(*n);
                }
                let mut data = json!({
                    "coin": coin,
                    "time": time,
                    "levels": [bid_vec, ask_vec],
                });
                if let Some(v) = n_sig_figs {
                    data["nSigFigs"] = v.clone();
                }
                if let Some(v) = mantissa {
                    data["mantissa"] = v.clone();
                }
                if let Some(n) = n_levels {
                    data["nLevels"] = json!(n);
                }
                Some(json!({"channel": channel, "data": data}).to_string())
            }
            Reducer::LastFrame(slot) => slot.clone(),
            Reducer::PriceMap { prices } => {
                if prices.is_empty() {
                    return None;
                }
                Some(json!({"channel": "allPrices", "data": prices}).to_string())
            }
            Reducer::Passthrough => None,
        }
    }
}

/// Apply one side's delta levels: sz=="0" removes, otherwise upsert by px.
fn apply_side(map: &mut HashMap<String, Value>, side: &[Value]) {
    for lvl in side {
        let px = match lvl.get("px").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => continue,
        };
        let sz = lvl.get("sz").and_then(|v| v.as_str()).unwrap_or("0");
        if sz == "0" {
            map.remove(&px);
        } else {
            map.insert(px, lvl.clone());
        }
    }
}

/// Sort levels by numeric price; bids descending, asks ascending.
fn sorted_levels(map: &HashMap<String, Value>, descending: bool) -> Vec<Value> {
    let mut v: Vec<(f64, &Value)> = map
        .iter()
        .map(|(px, lvl)| (px.parse::<f64>().unwrap_or(0.0), lvl))
        .collect();
    v.sort_by(|a, b| {
        if descending {
            b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal)
        }
    });
    v.into_iter().map(|(_, lvl)| lvl.clone()).collect()
}
