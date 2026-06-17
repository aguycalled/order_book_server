use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::{
    order_book::types::Side,
    types::node_data::{NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
};

pub(crate) mod inner;
pub(crate) mod node_data;
pub(crate) mod subscription;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Trade {
    pub coin: String,
    side: Side,
    px: String,
    sz: String,
    hash: String,
    time: u64,
    tid: u64,
    user: Address,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Level {
    px: String,
    sz: String,
    n: usize,
}

impl Level {
    pub(crate) const fn new(px: String, sz: String, n: usize) -> Self {
        Self { px, sz, n }
    }

    pub(crate) fn px(&self) -> &str {
        &self.px
    }

    pub(crate) fn sz(&self) -> &str {
        &self.sz
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L2Book {
    coin: String,
    time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_sig_figs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mantissa: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n_levels: Option<usize>,
    levels: [Vec<Level>; 2],
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum L4Book {
    Snapshot { coin: String, time: u64, height: u64, levels: [Vec<L4Order>; 2] },
    Updates(L4BookUpdates),
}

/// Aggregated trigger order book (stop losses / take profits)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TriggerBook {
    pub coin: String,
    pub time: u64,
    pub levels: [Vec<Level>; 2], // [bids, asks] — bucketed by trigger_px or limit_px
}

/// Best Bid/Offer - top of book only
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Bbo {
    pub coin: String,
    pub time: u64,
    pub bid: Option<Level>,
    pub ask: Option<Level>,
}

impl L2Book {
    pub const fn from_l2_snapshot(
        coin: String,
        snapshot: [Vec<Level>; 2],
        time: u64,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
        n_levels: Option<usize>,
    ) -> Self {
        Self { coin, time, n_sig_figs, mantissa, n_levels, levels: snapshot }
    }

    pub fn coin(&self) -> &str {
        &self.coin
    }

    pub const fn time(&self) -> u64 {
        self.time
    }

    pub const fn levels(&self) -> &[Vec<Level>; 2] {
        &self.levels
    }

    pub const fn n_sig_figs(&self) -> Option<u32> {
        self.n_sig_figs
    }

    pub const fn mantissa(&self) -> Option<u64> {
        self.mantissa
    }

    /// Re-bucket this L2Book to a different n_sig_figs/mantissa.
    /// The stored book must have finer resolution than the requested one.
    pub fn rebucket(&self, n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        use crate::order_book::{Px, Snapshot, Sz};
        use crate::types::inner::InnerLevel;

        // Convert Level -> InnerLevel
        let to_inner = |levels: &[Level]| -> Vec<InnerLevel> {
            levels
                .iter()
                .filter_map(|l| {
                    let px = Px::parse_from_str(&l.px).ok()?;
                    let sz = Sz::parse_from_str(&l.sz).ok()?;
                    Some(InnerLevel { px, sz, n: l.n })
                })
                .collect()
        };

        let inner_bids = to_inner(&self.levels[0]);
        let inner_asks = to_inner(&self.levels[1]);
        let snapshot = Snapshot::new([inner_bids, inner_asks]);
        let rebucketed = snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
        let levels = rebucketed.export_inner_snapshot();

        Self { coin: self.coin.clone(), time: self.time, n_sig_figs, mantissa, n_levels: None, levels }
    }
}

impl Trade {
    /// Create a trade from a single fill (raw broadcast without pairing)
    pub(crate) fn from_single_fill(fill: NodeDataFill) -> Self {
        let NodeDataFill(user, fill_data) = fill;
        Self {
            coin: fill_data.coin,
            side: fill_data.side,
            px: fill_data.px,
            sz: fill_data.sz,
            hash: fill_data.hash,
            time: fill_data.time,
            tid: fill_data.tid,
            user,
        }
    }

    pub(crate) fn px(&self) -> &str {
        &self.px
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct L4BookUpdates {
    pub time: u64,
    pub height: u64,
    pub order_statuses: Vec<NodeDataOrderStatus>,
    pub book_diffs: Vec<NodeDataOrderDiff>,
}

impl L4BookUpdates {
    pub(crate) const fn new(time: u64, height: u64) -> Self {
        Self { time, height, order_statuses: Vec::new(), book_diffs: Vec::new() }
    }
}

// RawL4Order is the version of a L4Order we want to serialize and deserialize directly
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct L4Order {
    // when serializing, this field is found outside of this struct
    // when deserializing, we move it into this struct
    pub user: Option<Address>,
    pub coin: String,
    pub side: Side,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    pub trigger_condition: String,
    pub is_trigger: bool,
    pub trigger_px: String,
    #[serde(default)]
    pub children: Vec<serde_json::Value>,
    pub is_position_tpsl: bool,
    pub reduce_only: bool,
    pub order_type: String,
    #[serde(default)]
    pub orig_sz: String,
    pub tif: Option<String>,
    pub cloid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OrderDiff {
    #[serde(rename_all = "camelCase")]
    New {
        sz: String,
    },
    #[serde(rename_all = "camelCase")]
    Update {
        orig_sz: String,
        new_sz: String,
    },
    Remove,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Fill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: Side,
    pub time: u64,
    pub start_position: String,
    pub dir: String,
    pub closed_pnl: String,
    pub hash: String,
    pub oid: u64,
    pub crossed: bool,
    pub fee: String,
    pub tid: u64,
    #[serde(default)]
    pub cloid: Option<String>,
    pub fee_token: String,
    #[serde(default)]
    pub twap_id: Option<u64>,
    pub liquidation: Option<Liquidation>,
    /// Builder address credited on this fill (HIP-3 / builder-code orders).
    /// Present directly on the fill — no order-status cache needed.
    #[serde(default)]
    pub builder: Option<String>,
    /// Exact builder fee we earned on this fill, quote-denominated decimal
    /// string (e.g. "0.003537"). Authoritative — no `notional × f` estimate.
    #[serde(default)]
    pub builder_fee: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Liquidation {
    pub liquidated_user: String,
    pub mark_px: String,
    pub method: String,
}

// ── Liquidation map types ───────────────────────────────────────────────

/// L2-style aggregated liquidation heatmap: price buckets with total size/count.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiquidationMapData {
    pub coin: String,
    pub time: u64,
    /// [longs_liquidated (below mark), shorts_liquidated (above mark)]
    pub levels: [Vec<LiquidationLevel>; 2],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LiquidationLevel {
    pub px: String,
    pub coin_sz: String,
    pub ntl_sz: String,
    pub n: usize,
}

/// L4 per-user liquidation detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L4LiquidationMapData {
    pub coin: String,
    pub time: u64,
    pub positions: Vec<L4LiquidationEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct L4LiquidationEntry {
    pub user: String,
    pub side: String,
    pub sz: String,
    pub entry_px: String,
    pub liq_px: String,
    pub leverage: String,
    pub margin_type: String,
}
