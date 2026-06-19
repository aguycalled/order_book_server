//! RocksDB-backed per-strategy volume/fee aggregates.
//!
//! Two key shapes, both JSON-serialized `StrategyStats` values:
//!   - 8 raw stratKey bytes                        → cumulative (all-time) row
//!   - 8 stratKey bytes + 4-byte big-endian day    → per-UTC-day row
//!
//! The day suffix is days since the Unix epoch. Big-endian keeps a strategy's
//! daily rows contiguous and date-ordered right after its cumulative row, so a
//! rolling-window report (24h/7d/30d) is one short prefix scan.

use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use log::{error, warn};
use rocksdb::{DB, Options, WriteBatch};
use serde::{Deserialize, Serialize};

/// A decoded strategy key: the 8-byte (16 hex) strategy id from a Borsa cloid.
pub type StratKey = [u8; 8];

/// Scale factor for volume/fee storage. px*sz as floats → u128 scaled by 1e8.
const VOLUME_SCALE: f64 = 1e8;

/// Decode the strategy key from a Borsa client order id (cloid).
///
/// Returns `Some(stratKey)` only when the cloid is a well-formed Borsa cloid:
/// `0x`-prefixed (optional), exactly 32 hex chars, and the first 4 hex chars
/// (the 2-byte magic) equal `b05a`. The stratKey is hex chars [6..22].
pub fn decode_strategy_cloid(cloid: &str) -> Option<StratKey> {
    let hex = cloid.strip_prefix("0x").or_else(|| cloid.strip_prefix("0X")).unwrap_or(cloid);
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    // Magic check: first 2 bytes (4 hex chars) must be `b05a`, case-insensitive.
    if !hex[0..4].eq_ignore_ascii_case("b05a") {
        return None;
    }
    // stratKey is the 8 bytes at hex chars [6..22] (lowercased, matchable
    // against the strategy's marketplace versionHash digest).
    let key_hex = hex[6..22].to_ascii_lowercase();
    let mut key = [0u8; 8];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&key_hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(key)
}

/// Parse a 16-hex stratKey string (as served on the wire / used in URLs) into
/// raw bytes. Accepts an optional `0x` prefix; rejects any other length.
pub fn decode_strat_key_hex(hex: &str) -> Option<StratKey> {
    let hex = hex.strip_prefix("0x").or_else(|| hex.strip_prefix("0X")).unwrap_or(hex);
    if hex.len() != 16 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut key = [0u8; 8];
    for (i, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(key)
}

/// Lowercase 16-hex rendering of a stratKey (the canonical wire form).
pub fn strat_key_hex(key: &StratKey) -> String {
    let mut s = String::with_capacity(16);
    for b in key {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StrategyStats {
    /// Cumulative notional fill value (px * sz), e8-scaled, stringified to
    /// survive JSON roundtrips (serde_json loses precision past ~2^53).
    #[serde(with = "u128_str")]
    pub volume_quote_e8: u128,
    /// Builder fees WE earned, e8-scaled, quote-denominated (USDC):
    /// notional × f / 100_000 where `f` is tenths of a basis point. Only
    /// accumulated on fills whose order carries our builder address.
    #[serde(default, with = "u128_str")]
    pub builder_fees_quote_e8: u128,
    pub fill_count: u64,
    /// Distinct filling-user addresses (lowercase `0x…`). Kept only on the
    /// cumulative row; daily rows leave it empty to stay compact.
    #[serde(default)]
    pub unique_users: BTreeSet<String>,
    /// Unix-millis of the first fill we applied for this strategy (0 = unset).
    #[serde(default)]
    pub first_update_ms: u64,
    /// Unix-millis of the last fill we applied for this strategy.
    pub last_update_ms: u64,
}

impl StrategyStats {
    /// Add a single fill's contribution. `track_users` is false for daily rows
    /// (we keep the unique-user set only on the cumulative row).
    pub fn accumulate(
        &mut self,
        notional_e8: u128,
        builder_fee_e8: u128,
        user_lower: &str,
        block_ms: u64,
        track_users: bool,
    ) {
        self.volume_quote_e8 = self.volume_quote_e8.saturating_add(notional_e8);
        self.builder_fees_quote_e8 = self.builder_fees_quote_e8.saturating_add(builder_fee_e8);
        self.fill_count = self.fill_count.saturating_add(1);
        if track_users {
            self.unique_users.insert(user_lower.to_string());
        }
        if self.first_update_ms == 0 || block_ms < self.first_update_ms {
            self.first_update_ms = block_ms;
        }
        if block_ms > self.last_update_ms {
            self.last_update_ms = block_ms;
        }
    }
}

/// Convert an e8-scaled u128 back to a plain f64 USD value.
pub fn e8_to_f64(v: u128) -> f64 {
    v as f64 / VOLUME_SCALE
}

mod u128_str {
    use serde::{Deserialize, Deserializer, Serializer};
    pub(super) fn serialize<S: Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }
    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

pub struct StrategyStatsDb {
    db: DB,
}

/// Cumulative-row key: 8 raw stratKey bytes.
pub fn cum_key(key: &StratKey) -> [u8; 8] {
    *key
}

/// Daily-row key: stratKey bytes + big-endian days-since-epoch.
pub fn day_key(key: &StratKey, day: u32) -> [u8; 12] {
    let mut k = [0u8; 12];
    k[..8].copy_from_slice(key);
    k[8..].copy_from_slice(&day.to_be_bytes());
    k
}

impl StrategyStatsDb {
    pub fn open(path: PathBuf) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let db = crate::open_db_with_repair(&opts, &path)?;
        Ok(Self { db })
    }

    pub fn get(&self, key: &StratKey) -> Option<StrategyStats> {
        self.get_raw(&cum_key(key))
    }

    pub fn get_raw(&self, key: &[u8]) -> Option<StrategyStats> {
        match self.db.get(key) {
            Ok(Some(bytes)) => match serde_json::from_slice::<StrategyStats>(&bytes) {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!("strategy_stats_db: deserialize error for key {key:02x?}: {e}");
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                error!("strategy_stats_db: get error for key {key:02x?}: {e}");
                None
            }
        }
    }

    pub fn write_batch_raw(&self, updates: &[(Vec<u8>, StrategyStats)]) {
        if updates.is_empty() {
            return;
        }
        let mut batch = WriteBatch::default();
        for (key, stats) in updates {
            match serde_json::to_vec(stats) {
                Ok(v) => batch.put(key, v),
                Err(e) => warn!("strategy_stats_db: serialize error for key {key:02x?}: {e}"),
            }
        }
        if let Err(e) = self.db.write(batch) {
            error!("strategy_stats_db: write_batch error: {e}");
        }
    }

    /// Daily rows for one strategy, inclusive day range (days since epoch).
    /// One prefix scan — daily keys for a stratKey sort contiguously.
    pub fn strategy_daily(&self, key: &StratKey, from_day: u32, to_day: u32) -> Vec<(u32, StrategyStats)> {
        let start = day_key(key, from_day);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward));
        let mut out = Vec::new();
        for item in iter {
            match item {
                Ok((k, v)) => {
                    if k.len() != 12 || k[..8] != key[..] {
                        break;
                    }
                    let day = u32::from_be_bytes([k[8], k[9], k[10], k[11]]);
                    if day > to_day {
                        break;
                    }
                    match serde_json::from_slice::<StrategyStats>(&v) {
                        Ok(s) => out.push((day, s)),
                        Err(e) => warn!("strategy_stats_db: deserialize error for day {day}: {e}"),
                    }
                }
                Err(e) => {
                    error!("strategy_stats_db: iteration error: {e}");
                    break;
                }
            }
        }
        out
    }
}

pub type SharedStrategyStatsDb = Arc<StrategyStatsDb>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_valid_cloid() {
        // magic b05a, version 01, stratKey = 0011223344556677, nonce = 8899aabbcc
        let cloid = "0xb05a010011223344556677008899aabb";
        let key = decode_strategy_cloid(cloid).expect("valid borsa cloid");
        assert_eq!(key, [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77]);
        assert_eq!(strat_key_hex(&key), "0011223344556677");
    }

    #[test]
    fn decode_accepts_no_0x_prefix_and_uppercase_magic() {
        let lower = decode_strategy_cloid("b05a01aabbccddeeff0011deadbeefca");
        let upper = decode_strategy_cloid("B05A01AABBCCDDEEFF0011DEADBEEFCA");
        assert!(lower.is_some());
        assert_eq!(lower, upper);
        assert_eq!(strat_key_hex(&lower.unwrap()), "aabbccddeeff0011");
    }

    #[test]
    fn decode_rejects_wrong_magic() {
        // Same length/shape but magic is dead, not b05a.
        assert_eq!(decode_strategy_cloid("0xdead010011223344556677008899aabb"), None);
    }

    #[test]
    fn decode_rejects_bad_length_and_nonhex() {
        assert_eq!(decode_strategy_cloid("0xb05a01"), None); // too short
        assert_eq!(decode_strategy_cloid("0xb05a010011223344556677008899aabbcc"), None); // too long
        assert_eq!(decode_strategy_cloid("0xb05a01zz11223344556677008899aabb"), None); // non-hex
        assert_eq!(decode_strategy_cloid(""), None);
    }

    #[test]
    fn day_keys_sort_after_cum_key_and_by_date() {
        let k: StratKey = [1, 2, 3, 4, 5, 6, 7, 8];
        let other: StratKey = [9, 0, 0, 0, 0, 0, 0, 0];
        let cum = cum_key(&k).to_vec();
        let d1 = day_key(&k, 20_000).to_vec();
        let d2 = day_key(&k, 20_001).to_vec();
        let other_cum = cum_key(&other).to_vec();
        assert!(cum < d1 && d1 < d2 && d2 < other_cum);
    }

    #[test]
    fn daily_scan_respects_range_and_strategy_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let db = StrategyStatsDb::open(dir.path().join("t.rocksdb")).unwrap();
        let k: StratKey = [1; 8];
        let k2: StratKey = [2; 8];
        let mut s = StrategyStats::default();
        s.fill_count = 7;
        db.write_batch_raw(&[
            (cum_key(&k).to_vec(), s.clone()),
            (day_key(&k, 100).to_vec(), s.clone()),
            (day_key(&k, 101).to_vec(), s.clone()),
            (day_key(&k, 105).to_vec(), s.clone()),
            (day_key(&k2, 101).to_vec(), s.clone()),
        ]);
        let rows = db.strategy_daily(&k, 100, 104);
        assert_eq!(rows.iter().map(|(d, _)| *d).collect::<Vec<_>>(), vec![100, 101]);
        // Old rows without the newer fields still deserialize.
        let legacy = r#"{"volume_quote_e8":"5","fill_count":1,"last_update_ms":0}"#;
        let parsed: StrategyStats = serde_json::from_str(legacy).unwrap();
        assert_eq!(parsed.builder_fees_quote_e8, 0);
        assert!(parsed.unique_users.is_empty());
        assert_eq!(parsed.first_update_ms, 0);
    }

    #[test]
    fn accumulate_tracks_users_and_first_last() {
        let mut s = StrategyStats::default();
        s.accumulate(100, 1, "0xaaa", 2000, true);
        s.accumulate(200, 2, "0xbbb", 1000, true);
        s.accumulate(50, 0, "0xaaa", 3000, true);
        assert_eq!(s.volume_quote_e8, 350);
        assert_eq!(s.builder_fees_quote_e8, 3);
        assert_eq!(s.fill_count, 3);
        assert_eq!(s.unique_users.len(), 2);
        assert_eq!(s.first_update_ms, 1000);
        assert_eq!(s.last_update_ms, 3000);
    }
}
