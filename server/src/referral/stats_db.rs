//! RocksDB-backed per-user volume/fee aggregates.
//!
//! Two key shapes, both JSON-serialized `UserStats` values:
//!   - 20 raw address bytes                        → cumulative (all-time) row
//!   - 20 address bytes + 4-byte big-endian day    → per-UTC-day row
//!
//! The day suffix is days since the Unix epoch. Big-endian keeps a user's
//! daily rows contiguous and date-ordered right after their cumulative row,
//! so a range report is one short prefix scan.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use alloy::primitives::Address;
use log::{error, warn};
use rocksdb::{DB, Options, WriteBatch};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct UserStats {
    /// Cumulative notional fill value (px * sz) as a 18-dp scaled u128-stringified
    /// to survive JSON roundtrips (serde_json loses precision past ~2^53).
    #[serde(with = "u128_str")]
    pub volume_quote_e8: u128,
    /// Gross fee sums (what the user paid, all components) keyed by
    /// `fee_token`. i128 covers negative (rebate) fees too.
    /// Stored as decimal strings for the same precision reason.
    pub fees_by_token: BTreeMap<String, String>,
    /// Builder fees WE earned, e8-scaled, quote-denominated (USDC):
    /// notional × f / 100_000 where `f` is tenths of a basis point.
    /// Only accumulated on fills whose order carries our builder address.
    #[serde(default, with = "u128_str")]
    pub builder_fees_quote_e8: u128,
    /// Notional volume from fills routed through OUR builder only — the
    /// "earning" volume. Distinct from `volume_quote_e8`, which also includes
    /// referral-set (HYBRIDGE) fills that carry no builder fee.
    #[serde(default, with = "u128_str")]
    pub builder_volume_quote_e8: u128,
    /// Referral rewards WE earned: reward_rate × gross fee, e8-scaled decimal
    /// strings keyed by `fee_token`. Only accumulated on fills by users in
    /// the tracked referral set.
    #[serde(default)]
    pub referral_fees_by_token: BTreeMap<String, String>,
    pub fill_count: u64,
    /// Unix-millis of the last fill we applied for this user.
    pub last_update_ms: u64,
    /// Whether the user qualifies via HYBRIDGE referral, builder-code, or both.
    /// Bitflags: 1 = referral match, 2 = builder match.
    pub match_flags: u8,
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

pub struct ReferralStatsDb {
    db: DB,
}

/// Cumulative-row key: 20 raw address bytes.
pub fn cum_key(addr: &Address) -> [u8; 20] {
    let mut k = [0u8; 20];
    k.copy_from_slice(addr.as_slice());
    k
}

/// Daily-row key: address bytes + big-endian days-since-epoch.
pub fn day_key(addr: &Address, day: u32) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[..20].copy_from_slice(addr.as_slice());
    k[20..].copy_from_slice(&day.to_be_bytes());
    k
}

impl ReferralStatsDb {
    pub fn open(path: PathBuf) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let db = crate::open_db_with_repair(&opts, &path)?;
        Ok(Self { db })
    }

    pub fn get(&self, addr: &Address) -> Option<UserStats> {
        self.get_raw(addr.as_slice())
    }

    pub fn get_raw(&self, key: &[u8]) -> Option<UserStats> {
        match self.db.get(key) {
            Ok(Some(bytes)) => match serde_json::from_slice::<UserStats>(&bytes) {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!("stats_db: deserialize error for key {key:02x?}: {e}");
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                error!("stats_db: get error for key {key:02x?}: {e}");
                None
            }
        }
    }

    pub fn write_batch(&self, updates: &[(Address, UserStats)]) {
        let raw: Vec<(Vec<u8>, UserStats)> = updates.iter().map(|(a, s)| (a.as_slice().to_vec(), s.clone())).collect();
        self.write_batch_raw(&raw);
    }

    pub fn write_batch_raw(&self, updates: &[(Vec<u8>, UserStats)]) {
        if updates.is_empty() {
            return;
        }
        let mut batch = WriteBatch::default();
        for (key, stats) in updates {
            match serde_json::to_vec(stats) {
                Ok(v) => batch.put(key, v),
                Err(e) => warn!("stats_db: serialize error for key {key:02x?}: {e}"),
            }
        }
        if let Err(e) = self.db.write(batch) {
            error!("stats_db: write_batch error: {e}");
        }
    }

    /// Daily rows for one user, inclusive day range (days since epoch).
    /// One prefix scan — daily keys for an address sort contiguously.
    pub fn user_daily(&self, addr: &Address, from_day: u32, to_day: u32) -> Vec<(u32, UserStats)> {
        let start = day_key(addr, from_day);
        let iter = self.db.iterator(rocksdb::IteratorMode::From(&start, rocksdb::Direction::Forward));
        let mut out = Vec::new();
        for item in iter {
            match item {
                Ok((k, v)) => {
                    if k.len() != 24 || k[..20] != addr.as_slice()[..] {
                        break;
                    }
                    let day = u32::from_be_bytes([k[20], k[21], k[22], k[23]]);
                    if day > to_day {
                        break;
                    }
                    match serde_json::from_slice::<UserStats>(&v) {
                        Ok(s) => out.push((day, s)),
                        Err(e) => warn!("stats_db: deserialize error for {addr} day {day}: {e}"),
                    }
                }
                Err(e) => {
                    error!("stats_db: iteration error: {e}");
                    break;
                }
            }
        }
        out
    }

    /// Full-scan top-N by volume. Acceptable while the tracked-user cardinality stays
    /// in the low thousands; revisit with a secondary index if it grows.
    pub fn top(&self, limit: usize) -> Vec<(Address, UserStats)> {
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        let mut all: Vec<(Address, UserStats)> = Vec::new();
        for item in iter {
            match item {
                Ok((k, v)) => {
                    if k.len() != 20 {
                        continue;
                    }
                    let mut bytes = [0u8; 20];
                    bytes.copy_from_slice(&k);
                    let addr = Address::from(bytes);
                    match serde_json::from_slice::<UserStats>(&v) {
                        Ok(s) => all.push((addr, s)),
                        Err(e) => warn!("stats_db: deserialize error for {addr}: {e}"),
                    }
                }
                Err(e) => {
                    error!("stats_db: iteration error: {e}");
                    break;
                }
            }
        }
        all.sort_by(|a, b| b.1.volume_quote_e8.cmp(&a.1.volume_quote_e8));
        all.truncate(limit);
        all
    }
}

pub type SharedReferralStatsDb = Arc<ReferralStatsDb>;

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(last: u8) -> Address {
        let mut b = [0u8; 20];
        b[19] = last;
        Address::from(b)
    }

    #[test]
    fn day_keys_sort_after_cum_key_and_by_date() {
        let a = addr(1);
        let cum = cum_key(&a).to_vec();
        let d1 = day_key(&a, 20_000).to_vec();
        let d2 = day_key(&a, 20_001).to_vec();
        let other = cum_key(&addr(2)).to_vec();
        assert!(cum < d1 && d1 < d2 && d2 < other);
    }

    #[test]
    fn daily_scan_respects_range_and_user_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let db = ReferralStatsDb::open(dir.path().join("t.rocksdb")).unwrap();
        let a = addr(1);
        let mut s = UserStats::default();
        s.fill_count = 7;
        db.write_batch_raw(&[
            (cum_key(&a).to_vec(), s.clone()),
            (day_key(&a, 100).to_vec(), s.clone()),
            (day_key(&a, 101).to_vec(), s.clone()),
            (day_key(&a, 105).to_vec(), s.clone()),
            (day_key(&addr(2), 101).to_vec(), s.clone()),
        ]);
        let rows = db.user_daily(&a, 100, 104);
        assert_eq!(rows.iter().map(|(d, _)| *d).collect::<Vec<_>>(), vec![100, 101]);
        // Old rows without the new fields still deserialize.
        let legacy = r#"{"volume_quote_e8":"5","fees_by_token":{},"fill_count":1,"last_update_ms":0,"match_flags":1}"#;
        let parsed: UserStats = serde_json::from_str(legacy).unwrap();
        assert_eq!(parsed.builder_fees_quote_e8, 0);
        assert!(parsed.referral_fees_by_token.is_empty());
    }
}
