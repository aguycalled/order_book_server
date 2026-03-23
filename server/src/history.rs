use crate::{
    listeners::order_book::{L2SnapshotParams, OrderBookListener},
    types::L2Book,
};
use axum::{
    extract::{Query, State},
    response::IntoResponse,
};
use log::{error, info};
use rocksdb::{DB, Options, WriteBatch};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

/// RocksDB-backed L2 order book history store.
pub struct L2History {
    db: DB,
}

impl L2History {
    pub fn open(path: PathBuf) -> Result<Self, rocksdb::Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let db = DB::open(&opts, path)?;
        Ok(Self { db })
    }

    /// Record L2 snapshots for all coins at the current moment.
    /// Stores at n_sig_figs=5 (finest bucketed resolution), no level cap.
    pub(crate) async fn record_snapshots(&self, listener: &Arc<Mutex<OrderBookListener>>) {
        let data = {
            let listener = listener.lock().await;
            listener.l2_snapshots()
        };

        let (time, l2_snapshots) = match data {
            Some(d) => d,
            None => {
                info!("History: order book not ready, skipping snapshot");
                return;
            }
        };

        let params = L2SnapshotParams::new(Some(5), None);
        let mut batch = WriteBatch::default();
        let mut count = 0u32;

        for (coin, param_map) in l2_snapshots.as_ref() {
            if let Some(snapshot) = param_map.get(&params) {
                let levels = snapshot.clone().export_inner_snapshot();
                let l2_book = L2Book::from_l2_snapshot(
                    coin.value(),
                    levels,
                    time,
                    Some(5),
                    None,
                    None,
                );
                let key = make_key(&coin.value(), time);
                match serde_json::to_vec(&l2_book) {
                    Ok(value) => {
                        batch.put(&key, &value);
                        count += 1;
                    }
                    Err(e) => {
                        error!("History: failed to serialize L2Book for {}: {}", coin.value(), e);
                    }
                }
            }
        }

        if count > 0 {
            match self.db.write(batch) {
                Ok(()) => info!("History: recorded {} L2 snapshots at time {}", count, time),
                Err(e) => error!("History: RocksDB write failed: {}", e),
            }
        }
    }

    /// Query L2 snapshots for a coin within a time range.
    /// Returns snapshots ordered by time ascending.
    pub fn query(
        &self,
        coin: &str,
        start: u64,
        end: u64,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Vec<L2Book> {
        let start_key = make_key(coin, start);
        let end_key = make_key(coin, end + 1); // exclusive upper bound

        let mut results = Vec::new();
        let prefix = make_prefix(coin);

        let iter = self.db.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));

        for item in iter {
            match item {
                Ok((key, value)) => {
                    // Stop if we've passed the end key or left this coin's prefix
                    if key.as_ref() >= end_key.as_slice() || !key.starts_with(&prefix) {
                        break;
                    }
                    match serde_json::from_slice::<L2Book>(&value) {
                        Ok(book) => {
                            // Re-bucket if requested n_sig_figs differs from stored (5)
                            let book = match n_sig_figs {
                                Some(n) if n < 5 => book.rebucket(Some(n), mantissa),
                                _ => book,
                            };
                            results.push(book);
                        }
                        Err(e) => {
                            error!("History: failed to deserialize L2Book: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("History: RocksDB iteration error: {}", e);
                    break;
                }
            }
        }

        results
    }
}

/// Key format: <coin>\x00<timestamp_ms as big-endian u64>
fn make_key(coin: &str, time: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(coin.len() + 1 + 8);
    key.extend_from_slice(coin.as_bytes());
    key.push(0x00);
    key.extend_from_slice(&time.to_be_bytes());
    key
}

/// Prefix for a coin's keys (used to bound iteration)
fn make_prefix(coin: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(coin.len() + 1);
    prefix.extend_from_slice(coin.as_bytes());
    prefix.push(0x00);
    prefix
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    coin: String,
    start: u64,
    end: u64,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

pub async fn history_handler(
    Query(params): Query<HistoryQuery>,
    State(history): State<Arc<L2History>>,
) -> impl IntoResponse {
    let results = history.query(
        &params.coin,
        params.start,
        params.end,
        params.n_sig_figs,
        params.mantissa,
    );

    axum::response::Json(results)
}
