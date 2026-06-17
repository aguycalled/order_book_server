//! Axum handlers for /stats/user/:address, /stats/user/:address/daily and
//! /stats/top.

use std::sync::Arc;

use alloy::primitives::Address;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::referral::{ReferralStatsDb, UserStats};

#[derive(Serialize)]
struct TopEntry {
    address: String,
    stats: UserStats,
}

#[derive(Deserialize)]
pub struct TopQuery {
    limit: Option<usize>,
}

pub async fn stats_user_handler(Path(addr): Path<String>, State(db): State<Arc<ReferralStatsDb>>) -> impl IntoResponse {
    let parsed: Address = match addr.parse() {
        Ok(a) => a,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid address").into_response(),
    };
    match db.get(&parsed) {
        Some(stats) => axum::response::Json(stats).into_response(),
        None => (StatusCode::NOT_FOUND, "user has no tracked fills").into_response(),
    }
}

#[derive(Deserialize)]
pub struct DailyQuery {
    /// Inclusive range bounds, YYYY-MM-DD (UTC). Defaults: last 30 days.
    from: Option<String>,
    to: Option<String>,
}

#[derive(Serialize)]
struct DailyEntry {
    date: String,
    stats: UserStats,
}

fn parse_epoch_day(s: &str) -> Option<u32> {
    let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    let days = (date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
    u32::try_from(days).ok()
}

fn epoch_day_to_date(day: u32) -> String {
    let date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Days::new(day as u64);
    date.format("%Y-%m-%d").to_string()
}

pub async fn stats_user_daily_handler(
    Path(addr): Path<String>,
    Query(q): Query<DailyQuery>,
    State(db): State<Arc<ReferralStatsDb>>,
) -> impl IntoResponse {
    let parsed: Address = match addr.parse() {
        Ok(a) => a,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid address").into_response(),
    };
    let today = (chrono::Utc::now().timestamp() / 86_400) as u32;
    let to_day = match q.to.as_deref() {
        Some(s) => match parse_epoch_day(s) {
            Some(d) => d,
            None => return (StatusCode::BAD_REQUEST, "invalid `to` date, want YYYY-MM-DD").into_response(),
        },
        None => today,
    };
    let from_day = match q.from.as_deref() {
        Some(s) => match parse_epoch_day(s) {
            Some(d) => d,
            None => return (StatusCode::BAD_REQUEST, "invalid `from` date, want YYYY-MM-DD").into_response(),
        },
        None => to_day.saturating_sub(29),
    };
    if from_day > to_day {
        return (StatusCode::BAD_REQUEST, "`from` is after `to`").into_response();
    }
    let rows = db.user_daily(&parsed, from_day, to_day);
    let out: Vec<DailyEntry> =
        rows.into_iter().map(|(day, stats)| DailyEntry { date: epoch_day_to_date(day), stats }).collect();
    axum::response::Json(out).into_response()
}

pub async fn stats_top_handler(Query(q): Query<TopQuery>, State(db): State<Arc<ReferralStatsDb>>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let rows = db.top(limit);
    let out: Vec<TopEntry> = rows.into_iter().map(|(a, s)| TopEntry { address: format!("{a:#x}"), stats: s }).collect();
    axum::response::Json(out).into_response()
}

/// One referee in a referral-accrual batch query.
#[derive(Deserialize)]
pub struct AccrualUser {
    address: String,
    /// Inclusive lower bound (YYYY-MM-DD, UTC) — the referee's `bound_at`. When
    /// set, only fills on/after this day count (sum of daily rows). When
    /// omitted, the user's all-time cumulative is used.
    #[serde(default)]
    from: Option<String>,
}

#[derive(Deserialize)]
pub struct AccrualBatch {
    users: Vec<AccrualUser>,
}

/// Per-referee builder-fee accrual: the exact builder fees we earned from this
/// user since `from`. This is the sole basis for referrer payouts (see
/// referral plan). Volume is returned for display only.
#[derive(Serialize)]
struct AccrualEntry {
    address: String,
    builder_fees_quote_e8: String,
    volume_quote_e8: String,
    fill_count: u64,
    last_update_ms: u64,
}

/// POST /stats/referral/accrual — batch lookup of per-user builder-fee accrual
/// from each user's `bound_at`. Body: `{ "users": [{ "address", "from"? }] }`.
/// Unknown / untracked addresses return zeros (not an error) so the caller can
/// map results 1:1.
pub async fn stats_referral_accrual_handler(
    State(db): State<Arc<ReferralStatsDb>>,
    axum::Json(batch): axum::Json<AccrualBatch>,
) -> impl IntoResponse {
    const MAX_USERS: usize = 1000;
    if batch.users.len() > MAX_USERS {
        return (StatusCode::BAD_REQUEST, "too many users (max 1000)").into_response();
    }
    let today = (chrono::Utc::now().timestamp() / 86_400) as u32;

    let mut out: Vec<AccrualEntry> = Vec::with_capacity(batch.users.len());
    for u in &batch.users {
        let Ok(addr) = u.address.parse::<Address>() else {
            return (StatusCode::BAD_REQUEST, format!("invalid address: {}", u.address)).into_response();
        };

        let mut builder_fees: u128 = 0;
        let mut volume: u128 = 0;
        let mut fill_count: u64 = 0;
        let mut last_update_ms: u64 = 0;

        match u.from.as_deref() {
            // Sum daily rows from `from`..today inclusive.
            Some(s) => {
                let Some(from_day) = parse_epoch_day(s) else {
                    return (StatusCode::BAD_REQUEST, format!("invalid `from` date: {s}")).into_response();
                };
                if from_day <= today {
                    for (_day, stats) in db.user_daily(&addr, from_day, today) {
                        builder_fees = builder_fees.saturating_add(stats.builder_fees_quote_e8);
                        volume = volume.saturating_add(stats.volume_quote_e8);
                        fill_count = fill_count.saturating_add(stats.fill_count);
                        last_update_ms = last_update_ms.max(stats.last_update_ms);
                    }
                }
            }
            // All-time cumulative row.
            None => {
                if let Some(stats) = db.get(&addr) {
                    builder_fees = stats.builder_fees_quote_e8;
                    volume = stats.volume_quote_e8;
                    fill_count = stats.fill_count;
                    last_update_ms = stats.last_update_ms;
                }
            }
        }

        out.push(AccrualEntry {
            address: format!("{addr:#x}"),
            builder_fees_quote_e8: builder_fees.to_string(),
            volume_quote_e8: volume.to_string(),
            fill_count,
            last_update_ms,
        });
    }

    axum::response::Json(out).into_response()
}
