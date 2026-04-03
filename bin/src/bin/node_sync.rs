#![allow(unused_crate_dependencies)]

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::get,
};
use clap::Parser;
use reqwest::Client;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};

const DEFAULT_NODE_INFO_URL: &str = "http://localhost:3001/info";
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8787";
const DEFAULT_DB_PATH: &str = "./node_sync_stats.sqlite3";
const ZERO_ADDR: &str = "0x0000000000000000000000000000000000000000";

#[derive(Debug, Clone, Parser)]
#[command(about = "Poll node sync status, persist it to SQLite, and serve a local dashboard")]
struct Args {
    /// Node /info endpoint
    #[arg(long, default_value = DEFAULT_NODE_INFO_URL)]
    node_info_url: String,

    /// Poll interval in seconds
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    interval_secs: u64,

    /// SQLite database path
    #[arg(long, default_value = DEFAULT_DB_PATH)]
    db_path: PathBuf,

    /// Dashboard bind address
    #[arg(long, default_value = DEFAULT_BIND_ADDR)]
    bind: String,
}

#[derive(Debug, Clone)]
struct AppState {
    db_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
struct SampleRow {
    collected_at_ms: i64,
    block_time_ms: Option<i64>,
    lag_seconds: Option<i64>,
    mark_px: Option<f64>,
    advance_ms: Option<i64>,
    status: String,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct ResolvedSeriesQuery {
    minutes: u32,
    limit: u32,
}

#[derive(Debug, Deserialize)]
struct SeriesQuery {
    minutes: Option<u32>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct SeriesSummary {
    window_minutes: u32,
    total_samples: usize,
    ok_samples: usize,
    down_samples: usize,
    avg_lag_seconds: Option<f64>,
    p95_lag_seconds: Option<f64>,
    max_lag_seconds: Option<i64>,
    latest_status: Option<String>,
    latest_lag_seconds: Option<i64>,
    latest_mark_px: Option<f64>,
    latest_error: Option<String>,
    latest_collected_at_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SeriesResponse {
    summary: SeriesSummary,
    samples: Vec<SampleRow>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(err) = run(args).await {
        eprintln!("node_sync failed: {err}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    initialize_db(&args.db_path)?;

    let bind_addr: SocketAddr = args
        .bind
        .parse()
        .map_err(|err| format!("invalid --bind '{}': {err}", args.bind))?;

    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|err| format!("failed to build reqwest client: {err}"))?;

    let app_state = AppState { db_path: args.db_path.clone() };
    let router = Router::new()
        .route("/", get(index_handler))
        .route("/api/stats/latest", get(latest_handler))
        .route("/api/stats/series", get(series_handler))
        .with_state(app_state);

    println!(
        "Polling {} every {}s | SQLite: {} | Dashboard: http://{}",
        args.node_info_url,
        args.interval_secs,
        args.db_path.display(),
        bind_addr,
    );

    let poller_args = args.clone();
    drop(tokio::spawn(async move {
        poll_loop(poller_args, client).await;
    }));

    let listener = tokio::net::TcpListener::bind(bind_addr)
        .await
        .map_err(|err| format!("failed to bind {bind_addr}: {err}"))?;
    axum::serve(listener, router)
        .await
        .map_err(|err| format!("http server error: {err}"))
}

async fn poll_loop(args: Args, client: Client) {
    let mut prev_block_time: Option<u64> = None;

    loop {
        let now_ms = now_ms();

        match fetch_status(&client, &args.node_info_url).await {
            Ok((block_time_ms, mark_px)) => {
                let lag_s = now_ms.saturating_sub(block_time_ms) / 1_000;
                let status = classify_status(lag_s).to_string();

                let (advance_ms, rate_text) = if let Some(prev) = prev_block_time {
                    if block_time_ms > prev {
                        let advanced = block_time_ms - prev;
                        (Some(to_i64(advanced)), format!("{:.1}s advance", advanced as f64 / 1_000.0))
                    } else {
                        (Some(0), "stalled".to_string())
                    }
                } else {
                    (None, "first check".to_string())
                };

                let sample = SampleRow {
                    collected_at_ms: to_i64(now_ms),
                    block_time_ms: Some(to_i64(block_time_ms)),
                    lag_seconds: Some(to_i64(lag_s)),
                    mark_px,
                    advance_ms,
                    status: status.clone(),
                    error: None,
                };
                persist_sample(args.db_path.clone(), sample).await;

                let mark_display = mark_px.map_or_else(|| "?".to_string(), |px| format!("{px:.2}"));
                println!(
                    "[{}] {} | lag: {:>4}s | block: {} | BTC: {} | {}",
                    fmt_time(now_ms),
                    colored_status(&status),
                    lag_s,
                    fmt_time(block_time_ms),
                    mark_display,
                    rate_text,
                );

                prev_block_time = Some(block_time_ms);
            }
            Err(err) => {
                let sample = SampleRow {
                    collected_at_ms: to_i64(now_ms),
                    block_time_ms: None,
                    lag_seconds: None,
                    mark_px: None,
                    advance_ms: None,
                    status: "DOWN".to_string(),
                    error: Some(err.clone()),
                };
                persist_sample(args.db_path.clone(), sample).await;

                println!(
                    "[{}] \x1b[31;1mDOWN\x1b[0m       | {}",
                    fmt_time(now_ms),
                    err,
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }
}

fn classify_status(lag_s: u64) -> &'static str {
    if lag_s < 5 {
        "SYNCED"
    } else if lag_s < 30 {
        "BEHIND"
    } else if lag_s < 120 {
        "LAGGING"
    } else {
        "FAR_BEHIND"
    }
}

fn colored_status(status: &str) -> &'static str {
    match status {
        "SYNCED" => "\x1b[32mSYNCED\x1b[0m    ",
        "BEHIND" => "\x1b[33mBEHIND\x1b[0m    ",
        "LAGGING" => "\x1b[31mLAGGING\x1b[0m   ",
        "FAR_BEHIND" => "\x1b[31;1mFAR BEHIND\x1b[0m",
        _ => "\x1b[36mUNKNOWN\x1b[0m   ",
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u128::from(u64::MAX)) as u64)
}

fn to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn now_ms_i64() -> i64 {
    to_i64(now_ms())
}

fn fmt_time(ms: u64) -> String {
    let secs = ms / 1_000;
    let h = (secs / 3_600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

async fn fetch_status(client: &Client, node_info_url: &str) -> Result<(u64, Option<f64>), String> {
    // clearinghouseState returns node's block time
    let body = serde_json::json!({
        "type": "clearinghouseState",
        "user": ZERO_ADDR,
    });
    let resp: serde_json::Value = client
        .post(node_info_url)
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("request: {err}"))?
        .error_for_status()
        .map_err(|err| format!("request status: {err}"))?
        .json()
        .await
        .map_err(|err| format!("json: {err}"))?;

    let block_time_ms = resp
        .get("time")
        .and_then(serde_json::Value::as_u64)
        .ok_or("missing or invalid 'time' field".to_string())?;

    // activeAssetData returns current mark price
    let body = serde_json::json!({
        "type": "activeAssetData",
        "user": ZERO_ADDR,
        "coin": "BTC",
    });
    let resp: serde_json::Value = client
        .post(node_info_url)
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("mark request: {err}"))?
        .error_for_status()
        .map_err(|err| format!("mark request status: {err}"))?
        .json()
        .await
        .map_err(|err| format!("mark json: {err}"))?;

    let mark_px = resp.get("markPx").and_then(|value| {
        if let Some(px) = value.as_f64() {
            Some(px)
        } else {
            value.as_str().and_then(|px| px.parse::<f64>().ok())
        }
    });

    Ok((block_time_ms, mark_px))
}

fn open_connection(db_path: &Path) -> Result<Connection, String> {
    let conn = Connection::open(db_path)
        .map_err(|err| format!("failed to open sqlite db '{}': {err}", db_path.display()))?;
    conn.busy_timeout(Duration::from_secs(2))
        .map_err(|err| format!("failed to set sqlite busy_timeout: {err}"))?;
    Ok(conn)
}

fn initialize_db(db_path: &Path) -> Result<(), String> {
    let conn = open_connection(db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         CREATE TABLE IF NOT EXISTS samples (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             collected_at_ms INTEGER NOT NULL,
             block_time_ms INTEGER,
             lag_seconds INTEGER,
             mark_px REAL,
             advance_ms INTEGER,
             status TEXT NOT NULL,
             error TEXT
         );
         CREATE INDEX IF NOT EXISTS idx_samples_collected_at_ms
             ON samples (collected_at_ms);",
    )
    .map_err(|err| format!("failed to initialize schema: {err}"))?;
    Ok(())
}

fn insert_sample(db_path: &Path, sample: &SampleRow) -> Result<(), String> {
    let conn = open_connection(db_path)?;
    conn.execute(
        "INSERT INTO samples (
             collected_at_ms, block_time_ms, lag_seconds, mark_px, advance_ms, status, error
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            sample.collected_at_ms,
            sample.block_time_ms,
            sample.lag_seconds,
            sample.mark_px,
            sample.advance_ms,
            sample.status,
            sample.error
        ],
    )
    .map_err(|err| format!("failed to insert sample: {err}"))?;
    Ok(())
}

fn parse_sample_row(row: &Row<'_>) -> rusqlite::Result<SampleRow> {
    Ok(SampleRow {
        collected_at_ms: row.get(0)?,
        block_time_ms: row.get(1)?,
        lag_seconds: row.get(2)?,
        mark_px: row.get(3)?,
        advance_ms: row.get(4)?,
        status: row.get(5)?,
        error: row.get(6)?,
    })
}

fn load_latest_sample(db_path: &Path) -> Result<Option<SampleRow>, String> {
    let conn = open_connection(db_path)?;
    conn.query_row(
        "SELECT collected_at_ms, block_time_ms, lag_seconds, mark_px, advance_ms, status, error
         FROM samples
         ORDER BY collected_at_ms DESC
         LIMIT 1",
        [],
        parse_sample_row,
    )
    .optional()
    .map_err(|err| format!("failed to query latest sample: {err}"))
}

fn resolve_series_query(query: SeriesQuery) -> ResolvedSeriesQuery {
    let minutes = query.minutes.unwrap_or(60).clamp(1, 43_200);
    let limit = query.limit.unwrap_or(2_000).clamp(50, 10_000);
    ResolvedSeriesQuery { minutes, limit }
}

fn load_series(db_path: &Path, query: ResolvedSeriesQuery) -> Result<Vec<SampleRow>, String> {
    let conn = open_connection(db_path)?;
    let cutoff_ms = now_ms_i64().saturating_sub(i64::from(query.minutes) * 60_000);
    let mut stmt = conn
        .prepare(
            "SELECT collected_at_ms, block_time_ms, lag_seconds, mark_px, advance_ms, status, error
             FROM samples
             WHERE collected_at_ms >= ?1
             ORDER BY collected_at_ms ASC
             LIMIT ?2",
        )
        .map_err(|err| format!("failed to prepare series query: {err}"))?;

    let mut rows = stmt
        .query(params![cutoff_ms, i64::from(query.limit)])
        .map_err(|err| format!("failed to execute series query: {err}"))?;

    let mut samples = Vec::new();
    while let Some(row) = rows.next().map_err(|err| format!("failed to iterate rows: {err}"))? {
        samples.push(parse_sample_row(row).map_err(|err| format!("failed to parse row: {err}"))?);
    }
    Ok(samples)
}

fn build_summary(window_minutes: u32, samples: &[SampleRow]) -> SeriesSummary {
    let mut lags: Vec<i64> = samples.iter().filter_map(|sample| sample.lag_seconds).collect();
    lags.sort_unstable();

    let avg_lag_seconds = if lags.is_empty() {
        None
    } else {
        Some(lags.iter().sum::<i64>() as f64 / lags.len() as f64)
    };

    let p95_lag_seconds = if lags.is_empty() {
        None
    } else {
        let idx = ((lags.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
        lags.get(idx).map(|v| *v as f64)
    };

    let latest = samples.last();
    let down_samples = samples.iter().filter(|sample| sample.status == "DOWN").count();
    let ok_samples = samples.len().saturating_sub(down_samples);

    SeriesSummary {
        window_minutes,
        total_samples: samples.len(),
        ok_samples,
        down_samples,
        avg_lag_seconds,
        p95_lag_seconds,
        max_lag_seconds: lags.last().copied(),
        latest_status: latest.map(|sample| sample.status.clone()),
        latest_lag_seconds: latest.and_then(|sample| sample.lag_seconds),
        latest_mark_px: latest.and_then(|sample| sample.mark_px),
        latest_error: latest.and_then(|sample| sample.error.clone()),
        latest_collected_at_ms: latest.map(|sample| sample.collected_at_ms),
    }
}

async fn persist_sample(db_path: PathBuf, sample: SampleRow) {
    let result = tokio::task::spawn_blocking(move || insert_sample(&db_path, &sample)).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => eprintln!("sqlite insert failed: {err}"),
        Err(err) => eprintln!("sqlite worker failed: {err}"),
    }
}

async fn index_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn latest_handler(State(state): State<AppState>) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    match tokio::task::spawn_blocking(move || load_latest_sample(&db_path)).await {
        Ok(Ok(Some(sample))) => (StatusCode::OK, Json(sample)).into_response(),
        Ok(Ok(None)) => (StatusCode::NOT_FOUND, "no samples yet".to_string()).into_response(),
        Ok(Err(err)) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("worker join error: {err}")).into_response(),
    }
}

async fn series_handler(
    State(state): State<AppState>,
    Query(query): Query<SeriesQuery>,
) -> impl IntoResponse {
    let query = resolve_series_query(query);
    let db_path = state.db_path.clone();

    match tokio::task::spawn_blocking(move || load_series(&db_path, query)).await {
        Ok(Ok(samples)) => {
            let response = SeriesResponse {
                summary: build_summary(query.minutes, &samples),
                samples,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(Err(err)) => (StatusCode::INTERNAL_SERVER_ERROR, err).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("worker join error: {err}")).into_response(),
    }
}

const INDEX_HTML: &str = r###"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Node Sync Dashboard</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
    :root {
      --bg: #f3f7fb;
      --bg-alt: #e8f0fb;
      --card: #ffffff;
      --ink: #0f2236;
      --muted: #5e7287;
      --line: #d2dfed;
      --accent: #0d9488;
      --ok: #0f9d58;
      --warn: #f59e0b;
      --bad: #dc2626;
      --down: #4b5563;
      --radius: 16px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at 8% 12%, #d7e8ff 0%, transparent 36%),
        radial-gradient(circle at 92% 10%, #dbfff6 0%, transparent 30%),
        linear-gradient(145deg, var(--bg), var(--bg-alt));
      min-height: 100vh;
    }
    .page {
      width: min(1160px, 96vw);
      margin: 22px auto 32px;
      display: grid;
      gap: 14px;
    }
    .hero {
      background: color-mix(in oklab, var(--card) 88%, #b7e0d8 12%);
      border: 1px solid var(--line);
      border-radius: calc(var(--radius) + 4px);
      box-shadow: 0 12px 32px rgba(15, 34, 54, 0.08);
      padding: 18px 18px 14px;
      display: flex;
      flex-wrap: wrap;
      align-items: end;
      justify-content: space-between;
      gap: 12px;
    }
    h1 {
      margin: 0;
      font-size: clamp(1.15rem, 2.2vw, 1.7rem);
      font-weight: 700;
      letter-spacing: 0.01em;
    }
    .subtitle {
      margin: 5px 0 0;
      color: var(--muted);
      font-size: 0.93rem;
    }
    .controls {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    .controls label {
      color: var(--muted);
      font-size: 0.86rem;
    }
    select, button {
      border-radius: 10px;
      border: 1px solid #bfd0e5;
      background: #ffffff;
      color: var(--ink);
      font: inherit;
      font-size: 0.9rem;
      padding: 8px 10px;
    }
    button {
      background: #0d9488;
      color: white;
      border-color: #0d9488;
      cursor: pointer;
      font-weight: 600;
    }
    .cards {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 12px 14px;
      min-height: 102px;
      box-shadow: 0 4px 14px rgba(15, 34, 54, 0.05);
    }
    .label {
      font-size: 0.78rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: var(--muted);
      margin: 0 0 8px;
    }
    .value {
      margin: 0;
      font-size: clamp(1.05rem, 2.4vw, 1.5rem);
      font-weight: 700;
      line-height: 1.2;
    }
    .subvalue {
      margin: 6px 0 0;
      color: var(--muted);
      font-size: 0.84rem;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      border: 1px solid transparent;
    }
    .ok { color: #0b5f35; background: #d8f6e7; border-color: #90deb5; }
    .warn { color: #8b5b00; background: #fff0ce; border-color: #f4cd77; }
    .bad { color: #8c1f1f; background: #ffe0e0; border-color: #f5acac; }
    .down { color: #334155; background: #e8edf3; border-color: #c6d1df; }
    .charts {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
    }
    .panel {
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      padding: 10px 12px 12px;
      box-shadow: 0 4px 14px rgba(15, 34, 54, 0.05);
    }
    .panel h3 {
      margin: 2px 0 8px;
      font-size: 0.95rem;
      letter-spacing: 0.03em;
      text-transform: uppercase;
      color: var(--muted);
    }
    canvas {
      width: 100%;
      height: 220px;
      border-radius: 10px;
      border: 1px solid #d7e3f1;
      background:
        linear-gradient(to bottom, #fbfdff, #f6f9fc);
    }
    .table-wrap { overflow-x: auto; }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.88rem;
    }
    th, td {
      border-bottom: 1px solid #dbe7f4;
      padding: 8px 6px;
      text-align: left;
      white-space: nowrap;
    }
    th {
      color: var(--muted);
      font-weight: 600;
      font-size: 0.8rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    td.mono, .mono {
      font-family: "IBM Plex Mono", ui-monospace, monospace;
      font-size: 0.82rem;
    }
    .footer-note {
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.78rem;
    }
    @media (max-width: 980px) {
      .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .charts { grid-template-columns: 1fr; }
      canvas { height: 200px; }
    }
    @media (max-width: 640px) {
      .cards { grid-template-columns: 1fr; }
      .hero { align-items: start; }
      .controls { width: 100%; justify-content: flex-start; flex-wrap: wrap; }
    }
  </style>
</head>
<body>
  <main class="page">
    <section class="hero">
      <div>
        <h1>Node Sync Dashboard</h1>
        <p class="subtitle">Live status + persisted history from the <span class="mono">node_sync</span> monitor.</p>
      </div>
      <div class="controls">
        <label for="window">Window</label>
        <select id="window">
          <option value="15">15m</option>
          <option value="60" selected>1h</option>
          <option value="240">4h</option>
          <option value="1440">24h</option>
        </select>
        <button id="refresh">Refresh</button>
      </div>
    </section>

    <section class="cards">
      <article class="card">
        <p class="label">Current Status</p>
        <span id="statusBadge" class="badge down">Loading</span>
        <p id="statusWhen" class="subvalue">Waiting for first sample...</p>
      </article>
      <article class="card">
        <p class="label">Lag / P95</p>
        <p id="lagValue" class="value">-</p>
        <p id="lagSub" class="subvalue">-</p>
      </article>
      <article class="card">
        <p class="label">Mark Price (BTC)</p>
        <p id="markValue" class="value">-</p>
        <p id="markSub" class="subvalue">-</p>
      </article>
      <article class="card">
        <p class="label">Samples</p>
        <p id="sampleValue" class="value">-</p>
        <p id="sampleSub" class="subvalue">-</p>
      </article>
    </section>

    <section class="charts">
      <article class="panel">
        <h3>Lag (seconds)</h3>
        <canvas id="lagChart"></canvas>
      </article>
      <article class="panel">
        <h3>Block Advance (seconds per poll)</h3>
        <canvas id="advanceChart"></canvas>
      </article>
    </section>

    <section class="panel">
      <h3>Recent Samples</h3>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Collected</th>
              <th>Status</th>
              <th>Lag (s)</th>
              <th>Mark</th>
              <th>Advance (s)</th>
              <th>Error</th>
            </tr>
          </thead>
          <tbody id="samplesBody"></tbody>
        </table>
      </div>
      <p class="footer-note">Data source: <span class="mono">/api/stats/series</span> (auto-refresh every 5s).</p>
    </section>
  </main>

  <script>
    const POLL_MS = 5000;
    let latestSamples = [];

    const fmtNum = (value, digits = 2) =>
      Number.isFinite(value) ? Number(value).toFixed(digits) : "-";

    const fmtInt = (value) =>
      Number.isFinite(value) ? String(Math.round(value)) : "-";

    const fmtTime = (ms) => {
      if (!Number.isFinite(ms)) return "-";
      return new Date(ms).toLocaleTimeString();
    };

    const statusClass = (status) => {
      if (status === "SYNCED") return "ok";
      if (status === "BEHIND") return "warn";
      if (status === "LAGGING" || status === "FAR_BEHIND") return "bad";
      return "down";
    };

    function drawChart(canvasId, samples, valueFn, color) {
      const canvas = document.getElementById(canvasId);
      const ctx = canvas.getContext("2d");
      const dpr = window.devicePixelRatio || 1;
      const width = canvas.clientWidth || 300;
      const height = canvas.clientHeight || 220;

      canvas.width = Math.floor(width * dpr);
      canvas.height = Math.floor(height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, width, height);

      const pad = { left: 42, right: 12, top: 12, bottom: 22 };
      const plotW = width - pad.left - pad.right;
      const plotH = height - pad.top - pad.bottom;
      const values = samples.map(valueFn).map((v) => Number.isFinite(v) ? Number(v) : null);
      const valid = values.filter((v) => v !== null);

      ctx.strokeStyle = "#dbe7f5";
      ctx.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = pad.top + (plotH / 4) * i;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(width - pad.right, y);
        ctx.stroke();
      }

      if (!valid.length) {
        ctx.fillStyle = "#6b7280";
        ctx.font = "12px IBM Plex Mono";
        ctx.fillText("No data in selected window", pad.left + 8, pad.top + 18);
        return;
      }

      let min = Math.min(...valid);
      let max = Math.max(...valid);
      if (min === max) {
        min -= 1;
        max += 1;
      }
      const span = max - min;
      const xStep = values.length > 1 ? plotW / (values.length - 1) : 0;

      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.beginPath();
      let started = false;
      values.forEach((value, idx) => {
        if (value === null) {
          started = false;
          return;
        }
        const x = pad.left + xStep * idx;
        const y = pad.top + (1 - (value - min) / span) * plotH;
        if (!started) {
          ctx.moveTo(x, y);
          started = true;
        } else {
          ctx.lineTo(x, y);
        }
      });
      ctx.stroke();

      const lastIdx = values.length - 1;
      const lastVal = values[lastIdx];
      if (lastVal !== null) {
        const x = pad.left + xStep * lastIdx;
        const y = pad.top + (1 - (lastVal - min) / span) * plotH;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(x, y, 3.2, 0, Math.PI * 2);
        ctx.fill();
      }

      ctx.fillStyle = "#64748b";
      ctx.font = "11px IBM Plex Mono";
      ctx.fillText(fmtNum(max, 2), 4, pad.top + 8);
      ctx.fillText(fmtNum(min, 2), 4, pad.top + plotH);

      if (samples.length) {
        ctx.textAlign = "left";
        ctx.fillText(fmtTime(samples[0].collected_at_ms), pad.left, height - 6);
        ctx.textAlign = "right";
        ctx.fillText(fmtTime(samples[samples.length - 1].collected_at_ms), width - pad.right, height - 6);
        ctx.textAlign = "left";
      }
    }

    function updateSummary(summary) {
      const status = summary.latest_status || "DOWN";
      const badge = document.getElementById("statusBadge");
      badge.className = `badge ${statusClass(status)}`;
      badge.textContent = status.replaceAll("_", " ");

      document.getElementById("statusWhen").textContent =
        summary.latest_collected_at_ms
          ? `updated ${fmtTime(summary.latest_collected_at_ms)}`
          : "no samples yet";

      document.getElementById("lagValue").textContent =
        summary.latest_lag_seconds !== null
          ? `${fmtInt(summary.latest_lag_seconds)}s`
          : "-";
      document.getElementById("lagSub").textContent =
        `avg ${fmtNum(summary.avg_lag_seconds)}s | p95 ${fmtNum(summary.p95_lag_seconds)}s`;

      document.getElementById("markValue").textContent =
        summary.latest_mark_px !== null
          ? `$${fmtNum(summary.latest_mark_px)}`
          : "-";
      document.getElementById("markSub").textContent =
        summary.latest_error ? summary.latest_error : `window ${summary.window_minutes}m`;

      document.getElementById("sampleValue").textContent = String(summary.total_samples);
      document.getElementById("sampleSub").textContent =
        `ok ${summary.ok_samples} | down ${summary.down_samples}`;
    }

    function updateTable(samples) {
      const tbody = document.getElementById("samplesBody");
      const rows = samples.slice(-20).reverse().map((sample) => {
        const status = sample.status || "DOWN";
        return `
          <tr>
            <td class="mono">${fmtTime(sample.collected_at_ms)}</td>
            <td><span class="badge ${statusClass(status)}">${status.replaceAll("_", " ")}</span></td>
            <td class="mono">${sample.lag_seconds !== null ? fmtInt(sample.lag_seconds) : "-"}</td>
            <td class="mono">${sample.mark_px !== null ? fmtNum(sample.mark_px) : "-"}</td>
            <td class="mono">${sample.advance_ms !== null ? fmtNum(sample.advance_ms / 1000, 2) : "-"}</td>
            <td class="mono">${sample.error ? sample.error : "-"}</td>
          </tr>
        `;
      });
      tbody.innerHTML = rows.join("");
    }

    function renderCharts(samples) {
      drawChart("lagChart", samples, (sample) => sample.lag_seconds, "#0d9488");
      drawChart("advanceChart", samples, (sample) =>
        sample.advance_ms !== null ? sample.advance_ms / 1000 : null, "#2563eb");
    }

    async function loadSeries() {
      const minutes = document.getElementById("window").value;
      const response = await fetch(`/api/stats/series?minutes=${encodeURIComponent(minutes)}&limit=3000`, {
        cache: "no-store",
      });
      if (!response.ok) {
        const body = await response.text();
        throw new Error(body || `HTTP ${response.status}`);
      }
      return response.json();
    }

    async function refresh() {
      try {
        const payload = await loadSeries();
        latestSamples = payload.samples || [];
        updateSummary(payload.summary);
        updateTable(latestSamples);
        renderCharts(latestSamples);
      } catch (error) {
        const badge = document.getElementById("statusBadge");
        badge.className = "badge down";
        badge.textContent = "DOWN";
        document.getElementById("statusWhen").textContent = String(error);
      }
    }

    document.getElementById("refresh").addEventListener("click", refresh);
    document.getElementById("window").addEventListener("change", refresh);
    window.addEventListener("resize", () => {
      if (latestSamples.length) {
        renderCharts(latestSamples);
      }
    });

    refresh();
    setInterval(refresh, POLL_MS);
  </script>
</body>
</html>
"###;
