//! Load generator for ws-relay. Opens many concurrent client connections,
//! subscribes each to an l2Book (round-robin over a coin set), and measures
//! connect success, time-to-first-snapshot, and steady message throughput.
//!
//! A fraction of connections can be made "slow" (read with delay) to verify the
//! relay drops laggards without starving healthy clients.

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "ws://127.0.0.1:8100")]
    url: String,
    #[arg(long, default_value_t = 1000)]
    conns: usize,
    /// Comma-separated coins to spread subscriptions across (=> upstream groups).
    #[arg(long, default_value = "BTC,ETH,SOL,HYPE")]
    coins: String,
    #[arg(long, default_value_t = 15)]
    duration: u64,
    /// New connections opened per 100ms (ramp; avoids a thundering herd).
    #[arg(long, default_value_t = 400)]
    rate: usize,
    /// Fraction (0-100) of connections that read slowly (simulate laggards).
    #[arg(long, default_value_t = 0)]
    slow_pct: u64,
}

struct Stats {
    connected: AtomicUsize,
    failed: AtomicUsize,
    frames: AtomicU64,
    first_frame_us_sum: AtomicU64,
    first_frame_count: AtomicU64,
    dropped: AtomicUsize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let coins: Vec<String> = args.coins.split(',').map(|s| s.trim().to_string()).collect();
    let stats = Arc::new(Stats {
        connected: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
        frames: AtomicU64::new(0),
        first_frame_us_sum: AtomicU64::new(0),
        first_frame_count: AtomicU64::new(0),
        dropped: AtomicUsize::new(0),
    });
    let deadline = Instant::now() + Duration::from_secs(args.duration);

    println!(
        "loadtest: {} conns over {} coins, {}s, ramp {}/100ms, slow {}%",
        args.conns, coins.len(), args.duration, args.rate, args.slow_pct
    );

    let mut handles = Vec::with_capacity(args.conns);
    for i in 0..args.conns {
        // ramp
        if i > 0 && i % args.rate == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let url = args.url.clone();
        let coin = coins[i % coins.len()].clone();
        let stats = stats.clone();
        let slow = (i as u64 % 100) < args.slow_pct;
        handles.push(tokio::spawn(async move {
            run_conn(url, coin, stats, deadline, slow).await;
        }));
    }

    // Report once all connection attempts have settled.
    let report = {
        let stats = stats.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;
                if Instant::now() >= deadline {
                    break;
                }
                println!(
                    "  [t] connected={} failed={} frames={} dropped={}",
                    stats.connected.load(Ordering::Relaxed),
                    stats.failed.load(Ordering::Relaxed),
                    stats.frames.load(Ordering::Relaxed),
                    stats.dropped.load(Ordering::Relaxed),
                );
            }
        })
    };

    for h in handles {
        let _ = h.await;
    }
    let _ = report.await;

    let conn = stats.connected.load(Ordering::Relaxed);
    let frames = stats.frames.load(Ordering::Relaxed);
    let ff_cnt = stats.first_frame_count.load(Ordering::Relaxed).max(1);
    let ff_avg = stats.first_frame_us_sum.load(Ordering::Relaxed) / ff_cnt;
    println!("\n=== RESULT ===");
    println!("connected:        {conn}");
    println!("failed:           {}", stats.failed.load(Ordering::Relaxed));
    println!("dropped(by relay):{}", stats.dropped.load(Ordering::Relaxed));
    println!("total frames:     {frames}");
    println!("frames/sec:       {}", frames / args.duration.max(1));
    println!("avg time-to-first-snapshot: {:.1} ms", ff_avg as f64 / 1000.0);
}

async fn run_conn(url: String, coin: String, stats: Arc<Stats>, deadline: Instant, slow: bool) {
    let started = Instant::now();
    let ws = match tokio_tungstenite::connect_async(&url).await {
        Ok((ws, _)) => ws,
        Err(_) => {
            stats.failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    stats.connected.fetch_add(1, Ordering::Relaxed);
    let (mut w, mut r) = ws.split();
    let sub = format!(r#"{{"method":"subscribe","subscription":{{"type":"l2Book","coin":"{coin}"}}}}"#);
    if w.send(Message::Text(sub.into())).await.is_err() {
        return;
    }

    let mut got_first = false;
    loop {
        let timeout = deadline.saturating_duration_since(Instant::now());
        if timeout.is_zero() {
            break;
        }
        match tokio::time::timeout(timeout, r.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if t.contains("\"l2Book\"") {
                    stats.frames.fetch_add(1, Ordering::Relaxed);
                    if !got_first {
                        got_first = true;
                        let us = started.elapsed().as_micros() as u64;
                        stats.first_frame_us_sum.fetch_add(us, Ordering::Relaxed);
                        stats.first_frame_count.fetch_add(1, Ordering::Relaxed);
                    }
                    if slow {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                stats.dropped.fetch_add(1, Ordering::Relaxed);
                break;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Err(_) => break,
        }
    }
}
