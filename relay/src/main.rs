//! ws-relay: a subscription-multiplexing fan-out tier in front of the core
//! order book WebSocket server.
//!
//!   clients ──(wss via nginx)──> ws-relay ──(1 conn per unique sub)──> :8000 core
//!
//! - Collapses N identical client subscriptions to one upstream connection.
//! - Reconstructs channel state so late joiners get a full snapshot, then ride
//!   a shared broadcast of the raw upstream frames (serialized once upstream).
//! - Isolates slow clients (bounded per-client queue + broadcast backlog → drop).
//! - Open access with global + per-IP connection caps (per-IP via X-Forwarded-For
//!   set by nginx).

mod reducer;
mod upstream;

use anyhow::Result;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use upstream::{Group, UpstreamManager};

#[derive(Parser, Debug)]
struct Args {
    /// Address to listen on for downstream clients (behind nginx).
    #[arg(long, default_value = "127.0.0.1:8100")]
    listen: String,
    /// Upstream core order book WS server.
    #[arg(long, default_value = "ws://127.0.0.1:8000/ws")]
    upstream: String,
    /// Max total concurrent client connections.
    #[arg(long, default_value_t = 20_000)]
    max_conns: usize,
    /// Max concurrent client connections per client IP (X-Forwarded-For).
    #[arg(long, default_value_t = 200)]
    max_conns_per_ip: usize,
    /// Per-client outbound queue depth; clients that exceed it are dropped.
    #[arg(long, default_value_t = 4096)]
    client_queue: usize,
}

/// Raise the soft open-files limit to the hard limit. systemd keeps the soft
/// limit at 1024 by default even when LimitNOFILE is high; without this the
/// relay would refuse connections past ~1000 fds.
fn raise_nofile() {
    unsafe {
        let mut lim = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
            lim.rlim_cur = lim.rlim_max;
            let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
            log::info!("RLIMIT_NOFILE soft raised to {}", lim.rlim_max);
        }
    }
}

struct Limits {
    active: AtomicUsize,
    max_conns: usize,
    max_per_ip: usize,
    per_ip: Mutex<HashMap<String, usize>>,
}

impl Limits {
    /// Try to admit a connection from `ip`. Returns false if a cap is hit.
    fn admit(&self, ip: &str) -> bool {
        if self.active.fetch_add(1, Ordering::SeqCst) >= self.max_conns {
            self.active.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        let mut map = self.per_ip.lock().unwrap();
        let c = map.entry(ip.to_string()).or_insert(0);
        if *c >= self.max_per_ip {
            drop(map);
            self.active.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        *c += 1;
        true
    }
    fn release(&self, ip: &str) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        let mut map = self.per_ip.lock().unwrap();
        if let Some(c) = map.get_mut(ip) {
            *c -= 1;
            if *c == 0 {
                map.remove(ip);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    raise_nofile();
    let args = Args::parse();

    let mgr = UpstreamManager::new(args.upstream.clone());
    let limits = Arc::new(Limits {
        active: AtomicUsize::new(0),
        max_conns: args.max_conns,
        max_per_ip: args.max_conns_per_ip,
        per_ip: Mutex::new(HashMap::new()),
    });
    let client_queue = args.client_queue;

    let listener = TcpListener::bind(&args.listen).await?;
    log::info!(
        "ws-relay listening on {} -> upstream {} (max_conns={}, per_ip={})",
        args.listen, args.upstream, args.max_conns, args.max_conns_per_ip
    );

    // Periodic stats: client connections vs upstream connections (the fan-out ratio).
    {
        let mgr = mgr.clone();
        let limits = limits.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tick.tick().await;
                log::info!(
                    "stats: client_conns={} upstream_subs={}",
                    limits.active.load(Ordering::SeqCst),
                    mgr.group_count()
                );
            }
        });
    }

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                log::warn!("accept error: {e}");
                continue;
            }
        };
        let mgr = mgr.clone();
        let limits = limits.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, peer, mgr, limits, client_queue).await {
                log::debug!("client {peer} ended: {e}");
            }
        });
    }
}

async fn handle_client(
    stream: TcpStream,
    peer: SocketAddr,
    mgr: Arc<UpstreamManager>,
    limits: Arc<Limits>,
    client_queue: usize,
) -> Result<()> {
    // Capture X-Forwarded-For during the handshake to identify the real client
    // IP behind nginx (the TCP peer is always 127.0.0.1).
    let xff: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let xff_cb = xff.clone();
    let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                    resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
        if let Some(v) = req.headers().get("x-forwarded-for") {
            if let Ok(s) = v.to_str() {
                // first hop is the client
                let ip = s.split(',').next().unwrap_or("").trim().to_string();
                if !ip.is_empty() {
                    *xff_cb.lock().unwrap() = Some(ip);
                }
            }
        }
        Ok(resp)
    };

    let ws = tokio_tungstenite::accept_hdr_async(stream, callback).await?;
    let client_ip = xff.lock().unwrap().clone().unwrap_or_else(|| peer.ip().to_string());

    if !limits.admit(&client_ip) {
        log::info!("rejected {client_ip}: connection cap reached");
        // best-effort close
        let mut ws = ws;
        let _ = ws.close(None).await;
        return Ok(());
    }
    struct Admit<'a> {
        limits: &'a Limits,
        ip: String,
    }
    impl Drop for Admit<'_> {
        fn drop(&mut self) {
            self.limits.release(&self.ip);
        }
    }
    let _admit = Admit { limits: &limits, ip: client_ip.clone() };

    let (mut write, mut read) = ws.split();

    // Single writer task drains the per-client outbound queue to the socket.
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(client_queue);
    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Active subscriptions for this client: key -> (group, pump task).
    let mut subs: HashMap<String, (Arc<Group>, tokio::task::JoinHandle<()>)> = HashMap::new();

    let result = client_loop(&mut read, &out_tx, &mgr, &mut subs, client_queue).await;

    // Teardown: stop pumps, release group refcounts.
    for (_, (group, pump)) in subs.drain() {
        pump.abort();
        mgr.leave(&group);
    }
    drop(out_tx);
    let _ = writer.await;
    result
}

async fn client_loop(
    read: &mut (impl StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin),
    out_tx: &mpsc::Sender<Message>,
    mgr: &Arc<UpstreamManager>,
    subs: &mut HashMap<String, (Arc<Group>, tokio::task::JoinHandle<()>)>,
    client_queue: usize,
) -> Result<()> {
    while let Some(msg) = read.next().await {
        match msg? {
            Message::Text(text) => {
                let value: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => {
                        let _ = out_tx
                            .try_send(Message::Text(
                                serde_json::json!({"channel":"error","data":"invalid JSON"}).to_string().into(),
                            ));
                        continue;
                    }
                };
                let method = value.get("method").and_then(|v| v.as_str()).unwrap_or("");
                match method {
                    "ping" => {
                        let _ = out_tx.try_send(Message::Text(
                            serde_json::json!({"channel":"pong"}).to_string().into(),
                        ));
                    }
                    "subscribe" => {
                        handle_subscribe(&value, out_tx, mgr, subs, client_queue);
                    }
                    "unsubscribe" => {
                        handle_unsubscribe(&value, out_tx, mgr, subs);
                    }
                    _ => {}
                }
            }
            Message::Ping(p) => {
                let _ = out_tx.try_send(Message::Pong(p));
            }
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
    Ok(())
}

fn handle_subscribe(
    value: &Value,
    out_tx: &mpsc::Sender<Message>,
    mgr: &Arc<UpstreamManager>,
    subs: &mut HashMap<String, (Arc<Group>, tokio::task::JoinHandle<()>)>,
    _client_queue: usize,
) {
    let sub_val = match value.get("subscription") {
        Some(s) if s.is_object() => s.clone(),
        _ => return,
    };
    let channel = match sub_val.get("type").and_then(|v| v.as_str()) {
        Some(t) => t.to_string(),
        None => return,
    };
    // Canonical key: serde_json Map is sorted, so this is stable across clients.
    let key = sub_val.to_string();
    if subs.contains_key(&key) {
        return;
    }

    let group = mgr.subscribe(key.clone(), channel.clone(), sub_val.clone());

    // Echo the subscription response (mirrors core server behavior).
    let _ = out_tx.try_send(Message::Text(
        serde_json::json!({"channel":"subscriptionResponse","data":value}).to_string().into(),
    ));

    // Atomically grab bootstrap snapshot + live receiver, then pump to client.
    // rx is subscribed now, so live frames during an async L4 fetch are buffered
    // and delivered after the snapshot.
    let (snapshot, mut rx) = group.join();
    // Channels the relay does not reconstruct (l4Book/l4TriggerBook) get an
    // authoritative snapshot fetched on-demand from the core.
    let needs_fetch = snapshot.is_none() && (channel == "l4Book" || channel == "l4TriggerBook");
    let mgr_fetch = mgr.clone();
    let sub_fetch = sub_val.clone();
    let out = out_tx.clone();
    let pump = tokio::spawn(async move {
        let boot = match snapshot {
            Some(s) => Some(s),
            None if needs_fetch => mgr_fetch.fetch_snapshot(&sub_fetch).await,
            None => None,
        };
        if let Some(snap) = boot {
            if out.send(Message::Text(snap.into())).await.is_err() {
                return;
            }
        }
        loop {
            match rx.recv().await {
                Ok(frame) => {
                    // Drop the client if it can't keep up (slow-client isolation).
                    if out.try_send(Message::Text(frame.as_ref().to_string().into())).is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => return,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    });
    subs.insert(key, (group, pump));
}

fn handle_unsubscribe(
    value: &Value,
    out_tx: &mpsc::Sender<Message>,
    mgr: &Arc<UpstreamManager>,
    subs: &mut HashMap<String, (Arc<Group>, tokio::task::JoinHandle<()>)>,
) {
    let sub_val = match value.get("subscription") {
        Some(s) if s.is_object() => s.clone(),
        _ => return,
    };
    let key = sub_val.to_string();
    if let Some((group, pump)) = subs.remove(&key) {
        pump.abort();
        mgr.leave(&group);
        let _ = out_tx.try_send(Message::Text(
            serde_json::json!({"channel":"subscriptionResponse","data":value}).to_string().into(),
        ));
    }
}
