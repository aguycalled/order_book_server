//! Upstream fan-in: one WebSocket connection to the core order book server per
//! unique subscription, shared by every downstream client that wants it.
//!
//! This is the core scaling win: thousands of identical client subscriptions
//! collapse to a single upstream connection. Each upstream frame is reconstructed
//! into per-channel state (for bootstrapping late joiners) and broadcast verbatim
//! to the group as a shared `Arc<str>` — serialized exactly once, upstream.

use crate::reducer::Reducer;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{broadcast, Notify};

/// Per-client broadcast backlog. A client that falls this far behind is dropped
/// (slow-client isolation) rather than stalling the group.
const BROADCAST_CAP: usize = 2048;
/// Keep an idle upstream connection alive briefly after the last client leaves,
/// to avoid reconnect churn when clients flap.
const IDLE_GRACE: Duration = Duration::from_secs(20);

pub struct Group {
    pub key: String,
    pub tx: broadcast::Sender<Arc<str>>,
    pub reducer: Mutex<Reducer>,
    pub subs: AtomicUsize,
    pub stop: Notify,
}

impl Group {
    /// Atomically capture the current bootstrap snapshot and a receiver such
    /// that the receiver sees exactly the frames applied *after* the snapshot.
    /// (The upstream task holds `reducer` across apply+send, so there is no gap
    /// or duplicate at the boundary.)
    pub fn join(&self) -> (Option<String>, broadcast::Receiver<Arc<str>>) {
        let r = self.reducer.lock().unwrap();
        let snap = r.snapshot_frame();
        let rx = self.tx.subscribe();
        drop(r);
        (snap, rx)
    }
}

pub struct UpstreamManager {
    upstream_url: String,
    groups: Mutex<HashMap<String, Arc<Group>>>,
}

impl UpstreamManager {
    pub fn new(upstream_url: String) -> Arc<Self> {
        Arc::new(Self { upstream_url, groups: Mutex::new(HashMap::new()) })
    }

    pub fn group_count(&self) -> usize {
        self.groups.lock().unwrap().len()
    }

    /// Join (or create) the group for `key`. Increments the subscriber count.
    pub fn subscribe(self: &Arc<Self>, key: String, channel: String, sub_val: Value) -> Arc<Group> {
        let mut groups = self.groups.lock().unwrap();
        if let Some(g) = groups.get(&key) {
            g.subs.fetch_add(1, Ordering::SeqCst);
            return g.clone();
        }
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        let group = Arc::new(Group {
            key: key.clone(),
            tx,
            reducer: Mutex::new(Reducer::new(&channel, &sub_val)),
            subs: AtomicUsize::new(1),
            stop: Notify::new(),
        });
        groups.insert(key.clone(), group.clone());
        let mgr = self.clone();
        let g = group.clone();
        tokio::spawn(async move { upstream_task(mgr, g, sub_val).await });
        group
    }

    /// Fetch a one-shot authoritative snapshot for a subscription by opening a
    /// throwaway upstream connection and returning its first data frame. Used to
    /// bootstrap late joiners on channels the relay does not reconstruct itself
    /// (l4Book/l4TriggerBook): the core builds the snapshot, no matching-engine
    /// duplication in the relay.
    pub async fn fetch_snapshot(&self, sub_val: &Value) -> Option<String> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let (ws, _) = tokio_tungstenite::connect_async(&self.upstream_url).await.ok()?;
        let (mut w, mut r) = ws.split();
        w.send(Message::Text(subscribe_msg(sub_val).into())).await.ok()?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let next = tokio::time::timeout_at(deadline, r.next()).await.ok()??;
            if let Message::Text(t) = next.ok()? {
                let v: Value = serde_json::from_str(t.as_str()).ok()?;
                let ch = v.get("channel").and_then(|x| x.as_str()).unwrap_or("");
                if !ch.is_empty() && ch != "subscriptionResponse" && ch != "pong" && ch != "error" {
                    let _ = w.send(Message::Close(None)).await;
                    return Some(t.to_string());
                }
            }
        }
    }

    /// Decrement subscriber count; if it hits zero, schedule idle teardown.
    pub fn leave(self: &Arc<Self>, group: &Arc<Group>) {
        let prev = group.subs.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            let mgr = self.clone();
            let g = group.clone();
            tokio::spawn(async move {
                tokio::time::sleep(IDLE_GRACE).await;
                if g.subs.load(Ordering::SeqCst) == 0 {
                    mgr.groups.lock().unwrap().remove(&g.key);
                    g.stop.notify_waiters();
                }
            });
        }
    }
}

fn subscribe_msg(sub_val: &Value) -> String {
    serde_json::json!({"method": "subscribe", "subscription": sub_val}).to_string()
}

async fn upstream_task(mgr: Arc<UpstreamManager>, group: Arc<Group>, sub_val: Value) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    let sub_text = subscribe_msg(&sub_val);
    let mut backoff_ms = 250u64;

    loop {
        // Stop if torn down while (re)connecting.
        if group.subs.load(Ordering::SeqCst) == 0 {
            return;
        }

        let conn = tokio::select! {
            r = tokio_tungstenite::connect_async(&mgr.upstream_url) => r,
            _ = group.stop.notified() => return,
        };
        let ws = match conn {
            Ok((ws, _)) => ws,
            Err(e) => {
                log::warn!("upstream connect failed for {}: {e}", group.key);
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                    _ = group.stop.notified() => return,
                }
                backoff_ms = (backoff_ms * 2).min(10_000);
                continue;
            }
        };
        let (mut write, mut read) = ws.split();

        // Fresh connection: rebuild state from the upstream's immediate snapshot.
        group.reducer.lock().unwrap().reset();
        if write.send(Message::Text(sub_text.clone().into())).await.is_err() {
            continue;
        }
        backoff_ms = 250;
        log::info!("upstream connected for {}", group.key);

        loop {
            tokio::select! {
                _ = group.stop.notified() => return,
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            handle_frame(&group, text.as_str());
                        }
                        Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Ping(p))) => {
                            let _ = write.send(Message::Pong(p)).await;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            log::warn!("upstream closed for {}", group.key);
                            break;
                        }
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Err(e)) => {
                            log::warn!("upstream read error for {}: {e}", group.key);
                            break;
                        }
                    }
                }
            }
        }
        // fall through to reconnect
    }
}

/// Apply a frame to group state and fan it out — atomically, so a concurrent
/// `join()` never sees a torn snapshot/stream boundary. Subscription-response
/// and pong control frames are not forwarded (each client gets its own).
fn handle_frame(group: &Arc<Group>, text: &str) {
    let value: Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(_) => return,
    };
    let channel = value.get("channel").and_then(|v| v.as_str()).unwrap_or("");
    if channel.is_empty() || channel == "subscriptionResponse" || channel == "pong" || channel == "error" {
        return;
    }
    let data = value.get("data").cloned().unwrap_or(Value::Null);

    let mut reducer = group.reducer.lock().unwrap();
    reducer.apply(text, channel, &data);
    // Send under the lock so join()'s (snapshot, subscribe) boundary is exact.
    let _ = group.tx.send(Arc::from(text));
    drop(reducer);
}
