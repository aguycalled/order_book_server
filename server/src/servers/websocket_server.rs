use crate::{
    history::{self, L2History},
    listeners::order_book::{
        HftMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, SnapshotMessage, TimedSnapshots, TriggerSnapshots, hl_listen_hft,
    },
    metrics::{
        BBO_CHANGES_TOTAL, BROADCAST_RECEIVERS, BROADCASTS_TOTAL, CHANNEL_DROPS_TOTAL, CHANNEL_LAG,
        MESSAGES_SENT_TOTAL, ORDERBOOK_HEIGHT, ORDERBOOK_LATEST_DATA_HEIGHT, WS_CONNECTIONS_ACTIVE,
        WS_CONNECTIONS_TOTAL, WS_SEND_ERRORS_TOTAL,
    },
    order_book::{Coin, Px, Snapshot, Sz},
    prelude::*,
    types::{
        Bbo, L2Book, L4Book, L4BookUpdates, L4Order, Level, Trade, TriggerBook,
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, OrderUpdate, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{SinkExt, StreamExt};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

use crate::ServerConfig;

pub async fn run_websocket_server(config: ServerConfig) -> Result<()> {
    // Separate channels: snapshots (L2/trigger/BBO) are low-frequency,
    // HFT messages (L4 diffs/statuses/fills) are high-frequency
    let (snapshot_tx, _) = channel::<Arc<SnapshotMessage>>(256);
    let (hft_tx, _) = channel::<Arc<HftMessage>>(256);

    // Market filter flags from config
    let market_filter = (config.include_perps, config.include_spot, config.include_hip3);
    let ignore_spot = !config.include_spot; // For OrderBookListener (legacy)
    let compression_level = config.compression_level;

    // Resolve data directory
    // Central task: listen to messages and forward them for distribution
    let listener = {
        OrderBookListener::new(Some(snapshot_tx.clone()), Some(hft_tx.clone()), ignore_spot)
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        let config = config.clone();
        tokio::spawn(async move {
            info!("Starting HFT-optimized listener");
            let result = hl_listen_hft(listener, config).await;
            if let Err(err) = result {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    // Open L2 history database
    let history_db_path = config.history_db_path.clone().unwrap_or_else(|| {
        let base = config
            .data_dir
            .clone()
            .unwrap_or_else(|| dirs::home_dir().expect("Could not find home directory").join("hl/data"));
        base.join("l2_history.rocksdb")
    });
    let l2_history = Arc::new(
        L2History::open(history_db_path.clone())
            .unwrap_or_else(|e| panic!("Failed to open L2 history database at {}: {}", history_db_path.display(), e)),
    );
    info!("L2 history database opened at {}", history_db_path.display());

    // Spawn 15-minute L2 snapshot recording task
    {
        let l2_history = l2_history.clone();
        let listener = listener.clone();
        tokio::spawn(async move {
            // Wait 30s for initial snapshot to be ready
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(15 * 60));
            loop {
                interval.tick().await;
                l2_history.record_snapshots(&listener).await;
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));

    let start_time = std::time::Instant::now();
    let listener_for_health = listener.clone();

    let app: Router = Router::new()
        .route(
            "/ws",
            get({
                let snapshot_tx = snapshot_tx.clone();
                let hft_tx = hft_tx.clone();
                let bbo_only = config.bbo_only;
                let listener = listener.clone();
                move |ws_upgrade| async move {
                    ws_handler(
                        ws_upgrade,
                        snapshot_tx.clone(),
                        hft_tx.clone(),
                        listener.clone(),
                        market_filter,
                        bbo_only,
                        websocket_opts,
                    )
                }
            }),
        )
        .route(
            "/health",
            get(move || {
                let listener = listener_for_health.clone();
                async move {
                    let is_ready = listener.lock().await.is_ready();
                    let uptime_secs = start_time.elapsed().as_secs();
                    let height = ORDERBOOK_HEIGHT.get();
                    let latest_data_height = ORDERBOOK_LATEST_DATA_HEIGHT.get();
                    let block_lag = (latest_data_height - height).max(0);
                    let connections = WS_CONNECTIONS_ACTIVE.get();
                    let body = format!(
                        r#"{{"status":"{}","uptime_seconds":{},"height":{},"latest_data_height":{},"block_lag":{},"connections":{}}}"
                    "#,
                        if is_ready { "ready" } else { "initializing" },
                        uptime_secs,
                        height,
                        latest_data_height,
                        block_lag,
                        connections,
                    );
                    axum::response::Response::builder().header("content-type", "application/json").body(body).unwrap()
                }
            }),
        )
        .route("/history/l2", get(history::history_handler))
        .with_state(l2_history);

    let tcp_listener = TcpListener::bind(&config.address).await?;
    info!("WebSocket server running at ws://{}", config.address);

    if let Err(err) = axum::serve(tcp_listener, app).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    snapshot_tx: Sender<Arc<SnapshotMessage>>,
    hft_tx: Sender<Arc<HftMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    market_filter: (bool, bool, bool),
    bbo_only: bool,
    websocket_opts: yawc::Options,
) -> impl IntoResponse {
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, snapshot_tx, hft_tx, listener, market_filter, bbo_only).await
    });

    resp
}

async fn handle_socket(
    mut socket: WebSocket,
    snapshot_tx: Sender<Arc<SnapshotMessage>>,
    hft_tx: Sender<Arc<HftMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    market_filter: (bool, bool, bool),
    bbo_only: bool,
) {
    // Track connection metrics
    WS_CONNECTIONS_ACTIVE.inc();
    WS_CONNECTIONS_TOTAL.inc();

    // Use a guard to decrement active connections when this function exits
    struct ConnectionGuard;
    impl Drop for ConnectionGuard {
        fn drop(&mut self) {
            WS_CONNECTIONS_ACTIVE.dec();
            BROADCAST_RECEIVERS.dec();
        }
    }
    let _connection_guard = ConnectionGuard;

    let mut snapshot_rx = snapshot_tx.subscribe();
    let mut hft_rx = hft_tx.subscribe();
    BROADCAST_RECEIVERS.set(snapshot_tx.receiver_count() as i64);
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    // Track last BBO per coin to avoid sending duplicates (bid_px, bid_sz, ask_px, ask_sz)
    let mut last_bbo: HashMap<String, (String, String, String, String)> = HashMap::new();
    // Track last-sent levels per subscription for delta computation
    let mut last_l2_levels: HashMap<String, [Vec<Level>; 2]> = HashMap::new();
    let mut last_trigger_levels: HashMap<String, [Vec<Level>; 2]> = HashMap::new();
    // Track last trade price per coin for allMids subscription
    let mut all_prices: HashMap<String, String> = HashMap::new();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await;
        return;
    }
    loop {
        select! {
            biased;
            // Snapshot channel: L2/trigger (low frequency) — PRIORITIZED
            recv_result = snapshot_rx.recv() => {
                match recv_result {
                    Ok(msg) => match msg.as_ref() {
                        SnapshotMessage::Snapshot{ l2_snapshots, trigger_snapshots, time } => {
                            universe = new_universe(l2_snapshots, market_filter.0, market_filter.1, market_filter.2);
                            for sub in manager.subscriptions() {
                                if !matches!(sub, Subscription::Bbo { .. }) {
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time, &mut last_bbo, &mut last_l2_levels).await;
                                }
                                if let Subscription::TriggerBook { coin, n_sig_figs, n_levels, mantissa } = sub {
                                    send_ws_data_from_trigger_book(&mut socket, coin, trigger_snapshots, *time, *n_sig_figs, *mantissa, *n_levels, &mut last_trigger_levels).await;
                                }
                            }
                        },
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        CHANNEL_LAG.set(n as i64);
                        CHANNEL_DROPS_TOTAL.inc();
                        log::info!("Snapshot receiver lagged: {n} messages");
                    }
                    Err(err) => {
                        error!("Snapshot receiver error: {err}");
                        return;
                    }
                }
            }

            // HFT channel: L4/fills/orderUpdates (high frequency)
            recv_result = hft_rx.recv() => {
                match recv_result {
                    Ok(msg) => match msg.as_ref() {
                        HftMessage::BboUpdate{ bbos, time } => {
                            for sub in manager.subscriptions() {
                                if let Subscription::Bbo { coin } = sub {
                                    send_ws_data_from_bbo(&mut socket, coin, bbos, *time, &mut last_bbo).await;
                                }
                            }
                        },
                        HftMessage::Fills{ batch } => {
                            let has_trades = manager.subscriptions().iter().any(|s| matches!(s, Subscription::Trades { .. }));
                            let has_all_prices = manager.subscriptions().iter().any(|s| matches!(s, Subscription::AllPrices { .. }));
                            if has_trades || has_all_prices {
                                let mut trades = coin_to_trades(batch);
                                if has_trades {
                                    for sub in manager.subscriptions() {
                                        send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                    }
                                }
                                if has_all_prices {
                                    // Update last prices and send deltas
                                    let mut changed: HashMap<String, String> = HashMap::new();
                                    for (coin, coin_trades) in &trades {
                                        if let Some(last) = coin_trades.last() {
                                            let px = last.px().to_string();
                                            if all_prices.get(coin) != Some(&px) {
                                                all_prices.insert(coin.clone(), px.clone());
                                                changed.insert(coin.clone(), px);
                                            }
                                        }
                                    }
                                    if !changed.is_empty() {
                                        for sub in manager.subscriptions() {
                                            if let Subscription::AllPrices { coin, coins } = sub {
                                                let filtered = filter_prices(&changed, coin, coins);
                                                if !filtered.is_empty() {
                                                    send_socket_message(&mut socket, ServerResponse::AllPrices(filtered)).await;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        HftMessage::L4OrderDiffs{ batch } => {
                            let has_l4 = manager.subscriptions().iter().any(|s| matches!(s, Subscription::L4Book { .. } | Subscription::L4TriggerBook { .. }));
                            if has_l4 {
                                let book_updates = coin_to_book_diffs_only(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &book_updates).await;
                                }
                            }
                        },
                        HftMessage::L4OrderStatuses{ batch } => {
                            let has_l4 = manager.subscriptions().iter().any(|s| matches!(s, Subscription::L4Book { .. } | Subscription::L4TriggerBook { .. }));
                            let has_order_updates = manager.subscriptions().iter().any(|s| matches!(s, Subscription::OrderUpdates { .. }));
                            if has_l4 {
                                let book_updates = coin_to_book_statuses_only(batch);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_book_updates(&mut socket, sub, &book_updates).await;
                                }
                            }
                            if has_order_updates {
                                for sub in manager.subscriptions() {
                                    send_ws_order_updates(&mut socket, sub, batch).await;
                                }
                            }
                        },
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // HFT lag is expected for non-L4 clients — silently skip
                    }
                    Err(_) => {}
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                match value {
                                    ClientMessage::Ping => {
                                        send_socket_message(&mut socket, ServerResponse::Pong).await;
                                    }
                                    _ => {
                                        receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone(), bbo_only, &mut last_l2_levels, &mut last_trigger_levels, &all_prices).await;
                                    }
                                }
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    bbo_only: bool,
    last_l2_levels: &mut HashMap<String, [Vec<Level>; 2]>,
    last_trigger_levels: &mut HashMap<String, [Vec<Level>; 2]>,
    all_prices: &HashMap<String, String>,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
        ClientMessage::Ping => unreachable!("Ping is handled before receive_client_message"),
    };
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await;
        return;
    }

    // In BBO-only mode, reject non-BBO subscriptions to save RAM
    if bbo_only {
        let is_bbo = matches!(&subscription, Subscription::Bbo { .. });
        if !is_bbo {
            let msg = ServerResponse::Error(
                "BBO-only mode: L2/L4/Trades subscriptions disabled. Only BBO subscriptions allowed.".to_string(),
            );
            send_socket_message(socket, msg).await;
            return;
        }
    }
    // Clear delta state on unsubscribe so resubscribe gets a fresh snapshot
    if let ClientMessage::Unsubscribe { subscription: ref sub } = client_message {
        match sub {
            Subscription::L2Book { coin, n_sig_figs, mantissa, .. } => {
                let key = format!("{}:{}:{}", coin, n_sig_figs.unwrap_or(0), mantissa.unwrap_or(0));
                last_l2_levels.remove(&key);
            }
            Subscription::TriggerBook { coin, n_sig_figs, mantissa, .. } => {
                let key = format!("trigger:{}:{}:{}", coin, n_sig_figs.unwrap_or(0), mantissa.unwrap_or(0));
                last_trigger_levels.remove(&key);
            }
            _ => {}
        }
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
        ClientMessage::Ping => unreachable!(),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await;
                    return;
                }
            }
        } else {
            None
        };
        // Extract allPrices filter before consuming client_message
        let is_all_prices = matches!(&client_message, ClientMessage::Subscribe { subscription: Subscription::AllPrices { .. } });
        let (ap_coin, ap_coins) = if let ClientMessage::Subscribe { subscription: Subscription::AllPrices { ref coin, ref coins } } = client_message {
            (coin.clone(), coins.clone())
        } else {
            (None, None)
        };

        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
        if let Some(snapshot_msg) = snapshot_msg {
            send_socket_message(socket, snapshot_msg).await;
        }
        if is_all_prices {
            let snapshot = filter_prices(all_prices, &ap_coin, &ap_coins);
            if !snapshot.is_empty() {
                send_socket_message(socket, ServerResponse::AllPrices(snapshot)).await;
            }
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
    }
}

/// Fast BBO broadcast - directly from BBO HashMap without L2 snapshot computation
async fn send_ws_data_from_bbo(
    socket: &mut WebSocket,
    coin: &str,
    bbos: &HashMap<Coin, (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>)>,
    time: u64,
    last_bbo: &mut HashMap<String, (String, String, String, String)>,
) {
    let coin_key = Coin::new(coin);
    if let Some((best_bid, best_ask)) = bbos.get(&coin_key) {
        // Convert to Level format - Px and Sz implement Debug for formatting
        let bid = best_bid
            .as_ref()
            .map(|(px, sz, n)| crate::types::Level::new(format!("{:?}", px), format!("{:?}", sz), *n as usize));
        let ask = best_ask
            .as_ref()
            .map(|(px, sz, n)| crate::types::Level::new(format!("{:?}", px), format!("{:?}", sz), *n as usize));

        // Deduplication check
        let bid_px = bid.as_ref().map(|b| b.px().to_string()).unwrap_or_default();
        let bid_sz = bid.as_ref().map(|b| b.sz().to_string()).unwrap_or_default();
        let ask_px = ask.as_ref().map(|a| a.px().to_string()).unwrap_or_default();
        let ask_sz = ask.as_ref().map(|a| a.sz().to_string()).unwrap_or_default();
        let current = (bid_px, bid_sz, ask_px, ask_sz);

        if last_bbo.get(coin) != Some(&current) {
            last_bbo.insert(coin.to_string(), current);
            BBO_CHANGES_TOTAL.with_label_values(&[coin]).inc();
            BROADCASTS_TOTAL.with_label_values(&["bbo"]).inc();
            let bbo = Bbo { coin: coin.to_string(), time, bid, ask };
            let msg = ServerResponse::Bbo(bbo);
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_socket_message(socket: &mut WebSocket, msg: ServerResponse) {
    let msg = serde_json::to_string(&msg);
    match msg {
        Ok(msg) => {
            if let Err(err) = socket.send(FrameView::text(msg)).await {
                error!("Failed to send: {err}");
                WS_SEND_ERRORS_TOTAL.inc();
            } else {
                MESSAGES_SENT_TOTAL.inc();
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

// derive it from l2_snapshots because thats convenient
// Filters coins based on market type flags
fn new_universe(
    l2_snapshots: &L2Snapshots,
    include_perps: bool,
    include_spot: bool,
    include_hip3: bool,
) -> HashSet<String> {
    l2_snapshots
        .as_ref()
        .iter()
        .filter_map(|(c, _)| {
            let include =
                (c.is_perp() && include_perps) || (c.is_spot() && include_spot) || (c.is_hip3() && include_hip3);
            if include { Some(c.clone().value()) } else { None }
        })
        .collect()
}

/// Compute delta between old and new level arrays.
/// Returns only changed/new levels, plus removed levels (sz="0", n=0).
fn compute_level_delta(old: &[Level], new: &[Level]) -> Vec<Level> {
    use std::collections::BTreeMap;
    // Build maps keyed by px
    let old_map: BTreeMap<&str, &Level> = old.iter().map(|l| (l.px(), l)).collect();
    let new_map: BTreeMap<&str, &Level> = new.iter().map(|l| (l.px(), l)).collect();

    let mut delta = Vec::new();

    // Changed or new levels
    for (px, new_level) in &new_map {
        match old_map.get(px) {
            Some(old_level) if **old_level == **new_level => {} // unchanged
            _ => delta.push((*new_level).clone()),           // new or changed
        }
    }

    // Removed levels (in old but not in new)
    for (px, _) in &old_map {
        if !new_map.contains_key(px) {
            delta.push(Level::new(px.to_string(), "0".to_string(), 0));
        }
    }

    delta
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>,
    time: u64,
    last_bbo: &mut HashMap<String, (String, String, String, String)>,
    last_l2_levels: &mut HashMap<String, [Vec<Level>; 2]>,
) {
    match subscription {
        Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } => {
            let snapshot = snapshot.get(&Coin::new(coin));
            if let Some(snapshot) =
                snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
            {
                let n = n_levels.unwrap_or(400).min(400);
                let current = snapshot.truncate(n).export_inner_snapshot();
                let key = format!("{}:{}:{}", coin, n_sig_figs.unwrap_or(0), mantissa.unwrap_or(0));

                let levels_to_send = if let Some(prev) = last_l2_levels.get(&key) {
                    let bid_delta = compute_level_delta(&prev[0], &current[0]);
                    let ask_delta = compute_level_delta(&prev[1], &current[1]);
                    if bid_delta.is_empty() && ask_delta.is_empty() {
                        return;
                    }
                    [bid_delta, ask_delta]
                } else {
                    current.clone()
                };

                last_l2_levels.insert(key, current);
                BROADCASTS_TOTAL.with_label_values(&["l2"]).inc();
                let l2_book =
                    L2Book::from_l2_snapshot(coin.clone(), levels_to_send, time, *n_sig_figs, *mantissa, *n_levels);
                send_socket_message(socket, ServerResponse::L2Book(l2_book)).await;
            } else {
                error!("Coin {coin} not found");
            }
        }
        Subscription::Bbo { coin } => {
            // Get default snapshot (no aggregation)
            let snapshot = snapshot.get(&Coin::new(coin));
            if let Some(snapshot) = snapshot.and_then(|s| s.get(&L2SnapshotParams::new(None, None))) {
                let levels = snapshot.truncate(1).export_inner_snapshot();
                let bid = levels[0].first().cloned();
                let ask = levels[1].first().cloned();

                // Only send if BBO changed (dedupe identical messages)
                let bid_px = bid.as_ref().map(|b| b.px().to_string()).unwrap_or_default();
                let bid_sz = bid.as_ref().map(|b| b.sz().to_string()).unwrap_or_default();
                let ask_px = ask.as_ref().map(|a| a.px().to_string()).unwrap_or_default();
                let ask_sz = ask.as_ref().map(|a| a.sz().to_string()).unwrap_or_default();
                let current = (bid_px, bid_sz, ask_px, ask_sz);

                if last_bbo.get(coin) != Some(&current) {
                    last_bbo.insert(coin.clone(), current);
                    let bbo = Bbo { coin: coin.clone(), time, bid, ask };
                    let msg = ServerResponse::Bbo(bbo);
                    send_socket_message(socket, msg).await;
                }
                // else: skip, BBO unchanged
            }
        }
        _ => {}
    }
}

fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let fills = batch.clone().events();
    let mut trades = HashMap::new();

    // Convert each fill directly to a trade (no pairing)
    for fill in fills {
        let trade = Trade::from_single_fill(fill);
        let coin = trade.coin.clone();
        trades.entry(coin).or_insert_with(Vec::new).push(trade);
    }

    trades
}

/// HFT helper: convert order diffs batch to book updates (without statuses)
fn coin_to_book_diffs_only(diff_batch: &Batch<NodeDataOrderDiff>) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    updates
}

/// HFT helper: convert order statuses batch to book updates (without diffs)
fn coin_to_book_statuses_only(status_batch: &Batch<NodeDataOrderStatus>) -> HashMap<String, L4BookUpdates> {
    let statuses = status_batch.clone().events();
    let time = status_batch.block_time();
    let height = status_batch.block_number();
    let mut updates = HashMap::new();
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_trigger_book(
    socket: &mut WebSocket,
    coin: &str,
    trigger_snapshots: &HashMap<Coin, Snapshot<InnerLevel>>,
    time: u64,
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
    _n_levels: Option<usize>,
    last_trigger_levels: &mut HashMap<String, [Vec<Level>; 2]>,
) {
    let coin_key = Coin::new(coin);
    if let Some(raw_snapshot) = trigger_snapshots.get(&coin_key) {
        let snapshot = if n_sig_figs.is_some() || mantissa.is_some() {
            raw_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa)
        } else {
            raw_snapshot.clone()
        };
        let current = snapshot.export_inner_snapshot();
        let key = format!("trigger:{}:{}:{}", coin, n_sig_figs.unwrap_or(0), mantissa.unwrap_or(0));

        let levels_to_send = if let Some(prev) = last_trigger_levels.get(&key) {
            let bid_delta = compute_level_delta(&prev[0], &current[0]);
            let ask_delta = compute_level_delta(&prev[1], &current[1]);
            if bid_delta.is_empty() && ask_delta.is_empty() {
                return;
            }
            [bid_delta, ask_delta]
        } else {
            current.clone()
        };

        last_trigger_levels.insert(key, current);
        BROADCASTS_TOTAL.with_label_values(&["triggerBook"]).inc();
        let trigger_book = TriggerBook { coin: coin.to_string(), time, levels: levels_to_send };
        send_socket_message(socket, ServerResponse::TriggerBook(trigger_book)).await;
    }
}

fn filter_prices(prices: &HashMap<String, String>, coin: &Option<String>, coins: &Option<Vec<String>>) -> HashMap<String, String> {
    if coin.is_none() && coins.is_none() {
        return prices.clone();
    }
    prices.iter().filter(|(k, _)| {
        if let Some(c) = coin {
            if k.as_str() == c { return true; }
        }
        if let Some(cs) = coins {
            if cs.iter().any(|c| c == k.as_str()) { return true; }
        }
        false
    }).map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn in_price_range(px: &str, start_px: &Option<String>, end_px: &Option<String>) -> bool {
    let val = match px.parse::<f64>() {
        Ok(v) => v,
        Err(_) => return false,
    };
    if let Some(s) = start_px {
        if let Ok(sv) = s.parse::<f64>() {
            if val < sv { return false; }
        }
    }
    if let Some(e) = end_px {
        if let Ok(ev) = e.parse::<f64>() {
            if val > ev { return false; }
        }
    }
    true
}

fn filter_l4_updates(updates: L4BookUpdates, start_px: &Option<String>, end_px: &Option<String>, trigger_only: bool) -> Option<L4BookUpdates> {
    let order_statuses: Vec<_> = updates.order_statuses.into_iter().filter(|s| {
        if trigger_only && !s.order.is_trigger { return false; }
        if !trigger_only && s.order.is_trigger { return false; }
        let px = if s.order.is_trigger { &s.order.trigger_px } else { &s.order.limit_px };
        in_price_range(px, start_px, end_px)
    }).collect();
    let book_diffs: Vec<_> = updates.book_diffs.into_iter().filter(|d| {
        // Diffs don't carry is_trigger, so for trigger-only subs we only send statuses
        if trigger_only { return false; }
        in_price_range(d.px(), start_px, end_px)
    }).collect();
    if order_statuses.is_empty() && book_diffs.is_empty() {
        return None;
    }
    Some(L4BookUpdates { time: updates.time, height: updates.height, order_statuses, book_diffs })
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &HashMap<String, L4BookUpdates>,
) {
    match subscription {
        Subscription::L4Book { coin, start_px, end_px } => {
            if let Some(updates) = book_updates.get(coin).cloned() {
                // Always filter to exclude trigger orders from l4Book
                let updates = filter_l4_updates(updates, start_px, end_px, false);
                if let Some(updates) = updates {
                    BROADCASTS_TOTAL.with_label_values(&["l4"]).inc();
                    send_socket_message(socket, ServerResponse::L4Book(L4Book::Updates(updates))).await;
                }
            }
        }
        Subscription::L4TriggerBook { coin, start_px, end_px } => {
            if let Some(updates) = book_updates.get(coin).cloned() {
                if let Some(updates) = filter_l4_updates(updates, start_px, end_px, true) {
                    BROADCASTS_TOTAL.with_label_values(&["l4TriggerBook"]).inc();
                    send_socket_message(socket, ServerResponse::L4TriggerBook(L4Book::Updates(updates))).await;
                }
            }
        }
        _ => {}
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            BROADCASTS_TOTAL.with_label_values(&["trades"]).inc();
            let msg = ServerResponse::Trades(trades);
            send_socket_message(socket, msg).await;
        }
    }
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        let (coin, start_px, end_px, trigger_only) = match self {
            Self::L4Book { coin, start_px, end_px } => (coin, start_px, end_px, false),
            Self::L4TriggerBook { coin, start_px, end_px } => (coin, start_px, end_px, true),
            _ => return Ok(None),
        };

        let snapshot = listener.lock().await.compute_snapshot();
        if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
            let requested_coin = Coin::new(coin);
            let filtered =
                snapshot.value().into_iter().filter(|(c, _)| *c == requested_coin).collect::<Vec<_>>().pop();
            if let Some((found_coin, coin_snapshot)) = filtered {
                let levels = coin_snapshot.as_ref().clone().map(|orders| {
                    orders.into_iter()
                        .filter(|o| {
                            if trigger_only && !o.is_trigger { return false; }
                            if !trigger_only && o.is_trigger { return false; }
                            let px = if o.is_trigger { &o.trigger_px } else { &o.limit_px.to_str() };
                            in_price_range(px, start_px, end_px)
                        })
                        .map(L4Order::from)
                        .collect()
                });
                let response = if trigger_only {
                    ServerResponse::L4TriggerBook(L4Book::Snapshot { coin: found_coin.value(), time, height, levels })
                } else {
                    ServerResponse::L4Book(L4Book::Snapshot { coin: found_coin.value(), time, height, levels })
                };
                return Ok(Some(response));
            }
        }
        Err("Snapshot Failed".into())
    }
}

/// Send order updates to OrderUpdates subscribers filtered by user address
async fn send_ws_order_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    batch: &Batch<NodeDataOrderStatus>,
) {
    if let Subscription::OrderUpdates { user } = subscription {
        // Parse the user address from the subscription
        let user_addr = match user.parse::<alloy::primitives::Address>() {
            Ok(addr) => addr,
            Err(_) => return, // Invalid address, skip
        };

        let time = batch.block_time();
        let height = batch.block_number();
        let statuses = batch.clone().events();

        // Filter statuses for this specific user
        let user_updates: Vec<OrderUpdate> = statuses
            .into_iter()
            .filter(|status| status.user == user_addr)
            .map(|status| OrderUpdate::new(status.user, time, height, status))
            .collect();

        if !user_updates.is_empty() {
            let msg = ServerResponse::OrderUpdates(user_updates);
            send_socket_message(socket, msg).await;
        }
    }
}
