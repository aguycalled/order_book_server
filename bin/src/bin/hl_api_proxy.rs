use std::net::Ipv4Addr;

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{
        OriginalUri, State,
        ws::{Message as AxumWsMessage, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, HeaderName, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::json;
use server::Result;
use tokio::sync::mpsc;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as TungsteniteMessage};

type UpstreamWs = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type UpstreamSender = futures_util::stream::SplitSink<UpstreamWs, TungsteniteMessage>;
type UpstreamReceiver = futures_util::stream::SplitStream<UpstreamWs>;

#[derive(Debug, Parser)]
#[command(author, version, about = "Proxy Hyperliquid API traffic, routing selected /info methods to a local node")]
struct Args {
    /// Proxy bind address.
    #[arg(long, default_value = "127.0.0.1")]
    address: Ipv4Addr,

    /// Proxy port.
    #[arg(long, default_value = "3003")]
    port: u16,

    /// Base URL for the official Hyperliquid HTTP API.
    #[arg(long, default_value = "https://api.hyperliquid.xyz")]
    official_api_base: String,

    /// WebSocket URL for the official Hyperliquid API.
    #[arg(long, default_value = "wss://api.hyperliquid.xyz/ws")]
    official_ws_url: String,

    /// WebSocket URL for the local orderbook server.
    #[arg(long, default_value = "ws://127.0.0.1:8000/ws")]
    local_ws_url: String,

    /// Base URL for the local node API.
    /// Selected POST /info request types are forwarded here.
    #[arg(long, default_value = "http://127.0.0.1:3001")]
    local_api_base: String,

    /// Log level: error, warn, info, debug, trace.
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[derive(Clone, Debug)]
struct AppState {
    client: Client,
    official_api_base: String,
    official_ws_url: String,
    local_ws_url: String,
    local_api_base: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // SAFETY: We set the log env before starting any async tasks.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("RUST_LOG", &args.log_level);
    }
    env_logger::init();

    let state = AppState {
        client: Client::builder().build()?,
        official_api_base: trim_trailing_slash(&args.official_api_base).to_string(),
        official_ws_url: args.official_ws_url,
        local_ws_url: args.local_ws_url,
        local_api_base: trim_trailing_slash(&args.local_api_base).to_string(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_proxy))
        .fallback(any(proxy_http))
        .with_state(state.clone());

    let bind_address = format!("{}:{}", args.address, args.port);
    let listener = tokio::net::TcpListener::bind(&bind_address).await?;

    println!("Hyperliquid API Proxy v{}", env!("CARGO_PKG_VERSION"));
    println!("  Listening: http://{bind_address}");
    println!("  Official HTTP: {}", state.official_api_base);
    println!("  Official WS:   {}", state.official_ws_url);
    println!("  Local WS:      {}", state.local_ws_url);
    println!("  Local API:     {}", state.local_api_base);
    println!("  Routing: /exchange -> official API");
    println!("           /info -> local node only for selected info types");
    println!("           everything else -> official API");
    println!("  WS routing: supported subscriptions -> local WS, everything else -> official WS");

    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        json!({
            "status": "ok",
            "official_api_base": state.official_api_base,
            "official_ws_url": state.official_ws_url,
            "local_ws_url": state.local_ws_url,
            "local_api_base": state.local_api_base,
        })
        .to_string(),
    )
}

async fn proxy_http(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Response {
    let upstream_base = select_http_upstream(&state, &method, uri.path(), &body);
    let target_url = build_target_url(upstream_base, &uri);

    match forward_http_request(&state.client, method, headers, body, &target_url).await {
        Ok(response) => response,
        Err(err) => error_response(StatusCode::BAD_GATEWAY, format!("proxy request failed: {err}")),
    }
}

async fn forward_http_request(
    client: &Client,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
    target_url: &str,
) -> std::result::Result<Response, reqwest::Error> {
    let request_headers = filtered_headers(&headers);
    let response = client.request(method, target_url).headers(request_headers).body(body).send().await?;
    let status = response.status();
    let response_headers = filtered_headers(response.headers());
    let body = response.bytes().await?;

    let mut proxied = Response::new(Body::from(body));
    *proxied.status_mut() = status;
    proxied.headers_mut().extend(response_headers);
    Ok(proxied)
}

async fn ws_proxy(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_proxy(socket, state))
}

async fn handle_ws_proxy(socket: WebSocket, state: AppState) {
    let (mut client_sender, mut client_receiver) = socket.split();
    let (to_client_tx, mut to_client_rx) = mpsc::channel::<AxumWsMessage>(128);

    let client_writer = tokio::spawn(async move {
        while let Some(message) = to_client_rx.recv().await {
            if client_sender.send(message).await.is_err() {
                break;
            }
        }
    });

    let mut reader_tasks = Vec::new();
    let mut official_sender =
        connect_upstream("official", &state.official_ws_url, &to_client_tx, &mut reader_tasks).await;
    let mut local_sender = connect_upstream("local", &state.local_ws_url, &to_client_tx, &mut reader_tasks).await;

    while let Some(message) = client_receiver.next().await {
        let message = match message {
            Ok(message) => message,
            Err(err) => {
                log::warn!("client websocket error: {err}");
                break;
            }
        };

        match message {
            AxumWsMessage::Text(text) => match classify_ws_text(&text) {
                WsRoute::DirectPong => {
                    if to_client_tx.send(AxumWsMessage::Text(r#"{"channel":"pong"}"#.into())).await.is_err() {
                        break;
                    }
                }
                WsRoute::Local => {
                    if !send_to_upstream(
                        "local",
                        local_sender.as_mut(),
                        TungsteniteMessage::Text(text.to_string().into()),
                        &to_client_tx,
                    )
                    .await
                    {
                        local_sender = None;
                    }
                }
                WsRoute::Official => {
                    if !send_to_upstream(
                        "official",
                        official_sender.as_mut(),
                        TungsteniteMessage::Text(text.to_string().into()),
                        &to_client_tx,
                    )
                    .await
                    {
                        official_sender = None;
                    }
                }
            },
            AxumWsMessage::Binary(bytes) => {
                let official_ok = send_to_upstream(
                    "official",
                    official_sender.as_mut(),
                    TungsteniteMessage::Binary(bytes.clone()),
                    &to_client_tx,
                )
                .await;
                let local_ok =
                    send_to_upstream("local", local_sender.as_mut(), TungsteniteMessage::Binary(bytes), &to_client_tx)
                        .await;
                if !official_ok {
                    official_sender = None;
                }
                if !local_ok {
                    local_sender = None;
                }
            }
            AxumWsMessage::Ping(bytes) => {
                let official_ok = send_to_upstream(
                    "official",
                    official_sender.as_mut(),
                    TungsteniteMessage::Ping(bytes.clone()),
                    &to_client_tx,
                )
                .await;
                let local_ok =
                    send_to_upstream("local", local_sender.as_mut(), TungsteniteMessage::Ping(bytes), &to_client_tx)
                        .await;
                if !official_ok {
                    official_sender = None;
                }
                if !local_ok {
                    local_sender = None;
                }
            }
            AxumWsMessage::Pong(bytes) => {
                let official_ok = send_to_upstream(
                    "official",
                    official_sender.as_mut(),
                    TungsteniteMessage::Pong(bytes.clone()),
                    &to_client_tx,
                )
                .await;
                let local_ok =
                    send_to_upstream("local", local_sender.as_mut(), TungsteniteMessage::Pong(bytes), &to_client_tx)
                        .await;
                if !official_ok {
                    official_sender = None;
                }
                if !local_ok {
                    local_sender = None;
                }
            }
            AxumWsMessage::Close(_) => {
                if let Some(sender) = official_sender.as_mut() {
                    let _ = sender.send(TungsteniteMessage::Close(None)).await;
                }
                if let Some(sender) = local_sender.as_mut() {
                    let _ = sender.send(TungsteniteMessage::Close(None)).await;
                }
                break;
            }
        }
    }

    drop(to_client_tx);
    if let Some(mut sender) = official_sender {
        let _ = sender.send(TungsteniteMessage::Close(None)).await;
    }
    if let Some(mut sender) = local_sender {
        let _ = sender.send(TungsteniteMessage::Close(None)).await;
    }
    for task in reader_tasks {
        let _ = task.await;
    }
    let _ = client_writer.await;
}

fn map_upstream_ws_message(message: TungsteniteMessage) -> Option<AxumWsMessage> {
    match message {
        TungsteniteMessage::Text(text) => Some(AxumWsMessage::Text(text.to_string().into())),
        TungsteniteMessage::Binary(bytes) => Some(AxumWsMessage::Binary(bytes)),
        TungsteniteMessage::Ping(bytes) => Some(AxumWsMessage::Ping(bytes)),
        TungsteniteMessage::Pong(bytes) => Some(AxumWsMessage::Pong(bytes)),
        TungsteniteMessage::Close(_) => None,
        TungsteniteMessage::Frame(_) => None,
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum WsRoute {
    DirectPong,
    Local,
    Official,
}

async fn connect_upstream(
    label: &'static str,
    url: &str,
    to_client_tx: &mpsc::Sender<AxumWsMessage>,
    reader_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
) -> Option<UpstreamSender> {
    match connect_async(url).await {
        Ok((stream, _)) => {
            let (sender, receiver) = stream.split();
            reader_tasks.push(spawn_upstream_reader(label, receiver, to_client_tx.clone()));
            Some(sender)
        }
        Err(err) => {
            log::warn!("failed to connect to {label} websocket upstream {url}: {err}");
            None
        }
    }
}

fn spawn_upstream_reader(
    label: &'static str,
    mut receiver: UpstreamReceiver,
    to_client_tx: mpsc::Sender<AxumWsMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(message) = receiver.next().await {
            match message {
                Ok(message) => {
                    if let Some(mapped) = map_upstream_ws_message(message) {
                        if to_client_tx.send(mapped).await.is_err() {
                            break;
                        }
                    }
                }
                Err(err) => {
                    log::warn!("{label} websocket upstream error: {err}");
                    let _ =
                        to_client_tx.send(ws_error_message(format!("{label} websocket upstream error: {err}"))).await;
                    break;
                }
            }
        }
    })
}

async fn send_to_upstream(
    label: &'static str,
    sender: Option<&mut UpstreamSender>,
    message: TungsteniteMessage,
    to_client_tx: &mpsc::Sender<AxumWsMessage>,
) -> bool {
    match sender {
        Some(sender) => match sender.send(message).await {
            Ok(()) => true,
            Err(err) => {
                let _ = to_client_tx
                    .send(ws_error_message(format!("failed to send to {label} websocket upstream: {err}")))
                    .await;
                false
            }
        },
        None => {
            let _ = to_client_tx.send(ws_error_message(format!("{label} websocket upstream unavailable"))).await;
            false
        }
    }
}

fn classify_ws_text(text: &str) -> WsRoute {
    let value = match serde_json::from_str::<serde_json::Value>(text) {
        Ok(value) => value,
        Err(_) => return WsRoute::Official,
    };

    match value.get("method").and_then(serde_json::Value::as_str) {
        Some("ping") => WsRoute::DirectPong,
        Some("subscribe") | Some("unsubscribe") => {
            let subscription_type = value
                .get("subscription")
                .and_then(|subscription| subscription.get("type"))
                .and_then(serde_json::Value::as_str);

            if matches!(subscription_type, Some("bbo" | "l2Book" | "l4Book" | "trades" | "orderUpdates")) {
                WsRoute::Local
            } else {
                WsRoute::Official
            }
        }
        _ => WsRoute::Official,
    }
}

fn ws_error_message(message: String) -> AxumWsMessage {
    AxumWsMessage::Text(json!({ "channel": "error", "data": message }).to_string().into())
}

fn select_http_upstream<'a>(state: &'a AppState, method: &Method, path: &str, body: &[u8]) -> &'a str {
    if routes_to_info(path) && *method == Method::POST && info_body_routes_to_local(body) {
        &state.local_api_base
    } else {
        &state.official_api_base
    }
}

fn routes_to_info(path: &str) -> bool {
    matches!(path, "/info" | "/info/")
}

fn info_body_routes_to_local(body: &[u8]) -> bool {
    let value = match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(value) => value,
        Err(_) => return false,
    };

    let info_type = match value.get("type").and_then(serde_json::Value::as_str) {
        Some(info_type) => info_type,
        None => return false,
    };

    local_info_method(info_type)
}

fn local_info_method(info_type: &str) -> bool {
    matches!(
        info_type,
        "meta"
            | "spotMeta"
            | "clearinghouseState"
            | "spotClearinghouseState"
            | "openOrders"
            | "exchangeStatus"
            | "frontendOpenOrders"
            | "liquidatable"
            | "activeAssetData"
            | "maxMarketOrderNtls"
            | "vaultSummaries"
            | "userVaultEquities"
            | "leadingVaults"
            | "extraAgents"
            | "subAccounts"
            | "userFees"
            | "userRateLimit"
            | "spotDeployState"
            | "perpDeployAuctionStatus"
            | "delegations"
            | "delegatorSummary"
            | "maxBuilderFee"
            | "userToMultiSigSigners"
            | "userRole"
            | "perpsAtOpenInterestCap"
            | "validatorL1Votes"
            | "marginTable"
            | "perpDexs"
    )
}

fn build_target_url(base: &str, uri: &axum::http::Uri) -> String {
    let path_and_query = uri.path_and_query().map_or(uri.path(), |value| value.as_str());
    format!("{}{path_and_query}", trim_trailing_slash(base))
}

fn trim_trailing_slash(value: &str) -> &str {
    value.trim_end_matches('/')
}

fn filtered_headers(headers: &HeaderMap) -> HeaderMap {
    let mut filtered = HeaderMap::new();

    for (name, value) in headers {
        if is_hop_by_hop_header(name) {
            continue;
        }
        filtered.append(name, value.clone());
    }

    filtered
}

fn is_hop_by_hop_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "host"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "content-length"
    )
}

fn error_response(status: StatusCode, message: String) -> Response {
    let mut response = Response::new(Body::from(message));
    *response.status_mut() = status;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "text/plain; charset=utf-8".parse().expect("static header value must parse"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_path_is_detected() {
        assert!(routes_to_info("/info"));
        assert!(routes_to_info("/info/"));
        assert!(!routes_to_info("/exchange"));
    }

    #[test]
    fn listed_info_methods_route_to_local() {
        assert!(local_info_method("meta"));
        assert!(local_info_method("exchangeStatus"));
        assert!(local_info_method("perpDexs"));
        assert!(!local_info_method("allMids"));
    }

    #[test]
    fn info_body_uses_type_field_for_routing() {
        assert!(info_body_routes_to_local(br#"{"type":"meta"}"#));
        assert!(!info_body_routes_to_local(br#"{"type":"allMids"}"#));
        assert!(!info_body_routes_to_local(br#"{"coin":"BTC"}"#));
    }

    #[test]
    fn target_url_preserves_query_string() {
        let uri = "/info?foo=bar".parse().expect("uri should parse");
        assert_eq!(build_target_url("https://api.hyperliquid.xyz/", &uri), "https://api.hyperliquid.xyz/info?foo=bar");
    }

    #[test]
    fn websocket_supported_subscriptions_route_local() {
        assert_eq!(
            classify_ws_text(r#"{ "method": "subscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }"#),
            WsRoute::Local
        );
        assert_eq!(
            classify_ws_text(
                r#"{ "method": "unsubscribe", "subscription": { "type": "orderUpdates", "user": "0x1234567890abcdef1234567890abcdef12345678" } }"#
            ),
            WsRoute::Local
        );
    }

    #[test]
    fn websocket_unsupported_messages_stay_official() {
        assert_eq!(
            classify_ws_text(
                r#"{ "method": "subscribe", "subscription": { "type": "userFills", "user": "0x1234567890abcdef1234567890abcdef12345678" } }"#
            ),
            WsRoute::Official
        );
        assert_eq!(
            classify_ws_text(
                r#"{ "method": "post", "request": { "type": "info", "payload": { "type": "allMids" } } }"#
            ),
            WsRoute::Official
        );
    }

    #[test]
    fn websocket_ping_is_answered_directly() {
        assert_eq!(classify_ws_text(r#"{ "method": "ping" }"#), WsRoute::DirectPong);
    }

    #[test]
    fn select_http_upstream_routes_info_to_local_only_for_listed_types() {
        let state = AppState {
            client: Client::builder().build().expect("client should build"),
            official_api_base: "https://api.hyperliquid.xyz".to_string(),
            official_ws_url: "wss://api.hyperliquid.xyz/ws".to_string(),
            local_ws_url: "ws://127.0.0.1:8000/ws".to_string(),
            local_api_base: "http://127.0.0.1:3001".to_string(),
        };

        assert_eq!(
            select_http_upstream(&state, &Method::POST, "/info", br#"{"type":"meta"}"#),
            state.local_api_base.as_str()
        );
        assert_eq!(
            select_http_upstream(&state, &Method::POST, "/info", br#"{"type":"allMids"}"#),
            state.official_api_base.as_str()
        );
        assert_eq!(
            select_http_upstream(&state, &Method::POST, "/exchange", br#"{}"#),
            state.official_api_base.as_str()
        );
    }
}
