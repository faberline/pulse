//! Manual fault harness, never bundled in Pulse. Hold finished local results until
//! PRIVATE_GATE exists. Only literal loopback ports are accepted. No payload logging.
use axum::{
    Router,
    body::Bytes,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use futures_util::{SinkExt, StreamExt};
use pulse_link::{
    journal::{private_dir, private_write},
    protocol::{ClientWire, DevicePayload, MAX_FRAME, parse},
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_tungstenite::tungstenite::{Message as Peer, client::IntoClientRequest};

#[derive(Clone)]
struct Proxy {
    upstream: u16,
    gate: PathBuf,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err("use replay_proxy UPSTREAM_PORT LISTEN_PORT PRIVATE_GATE".into());
    }
    let proxy = Proxy {
        upstream: args[0].parse()?,
        gate: PathBuf::from(&args[2]),
    };
    private_dir(
        proxy
            .gate
            .parent()
            .ok_or("private gate directory required")?,
    )?;
    let listener =
        tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, args[1].parse::<u16>()?))
            .await?;
    eprintln!(
        "Fault harness on {}; local results held until gate exists",
        listener.local_addr()?
    );
    let router = Router::new()
        .route("/v1/device-pairings/redeem", post(pair))
        .route("/v1/devices/connect", get(upgrade))
        .with_state(Arc::new(proxy));
    tokio::select! {result=axum::serve(listener,router)=>result?, result=tokio::signal::ctrl_c()=>result?};
    Ok(())
}
async fn pair(State(proxy): State<Arc<Proxy>>, body: Bytes) -> axum::response::Response {
    let result = async {
        let response = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()?
            .post(format!(
                "http://127.0.0.1:{}/v1/device-pairings/redeem",
                proxy.upstream
            ))
            .body(body)
            .send()
            .await?;
        let status = response.status();
        let body = response.bytes().await?;
        Ok::<_, reqwest::Error>(
            (status, [("content-type", "application/json")], body).into_response(),
        )
    }
    .await;
    result.unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}
async fn upgrade(
    State(proxy): State<Arc<Proxy>>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    let Some(auth) = headers.get("authorization").cloned() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    ws.max_message_size(MAX_FRAME)
        .max_frame_size(MAX_FRAME)
        .on_upgrade(move |socket| async move {
            let _ = relay(proxy, socket, auth).await;
        })
}
async fn relay(
    proxy: Arc<Proxy>,
    mut socket: WebSocket,
    auth: axum::http::HeaderValue,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut request =
        format!("ws://127.0.0.1:{}/v1/devices/connect", proxy.upstream).into_client_request()?;
    request.headers_mut().insert("authorization", auth);
    let (mut peer, _) = tokio_tungstenite::connect_async(request).await?;
    loop {
        tokio::select! {
            message=socket.recv()=>{
                let Some(Ok(Message::Text(text)))=message else {break};
                if let Ok(ClientWire::Data{envelope})=parse::<ClientWire>(text.as_bytes())
                    && matches!(envelope.payload,DevicePayload::Result{..}) && !proxy.gate.exists() {
                    let marker=proxy.gate.with_extension("held");
                    if !marker.exists() {private_write(&marker,b"finished result held before server receipt\n")?;}
                    let _=socket.close().await;
                    let _=peer.close(None).await;
                    break;
                }
                peer.send(Peer::Text(text.to_string().into())).await?;
            }
            message=peer.next()=>{
                let Some(Ok(Peer::Text(text)))=message else {break};
                socket.send(Message::Text(text.to_string().into())).await?;
            }
        }
    }
    Ok(())
}
