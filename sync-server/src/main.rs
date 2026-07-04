//! hashweb-sync — a live-collaboration relay that is itself a replica.
//!
//! The server holds a `HashWeb` and speaks canonical snapshots over
//! WebSocket:
//!   - on join, a client receives the server's current canonical bytes;
//!   - a client that changes locally sends its full snapshot; the server
//!     MERGES it (union of knowledge — never trusts, never interprets),
//!     persists, and broadcasts the merged canonical state to every
//!     client;
//!   - clients merge what they receive and reply only if their bytes
//!     still differ. "Equal op sets ⟺ identical snapshots" makes byte
//!     equality the convergence test, so the exchange provably quiesces.
//!
//! Snapshots are whole-state (kb-scale); the delta/outbox refinement is
//! APP_NOTES #8.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::sync::{broadcast, Mutex};
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;

use hashseq::encoding::{decode_hashweb, encode_hashweb};
use hashseq::HashWeb;

struct Shared {
    web: HashWeb,
    bytes: Vec<u8>,
}

struct App {
    shared: Mutex<Shared>,
    tx: broadcast::Sender<Arc<Vec<u8>>>,
    state_path: PathBuf,
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mut port = 8093u16;
    let mut web_dir = PathBuf::from("web");
    let mut state_path = PathBuf::from("kb-state.bin");
    while let Some(a) = args.next() {
        match a.as_str() {
            "--port" => port = args.next().expect("--port N").parse().expect("port"),
            "--web-dir" => web_dir = args.next().expect("--web-dir PATH").into(),
            "--state" => state_path = args.next().expect("--state PATH").into(),
            other => panic!("unknown arg: {other}"),
        }
    }

    let web = match std::fs::read(&state_path) {
        Ok(bytes) => match decode_hashweb(&bytes) {
            Ok(web) => {
                eprintln!("[sync] loaded state: {} bytes, {} objects", bytes.len(), web.object_count());
                web
            }
            Err(e) => {
                eprintln!("[sync] state file undecodable ({e:?}); starting empty");
                HashWeb::new()
            }
        },
        Err(_) => {
            eprintln!("[sync] no state file; starting empty");
            HashWeb::new()
        }
    };
    let bytes = encode_hashweb(&web);
    let (tx, _) = broadcast::channel(64);
    let app = Arc::new(App {
        shared: Mutex::new(Shared { web, bytes }),
        tx,
        state_path,
    });

    let kb = web_dir.join("kb.html");
    let router = Router::new()
        .route("/sync", get(ws_handler))
        .route(
            "/",
            get(move || async move {
                match tokio::fs::read(&kb).await {
                    Ok(body) => ([("content-type", "text/html; charset=utf-8")], body).into_response(),
                    Err(_) => (axum::http::StatusCode::NOT_FOUND, "kb.html not found").into_response(),
                }
            }),
        )
        .fallback_service(ServeDir::new(web_dir))
        // Clients must revalidate on every load: a phone running week-old
        // cached kb.js against a fresher peer is how documents get mangled.
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-cache"),
        ))
        .with_state(app);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    eprintln!("[sync] listening on http://{addr}/");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    axum::serve(listener, router).await.expect("serve");
}

async fn ws_handler(ws: WebSocketUpgrade, State(app): State<Arc<App>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| client_loop(socket, app))
}

async fn client_loop(mut socket: WebSocket, app: Arc<App>) {
    // Late-joiner bootstrap: current canonical state.
    let (hello, mut rx) = {
        let shared = app.shared.lock().await;
        (shared.bytes.clone(), app.tx.subscribe())
    };
    if socket.send(Message::Binary(hello.into())).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            broadcastd = rx.recv() => {
                match broadcastd {
                    Ok(bytes) => {
                        if socket.send(Message::Binary(bytes.as_ref().clone().into())).await.is_err() {
                            return;
                        }
                    }
                    // Lagged: skip to the freshest state.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let bytes = app.shared.lock().await.bytes.clone();
                        if socket.send(Message::Binary(bytes.into())).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            incoming = socket.recv() => {
                let Some(Ok(msg)) = incoming else { return };
                let Message::Binary(data) = msg else { continue };
                let theirs = match decode_hashweb(&data) {
                    Ok(web) => web,
                    Err(e) => {
                        eprintln!("[sync] rejecting undecodable snapshot ({} bytes): {e:?}", data.len());
                        continue;
                    }
                };
                let reply = {
                    let mut shared = app.shared.lock().await;
                    shared.web.merge(theirs);
                    let merged = encode_hashweb(&shared.web);
                    if merged != shared.bytes {
                        shared.bytes = merged.clone();
                        persist(&app.state_path, &merged).await;
                        let _ = app.tx.send(Arc::new(merged));
                        None // this client hears the broadcast like everyone
                    } else if data.as_ref() != shared.bytes.as_slice() {
                        // Client is strictly behind: catch it up directly.
                        Some(shared.bytes.clone())
                    } else {
                        None // converged — silence ends the exchange
                    }
                };
                if let Some(bytes) = reply {
                    if socket.send(Message::Binary(bytes.into())).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

async fn persist(path: &PathBuf, bytes: &[u8]) {
    let tmp = path.with_extension("tmp");
    if let Err(e) = tokio::fs::write(&tmp, bytes).await {
        eprintln!("[sync] persist failed: {e}");
        return;
    }
    if let Err(e) = tokio::fs::rename(&tmp, path).await {
        eprintln!("[sync] persist rename failed: {e}");
    }
}
