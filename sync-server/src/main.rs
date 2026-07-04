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
//! Steady-state sync is DELTAS (0xDE frames of authored ops, relayed
//! raw to every client) and one-shot ARTIFACT pushes (0xAF ‖ bytes,
//! content-addressed). Snapshots remain the hello / reconnect-resync
//! path — and the compatibility path for older clients. Persistence is
//! debounced: deltas mark the state dirty; a background task re-encodes
//! and writes at most every few seconds; hello/lagged/catch-up replies
//! always encode fresh when dirty (a stale hello would strand a joiner
//! between the snapshot and the deltas it missed).

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

use hashseq::encoding::{
    apply_delta, decode_hashweb, encode_hashweb, encode_hashweb_ops, ARTIFACT_TAG, DELTA_TAG,
};
use hashseq::HashWeb;

struct Shared {
    web: HashWeb,
    /// The OPS-ONLY wire snapshot (hello / resync / quiesce compare).
    /// Artifact bytes never ride snapshots on the wire — they are pushed
    /// once (0xAF) or fetched by GET /artifact/:id, immutable-cached.
    bytes: Vec<u8>,
    /// Ops or artifacts landed since `bytes` / the state file were
    /// derived. Cleared by the debounced persist task (disk gets the
    /// FULL encoding; the wire never does).
    dirty: bool,
}

impl Shared {
    /// Fresh ops-wire bytes; leaves `dirty` set so the persist task
    /// still flushes the full state to disk.
    fn fresh_bytes(&mut self) -> Vec<u8> {
        if self.dirty {
            self.bytes = encode_hashweb_ops(&self.web);
        }
        self.bytes.clone()
    }
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
    let bytes = encode_hashweb_ops(&web);
    let (tx, _) = broadcast::channel(64);
    let app = Arc::new(App {
        shared: Mutex::new(Shared { web, bytes, dirty: false }),
        tx,
        state_path,
    });

    // Debounced persistence: deltas mark dirty; this task folds them into
    // the canonical state file at most every 3 seconds.
    {
        let app = app.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3));
            loop {
                tick.tick().await;
                let full = {
                    let mut shared = app.shared.lock().await;
                    if !shared.dirty {
                        continue;
                    }
                    shared.bytes = encode_hashweb_ops(&shared.web);
                    shared.dirty = false;
                    encode_hashweb(&shared.web) // disk keeps the artifacts
                };
                persist(&app.state_path, &full).await;
            }
        });
    }

    let kb = web_dir.join("kb.html");
    let router = Router::new()
        .route("/sync", get(ws_handler))
        // Content-addressed artifact fetch: the id IS the content, so the
        // response is immutable — the browser HTTP cache becomes the
        // artifact store (this route sets its own Cache-Control; the
        // global layer only fills in where absent).
        .route("/artifact/{id}", get(artifact_handler))
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
        // (if_not_present: the artifact route sets immutable itself.)
        .layer(SetResponseHeaderLayer::if_not_present(
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
    // Late-joiner bootstrap: current canonical state (fresh — a stale
    // hello would strand the joiner between snapshot and missed deltas).
    let (hello, mut rx) = {
        let mut shared = app.shared.lock().await;
        (shared.fresh_bytes(), app.tx.subscribe())
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
                    // Lagged past deltas: skip to the freshest full state.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let bytes = app.shared.lock().await.fresh_bytes();
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
                match data.first() {
                    // Steady state: a delta of authored ops. Apply (opens
                    // unknown objects from kind+origin, idempotent) and
                    // relay the RAW bytes to every client — no re-encode.
                    Some(&DELTA_TAG) => {
                        let ok = {
                            let mut shared = app.shared.lock().await;
                            match apply_delta(&mut shared.web, &data) {
                                Ok(_) => {
                                    shared.dirty = true;
                                    true
                                }
                                Err(e) => {
                                    eprintln!("[sync] bad delta ({} bytes): {e:?}", data.len());
                                    false
                                }
                            }
                        };
                        if ok {
                            let _ = app.tx.send(Arc::new(data.to_vec()));
                        }
                    }
                    // An artifact travels ONCE, at upload: store + relay.
                    Some(&ARTIFACT_TAG) => {
                        {
                            let mut shared = app.shared.lock().await;
                            shared.web.provide_artifact_bytes(data[1..].to_vec());
                            shared.dirty = true;
                        }
                        let _ = app.tx.send(Arc::new(data.to_vec()));
                    }
                    // Anything else: a snapshot (hello-resync from a
                    // reconnecting client, or an older client's whole-state
                    // exchange). Merge/quiesce — but ALL comparisons happen
                    // in ops-encoding space: a legacy client sends full
                    // snapshots, and comparing its bytes to our ops bytes
                    // directly would never quiesce (a 7MB ping-pong).
                    _ => {
                        let theirs = match decode_hashweb(&data) {
                            Ok(web) => web,
                            Err(e) => {
                                eprintln!("[sync] rejecting undecodable snapshot ({} bytes): {e:?}", data.len());
                                continue;
                            }
                        };
                        let theirs_ops = encode_hashweb_ops(&theirs);
                        let (changed, reply) = {
                            let mut shared = app.shared.lock().await;
                            let before = shared.fresh_bytes();
                            shared.web.merge(theirs);
                            let merged_ops = encode_hashweb_ops(&shared.web);
                            if merged_ops != before {
                                // The client taught us something: everyone
                                // hears the new state; disk flushes via the
                                // dirty task (artifacts may have arrived too).
                                shared.bytes = merged_ops.clone();
                                shared.dirty = true;
                                (Some(merged_ops), None)
                            } else if theirs_ops != merged_ops {
                                // Client is strictly behind: catch it up.
                                (None, Some(merged_ops))
                            } else {
                                (None, None) // converged — silence ends the exchange
                            }
                        };
                        if let Some(merged) = changed {
                            let _ = app.tx.send(Arc::new(merged));
                        }
                        if let Some(bytes) = reply {
                            if socket.send(Message::Binary(bytes.into())).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn artifact_handler(
    axum::extract::Path(id_hex): axum::extract::Path<String>,
    State(app): State<Arc<App>>,
) -> impl IntoResponse {
    let Ok(raw) = hex::decode(&id_hex) else {
        return (axum::http::StatusCode::BAD_REQUEST, "bad id").into_response();
    };
    if raw.len() != 32 {
        return (axum::http::StatusCode::BAD_REQUEST, "bad id").into_response();
    }
    let mut id = [0u8; 32];
    id.copy_from_slice(&raw);
    let bytes = {
        let shared = app.shared.lock().await;
        shared.web.artifact_bytes(&hashseq::Id(id)).cloned()
    };
    match bytes {
        Some(b) => (
            [
                ("content-type", "application/octet-stream"),
                // Content-addressed: the id commits to the bytes forever.
                ("cache-control", "public, max-age=31536000, immutable"),
            ],
            b,
        )
            .into_response(),
        None => (axum::http::StatusCode::NOT_FOUND, "unknown artifact").into_response(),
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
