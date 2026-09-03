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
//! and writes at most every few seconds (artifacts flush immediately —
//! a client pushes those bytes exactly once); hello/lagged/catch-up
//! replies always encode fresh when the wire cache is stale (a stale
//! hello would strand a joiner between the snapshot and the deltas it
//! missed), and the encoded snapshot is cached until the next change.
//!
//! Write caps (no auth — anyone reaching the port can write): frames
//! are capped at `MAX_WS_MESSAGE`, artifacts at `MAX_ARTIFACT_BYTES`
//! each and `HASHWEB_MAX_ARTIFACT_BYTES` in total, objects at
//! `MAX_NEW_OBJECTS_PER_DELTA` per delta and `HASHWEB_MAX_OBJECTS`
//! in total.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::sync::{broadcast, Mutex};
use tower_http::services::ServeDir;
use tower_http::set_header::SetResponseHeaderLayer;

use hashseq::encoding::{
    apply_delta, decode_hashweb, decode_id, decode_varint, encode_hashweb, encode_hashweb_ops,
    ARTIFACT_TAG, DELTA_TAG,
};
use hashseq::value::{value_id_of_bytes, KIND_KV, KIND_SEQ};
use hashseq::{object_id, HashWeb};

/// Largest WebSocket message / frame accepted (the client caps images at
/// 1.5 MB; snapshots of a real KB are well under this).
const MAX_WS_MESSAGE: usize = 4 << 20;
/// Largest single artifact (0xAF frame payload) accepted.
const MAX_ARTIFACT_BYTES: usize = 2 << 20;
/// Most objects one delta may open.
const MAX_NEW_OBJECTS_PER_DELTA: usize = 256;
const DEFAULT_MAX_ARTIFACT_TOTAL: usize = 512 << 20;
const DEFAULT_MAX_OBJECTS: usize = 200_000;

struct Shared {
    web: HashWeb,
    /// Cached OPS-ONLY wire snapshot (hello / resync / quiesce compare),
    /// valid while `wire_stale` is false. Artifact bytes never ride
    /// snapshots on the wire — they are pushed once (0xAF) or fetched by
    /// GET /artifact/:id, immutable-cached.
    bytes: Bytes,
    /// Ops landed since `bytes` was encoded.
    wire_stale: bool,
    /// Ops or artifacts landed since the state file was written.
    /// Cleared by `flush` (disk gets the FULL encoding; the wire never
    /// does); re-set if the write fails.
    dirty: bool,
    /// Approximate artifact-store size (full encoding minus ops
    /// encoding, re-derived on every flush; bumped on ingest between).
    artifact_total: usize,
}

impl Shared {
    /// Current ops-wire bytes, re-encoded only if something landed since
    /// the last caller asked.
    fn fresh_bytes(&mut self) -> Bytes {
        if self.wire_stale {
            self.bytes = Bytes::from(encode_hashweb_ops(&self.web));
            self.wire_stale = false;
        }
        self.bytes.clone()
    }
}

struct App {
    shared: Mutex<Shared>,
    /// Serialises flushes (tick / artifact ingest / shutdown) so two
    /// writers never race on the tmp file.
    persist_lock: Mutex<()>,
    tx: broadcast::Sender<Bytes>,
    state_path: PathBuf,
    max_artifact_total: usize,
    max_objects: usize,
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(v) => v.trim().parse().unwrap_or_else(|_| panic!("{name}={v:?} is not a number")),
        Err(_) => default,
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    // A panic anywhere (persist task, a connection task) must not leave
    // the process serving with persistence silently off: die loudly and
    // let systemd restart us.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_hook(info);
        eprintln!("[sync] panic: {info}; aborting");
        std::process::abort();
    }));

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
    let max_artifact_total = env_usize("HASHWEB_MAX_ARTIFACT_BYTES", DEFAULT_MAX_ARTIFACT_TOTAL);
    let max_objects = env_usize("HASHWEB_MAX_OBJECTS", DEFAULT_MAX_OBJECTS);

    let web = match std::fs::read(&state_path) {
        Ok(bytes) => match decode_hashweb(&bytes) {
            Ok(web) => {
                eprintln!("[sync] loaded state: {} bytes, {} objects", bytes.len(), web.object_count());
                web
            }
            Err(e) => {
                // Never clobber an undecodable file on the next flush:
                // set it aside so it can be inspected / recovered.
                let aside = PathBuf::from(format!("{}.corrupt-{}", state_path.display(), unix_now()));
                if let Err(re) = std::fs::rename(&state_path, &aside) {
                    eprintln!(
                        "[sync] state file undecodable ({e:?}) and could not be moved aside to {}: {re}; refusing to start",
                        aside.display()
                    );
                    std::process::exit(1);
                }
                eprintln!(
                    "[sync] state file undecodable ({e:?}); moved to {}; starting empty",
                    aside.display()
                );
                HashWeb::new()
            }
        },
        Err(_) => {
            eprintln!("[sync] no state file; starting empty");
            HashWeb::new()
        }
    };
    let ops = encode_hashweb_ops(&web);
    let artifact_total = encode_hashweb(&web).len().saturating_sub(ops.len());
    eprintln!(
        "[sync] caps: artifact store {} MiB (~{} MiB used), objects {} ({} used)",
        max_artifact_total >> 20,
        artifact_total >> 20,
        max_objects,
        web.object_count()
    );
    let (tx, _) = broadcast::channel(1024);
    let app = Arc::new(App {
        shared: Mutex::new(Shared {
            web,
            bytes: Bytes::from(ops),
            wire_stale: false,
            dirty: false,
            artifact_total,
        }),
        persist_lock: Mutex::new(()),
        tx,
        state_path,
        max_artifact_total,
        max_objects,
    });

    // Debounced persistence: deltas mark dirty; this task folds them into
    // the canonical state file at most every 3 seconds.
    let persist_task = {
        let app = app.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3));
            loop {
                tick.tick().await;
                flush(&app).await;
            }
        })
    };

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
        .with_state(app.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    eprintln!("[sync] listening on http://{addr}/");
    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    let mut persist_task = persist_task;
    tokio::select! {
        r = axum::serve(listener, router) => {
            r.expect("serve");
        }
        // The persist loop never returns; if it does, something is
        // wrong enough that continuing without persistence is worse
        // than a restart.
        r = &mut persist_task => {
            eprintln!("[sync] persist task ended unexpectedly: {r:?}; aborting");
            std::process::abort();
        }
        _ = shutdown_signal() => {
            // Open websockets would keep a hyper graceful shutdown
            // waiting forever; flush the state file and exit instead.
            eprintln!("[sync] shutdown signal; flushing");
            flush(&app).await;
            eprintln!("[sync] flushed; exiting");
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                eprintln!("[sync] cannot listen for SIGTERM: {e}");
                std::future::pending::<()>().await
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}

/// Fold dirty state into the state file (full encoding, artifacts
/// included). Serialised by `persist_lock`; on a failed write the state
/// stays dirty so the next tick retries.
async fn flush(app: &App) {
    let _guard = app.persist_lock.lock().await;
    let full = {
        let mut shared = app.shared.lock().await;
        if !shared.dirty {
            return;
        }
        let full = encode_hashweb(&shared.web);
        let ops_len = shared.fresh_bytes().len();
        shared.artifact_total = full.len().saturating_sub(ops_len);
        shared.dirty = false;
        full
    };
    if let Err(e) = persist(&app.state_path, &full).await {
        eprintln!("[sync] persist failed: {e}; state stays dirty");
        app.shared.lock().await.dirty = true;
    }
}

/// Walk a delta's group headers and count the objects it would open,
/// validating framing on the way (the full op decode happens in
/// `apply_delta`). Lets the object cap be enforced BEFORE any state
/// mutates.
fn delta_new_objects(web: &HashWeb, bytes: &[u8]) -> Result<usize, hashseq::encoding::DecodeError> {
    use hashseq::encoding::DecodeError;
    let mut pos = 1;
    let mut new = 0usize;
    while pos < bytes.len() {
        let kind = bytes[pos];
        pos += 1;
        if kind != KIND_SEQ && kind != KIND_KV {
            return Err(DecodeError::InvalidOpTag(kind));
        }
        let (origin, used) = decode_id(&bytes[pos..])?;
        pos += used;
        let obj = object_id(kind, &origin);
        if web.seq(&obj).is_none() && web.kv(&obj).is_none() {
            new += 1;
        }
        let (n, used) = decode_varint(&bytes[pos..])?;
        pos += used;
        for _ in 0..n {
            let (len, used) = decode_varint(&bytes[pos..])?;
            pos += used;
            if len > bytes.len() - pos {
                return Err(DecodeError::UnexpectedEof);
            }
            pos += len;
        }
    }
    Ok(new)
}

async fn ws_handler(ws: WebSocketUpgrade, State(app): State<Arc<App>>) -> impl IntoResponse {
    ws.max_message_size(MAX_WS_MESSAGE)
        .max_frame_size(MAX_WS_MESSAGE)
        .on_upgrade(move |socket| client_loop(socket, app))
}

async fn client_loop(mut socket: WebSocket, app: Arc<App>) {
    // Late-joiner bootstrap: current canonical state (fresh — a stale
    // hello would strand the joiner between snapshot and missed deltas).
    let (hello, mut rx) = {
        let mut shared = app.shared.lock().await;
        (shared.fresh_bytes(), app.tx.subscribe())
    };
    if socket.send(Message::Binary(hello)).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            broadcastd = rx.recv() => {
                match broadcastd {
                    Ok(bytes) => {
                        if socket.send(Message::Binary(bytes)).await.is_err() {
                            return;
                        }
                    }
                    // Lagged past deltas: skip to the freshest full state.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[sync] client lagged {n} messages; resyncing with a snapshot");
                        let bytes = app.shared.lock().await.fresh_bytes();
                        if socket.send(Message::Binary(bytes)).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            incoming = socket.recv() => {
                let msg = match incoming {
                    Some(Ok(msg)) => msg,
                    // Includes oversized frames (tungstenite Capacity
                    // error): the connection is dropped.
                    Some(Err(e)) => {
                        eprintln!("[sync] closing connection: {e}");
                        return;
                    }
                    None => return,
                };
                let Message::Binary(data) = msg else { continue };
                match data.first() {
                    // Steady state: a delta of authored ops. Apply (opens
                    // unknown objects from kind+origin, idempotent) and
                    // relay the RAW bytes to every client — no re-encode.
                    Some(&DELTA_TAG) => {
                        let ok = {
                            let mut shared = app.shared.lock().await;
                            let new_objects = match delta_new_objects(&shared.web, &data) {
                                Ok(n) => n,
                                Err(e) => {
                                    eprintln!("[sync] bad delta framing ({} bytes): {e:?}", data.len());
                                    continue;
                                }
                            };
                            if new_objects > MAX_NEW_OBJECTS_PER_DELTA {
                                eprintln!(
                                    "[sync] rejecting delta opening {new_objects} objects (cap {MAX_NEW_OBJECTS_PER_DELTA} per delta)"
                                );
                                continue;
                            }
                            let total = shared.web.object_count() + new_objects;
                            if new_objects > 0 && total > app.max_objects {
                                eprintln!(
                                    "[sync] rejecting delta: {total} objects would exceed cap {} (HASHWEB_MAX_OBJECTS)",
                                    app.max_objects
                                );
                                continue;
                            }
                            match apply_delta(&mut shared.web, &data) {
                                Ok(_) => {
                                    shared.wire_stale = true;
                                    shared.dirty = true;
                                    true
                                }
                                Err(e) => {
                                    eprintln!("[sync] bad delta ({} bytes): {e:?}", data.len());
                                    // Groups before the bad one already
                                    // landed: persist what we hold.
                                    shared.wire_stale = true;
                                    shared.dirty = true;
                                    false
                                }
                            }
                        };
                        if ok {
                            let _ = app.tx.send(data);
                        }
                    }
                    // An artifact travels ONCE, at upload: store, flush
                    // to disk right away (the client will not resend
                    // it), then relay.
                    Some(&ARTIFACT_TAG) => {
                        let payload = &data[1..];
                        if payload.len() > MAX_ARTIFACT_BYTES {
                            eprintln!(
                                "[sync] rejecting artifact of {} bytes (cap {MAX_ARTIFACT_BYTES})",
                                payload.len()
                            );
                            continue;
                        }
                        let stored = {
                            let mut shared = app.shared.lock().await;
                            let vid = value_id_of_bytes(payload);
                            if shared.web.artifact_bytes(&vid).is_some() {
                                true // already held: relay is still harmless
                            } else if shared.artifact_total + payload.len() > app.max_artifact_total {
                                eprintln!(
                                    "[sync] rejecting artifact {}: store at ~{} of {} bytes (HASHWEB_MAX_ARTIFACT_BYTES)",
                                    hex::encode(vid.0),
                                    shared.artifact_total,
                                    app.max_artifact_total
                                );
                                false
                            } else {
                                shared.web.provide_artifact_bytes(payload.to_vec());
                                shared.artifact_total += payload.len();
                                shared.dirty = true;
                                true
                            }
                        };
                        if stored {
                            flush(&app).await;
                            let _ = app.tx.send(data);
                        }
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
                            let objects_after = {
                                // Union of object sets: count what the
                                // snapshot would add before merging.
                                let known = |id: &hashseq::Id| shared.web.seq(id).is_some() || shared.web.kv(id).is_some();
                                shared.web.object_count() + theirs.objects().filter(|id| !known(id)).count()
                            };
                            if objects_after > app.max_objects {
                                eprintln!(
                                    "[sync] rejecting snapshot: {objects_after} objects would exceed cap {} (HASHWEB_MAX_OBJECTS)",
                                    app.max_objects
                                );
                                continue;
                            }
                            shared.web.merge(theirs);
                            let merged_ops = Bytes::from(encode_hashweb_ops(&shared.web));
                            if merged_ops != before {
                                // The client taught us something: everyone
                                // hears the new state; disk flushes via the
                                // dirty task (artifacts may have arrived too).
                                shared.bytes = merged_ops.clone();
                                shared.wire_stale = false;
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
                            let _ = app.tx.send(merged);
                        }
                        if let Some(bytes) = reply {
                            if socket.send(Message::Binary(bytes)).await.is_err() {
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

/// Atomic, durable replace: write tmp, fsync it, rename over the state
/// file, fsync the directory so the rename itself survives a crash.
async fn persist(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, bytes).await?;
    tokio::fs::File::open(&tmp).await?.sync_all().await?;
    tokio::fs::rename(&tmp, path).await?;
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    tokio::fs::File::open(&dir).await?.sync_all().await?;
    Ok(())
}
