//! Shared bridge-client used by every non-bridge subcommand.
//!
//! Connects to `$XDG_RUNTIME_DIR/iris.sock`, sends JSON-lines requests,
//! demuxes responses by their request `id`, fans events into a broadcast
//! channel that callers can subscribe to.
//!
//! Threading model: one tokio task owns the socket's read half and pumps
//! every line either to a per-request `oneshot` (responses) or the
//! broadcast (events). Callers `await` on the `oneshot` returned by
//! `request()`. Drops on `IrisClient::drop` abort the read task and the
//! writer goes out of scope.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow};
use directories::BaseDirs;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::OwnedWriteHalf;
use tokio::sync::{Mutex, broadcast, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::bridge::proto::{Event, Op, Request, ServerMessage};

pub mod proto_re;

/// Capacity for the event broadcast. Same rationale as the bridge-side
/// channel: lossy on slow consumers, but generous (256) for a personal
/// tool. Lagged subscribers receive `RecvError::Lagged` and can choose to
/// re-subscribe.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// How long `request()` waits for a matching response before erroring.
/// Bridge ops are local UDS round-trips; anything over a few seconds is a
/// real problem. 10s is forgiving.
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

pub struct IrisClient {
    writer: Arc<Mutex<OwnedWriteHalf>>,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<ResponseOutcome>>>>,
    events_tx: broadcast::Sender<Event>,
    next_id: AtomicU64,
    read_task: JoinHandle<()>,
}

/// What the read task hands back over the per-request `oneshot`. We keep
/// the `Result` shape so `request()` can convert a bridge `error` into a
/// typed `Err`, while a transport-level failure (server hung up before
/// responding) becomes a different `Err`.
enum ResponseOutcome {
    /// Bridge replied with `ok: true`; payload is `data` (or `null`).
    Ok(Value),
    /// Bridge replied with `ok: false`; payload is the error string.
    Err(String),
}

impl IrisClient {
    /// Connect to the iris bridge at the default socket path.
    pub async fn connect() -> Result<Self> {
        Self::connect_at(default_socket_path()?).await
    }

    /// Connect to a specific socket path. Tests use this with a tempfile
    /// path; production callers go through `connect()`.
    pub async fn connect_at(path: PathBuf) -> Result<Self> {
        let stream = UnixStream::connect(&path)
            .await
            .with_context(|| format!("connecting to iris bridge at {}", path.display()))?;
        let (read_half, write_half) = stream.into_split();
        let writer = Arc::new(Mutex::new(write_half));
        let pending: Arc<Mutex<HashMap<String, oneshot::Sender<ResponseOutcome>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let (events_tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);

        let read_pending = pending.clone();
        let read_events = events_tx.clone();
        let read_task = tokio::spawn(async move {
            let mut lines = BufReader::new(read_half).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        if let Err(e) = dispatch_line(&line, &read_pending, &read_events).await {
                            warn!("bridge sent unparsable line: {e:#} ({line})");
                        }
                    }
                    Ok(None) => {
                        debug!("bridge closed the connection");
                        break;
                    }
                    Err(e) => {
                        warn!("read from bridge failed: {e:#}");
                        break;
                    }
                }
            }
            // Bridge is gone — drain any pending oneshots so callers
            // observe the disconnect instead of hanging on the awaits.
            let mut p = read_pending.lock().await;
            for (_, tx) in p.drain() {
                let _ = tx.send(ResponseOutcome::Err(
                    "bridge connection closed before response arrived".into(),
                ));
            }
        });

        Ok(Self {
            writer,
            pending,
            events_tx,
            next_id: AtomicU64::new(1),
            read_task,
        })
    }

    /// Send a request, await the matching response. Returns the `data`
    /// payload on success, or an error containing the bridge's `error`
    /// string on `ok: false`.
    pub async fn request(&self, op: Op) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed).to_string();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);

        // Pre-serialize so a serde error on a malformed Op also cleans up.
        // Insert-then-write means we MUST clean up on every error path
        // before the await on `rx`; otherwise a write/serde failure leaks
        // the pending entry until Drop.
        let do_write = async {
            let req = Request { id: id.clone(), op };
            let line = serde_json::to_string(&req)?;
            let mut w = self.writer.lock().await;
            w.write_all(line.as_bytes()).await?;
            w.write_all(b"\n").await?;
            Ok::<(), anyhow::Error>(())
        };
        if let Err(e) = do_write.await {
            self.pending.lock().await.remove(&id);
            return Err(e);
        }

        let outcome = match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
            Ok(Ok(o)) => o,
            Ok(Err(_)) => {
                // Sender dropped — read task ended, bridge is gone. The
                // read task already drained `pending` on its way out, so
                // there's nothing more to remove here.
                anyhow::bail!("bridge connection lost before response");
            }
            Err(_) => {
                // Timeout — clean up our pending entry so it doesn't leak.
                self.pending.lock().await.remove(&id);
                anyhow::bail!("bridge request timed out after {REQUEST_TIMEOUT:?}");
            }
        };

        match outcome {
            ResponseOutcome::Ok(v) => Ok(v),
            ResponseOutcome::Err(e) => Err(anyhow!("bridge: {e}")),
        }
    }

    /// Subscribe to one or more event topics. Returns a `broadcast::Receiver`
    /// that yields `Event`s — caller is responsible for re-subscribing on
    /// `RecvError::Lagged` if it wants to keep up.
    ///
    /// The first `subscribe()` call sends `Op::Subscribe` to the bridge;
    /// subsequent calls also send `Op::Subscribe` for the requested
    /// topics — the bridge keeps a set, so duplicate subscriptions are
    /// idempotent. We don't deduplicate client-side.
    pub async fn subscribe(&self, topics: &[&str]) -> Result<broadcast::Receiver<Event>> {
        let rx = self.events_tx.subscribe();
        let topic_strings: Vec<String> = topics.iter().map(|s| (*s).into()).collect();
        self.request(Op::Subscribe { topics: topic_strings }).await?;
        Ok(rx)
    }

    /// Unsubscribe from topics on the bridge side. The local broadcast
    /// receiver stays valid for any other subscribed topics; if it was
    /// the only subscription, you can drop the receiver.
    pub async fn unsubscribe(&self, topics: &[&str]) -> Result<()> {
        let topic_strings: Vec<String> = topics.iter().map(|s| (*s).into()).collect();
        self.request(Op::Unsubscribe { topics: topic_strings }).await?;
        Ok(())
    }
}

impl Drop for IrisClient {
    fn drop(&mut self) {
        self.read_task.abort();
    }
}

async fn dispatch_line(
    line: &str,
    pending: &Arc<Mutex<HashMap<String, oneshot::Sender<ResponseOutcome>>>>,
    events_tx: &broadcast::Sender<Event>,
) -> Result<()> {
    let msg: ServerMessage = serde_json::from_str(line)?;
    match msg {
        ServerMessage::Response(resp) => {
            if let Some(tx) = pending.lock().await.remove(&resp.id) {
                let outcome = if resp.ok {
                    ResponseOutcome::Ok(resp.data.unwrap_or(Value::Null))
                } else {
                    ResponseOutcome::Err(resp.error.unwrap_or_else(|| "unknown error".into()))
                };
                // Caller may have given up (timeout / cancellation); ignore
                // the send error.
                let _ = tx.send(outcome);
            } else {
                // Response with no matching pending entry. The bridge
                // shouldn't do this, but invalid-JSON requests echo back
                // with id "<unparsed>" — log and ignore.
                debug!("dropping unmatched response id={}", resp.id);
            }
        }
        ServerMessage::Event(ev) => {
            // best-effort: if no subscribers are listening we drop.
            let _ = events_tx.send(ev);
        }
    }
    Ok(())
}

fn default_socket_path() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        return Ok(PathBuf::from(dir).join("iris.sock"));
    }
    if let Some(dirs) = BaseDirs::new() {
        if let Some(rd) = dirs.runtime_dir() {
            return Ok(rd.join("iris.sock"));
        }
    }
    anyhow::bail!("XDG_RUNTIME_DIR not set and no runtime dir fallback available")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::proto::{Response, ServerMessage};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::net::UnixListener;

    /// Spin up a fake bridge that reads one line, decodes the request id,
    /// sends back a canned response. Returns the connected client + tmp.
    async fn fake_bridge_responding(
        responder: impl FnOnce(&str) -> ServerMessage + Send + 'static,
    ) -> (TempDir, IrisClient, JoinHandle<()>) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&path).unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (r, w) = stream.into_split();
            let mut lines = BufReader::new(r).lines();
            let line = lines.next_line().await.unwrap().unwrap();
            let resp = responder(&line);
            let mut w = w;
            let s = serde_json::to_string(&resp).unwrap();
            w.write_all(s.as_bytes()).await.unwrap();
            w.write_all(b"\n").await.unwrap();
            // Hold the connection open until the test drops the client so
            // the read task doesn't see EOF mid-test.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = IrisClient::connect_at(path).await.unwrap();
        (tmp, client, server)
    }

    #[tokio::test]
    async fn request_response_round_trip() {
        let (_tmp, client, _server) = fake_bridge_responding(|line| {
            let req: Request = serde_json::from_str(line).unwrap();
            ServerMessage::Response(Response::ok(&req.id, serde_json::json!({"hello": "world"})))
        })
        .await;

        let data = client.request(Op::Noop).await.unwrap();
        assert_eq!(data["hello"], "world");
    }

    #[tokio::test]
    async fn bridge_error_surfaces_as_err() {
        let (_tmp, client, _server) = fake_bridge_responding(|line| {
            let req: Request = serde_json::from_str(line).unwrap();
            ServerMessage::Response(Response::err(&req.id, "some bridge problem"))
        })
        .await;

        let err = client.request(Op::Noop).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("some bridge problem"), "got: {msg}");
    }

    #[tokio::test]
    async fn dropped_connection_unblocks_pending_request() {
        // Server accepts then immediately closes — request should error,
        // not hang forever.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let _server = tokio::spawn(async move {
            let _ = listener.accept().await;
            // Drop both halves immediately by letting the function return.
        });

        let client = IrisClient::connect_at(path).await.unwrap();
        let err = client.request(Op::Noop).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("connection") || msg.contains("lost") || msg.contains("closed"),
            "expected disconnect error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn in_flight_request_unblocks_when_bridge_closes() {
        // Bridge accepts and reads the request line, then closes WITHOUT
        // sending a response. The read task on the client side hits EOF
        // and must drain `pending` so the awaiting `request()` errors
        // promptly instead of waiting out REQUEST_TIMEOUT.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let _server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (r, _w) = stream.into_split();
            let mut lines = BufReader::new(r).lines();
            let _ = lines.next_line().await;
            // Drop both halves -> client sees EOF.
        });

        let client = IrisClient::connect_at(path).await.unwrap();
        let start = std::time::Instant::now();
        let err = client.request(Op::Noop).await.unwrap_err();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(3),
            "request should fast-fail on EOF, took {elapsed:?}"
        );
        let msg = format!("{err:#}");
        assert!(
            msg.contains("connection lost") || msg.contains("closed"),
            "expected disconnect error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn subscribe_yields_events() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let _server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (r, w) = stream.into_split();
            let mut lines = BufReader::new(r).lines();
            // Read the subscribe request, ack it, then push an event.
            let line = lines.next_line().await.unwrap().unwrap();
            let req: Request = serde_json::from_str(&line).unwrap();
            let resp = ServerMessage::Response(Response::ok(&req.id, serde_json::json!({})));
            let mut w = w;
            let s = serde_json::to_string(&resp).unwrap();
            w.write_all(s.as_bytes()).await.unwrap();
            w.write_all(b"\n").await.unwrap();
            let ev = ServerMessage::Event(Event {
                event: "focus".into(),
                ts: 0,
                data: serde_json::json!({"focused_window_id": 7}),
            });
            let s = serde_json::to_string(&ev).unwrap();
            w.write_all(s.as_bytes()).await.unwrap();
            w.write_all(b"\n").await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = IrisClient::connect_at(path).await.unwrap();
        let mut events = client.subscribe(&["focus"]).await.unwrap();
        let ev = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .expect("did not receive event in time")
            .unwrap();
        assert_eq!(ev.event, "focus");
        assert_eq!(ev.data["focused_window_id"], 7);
    }
}
