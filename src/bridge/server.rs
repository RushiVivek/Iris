//! Unix-socket server. Each accepted connection runs as one tokio task that
//! does three things concurrently:
//!  1. Reads JSON-lines `Request`s, dispatches to the op handler.
//!  2. Receives broadcast `Event`s, filters by the client's subscribed
//!     topics, writes them out as JSON lines.
//!  3. Exits when either side closes.
//!
//! Framing: line-delimited JSON. No length prefix. UTF-8.

#![allow(dead_code)]

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use niri_ipc::{Action, Request as NiriRequest, Response as NiriResponse};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::niri_conn::query;
use super::proto::{self, Op, Request, Response, ServerMessage};
use super::state::{ClientSubs, SharedState};

pub async fn spawn(sock_path: PathBuf, state: SharedState) -> Result<JoinHandle<()>> {
    let listener = UnixListener::bind(&sock_path)
        .with_context(|| format!("binding {}", sock_path.display()))?;
    // Make the socket user-only. The bind already happened with the process
    // umask; explicit chmod is a defense in case umask is weird.
    let perms = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(&sock_path, perms)
        .with_context(|| format!("chmod 0600 {}", sock_path.display()))?;

    info!(socket = %sock_path.display(), "iris bridge listening");

    Ok(tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let st = state.clone();
                    tokio::spawn(async move {
                        let id = next_client_id();
                        debug!(client = id, "client connected");
                        if let Err(e) = handle_client(stream, st).await {
                            warn!(client = id, "client task ended: {e:#}");
                        } else {
                            debug!(client = id, "client disconnected");
                        }
                    });
                }
                Err(e) => {
                    error!("accept failed: {e:#}");
                    // Brief pause to avoid spinning on a broken listener.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }))
}

fn next_client_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(1);
    N.fetch_add(1, Ordering::Relaxed)
}

/// Per-client task. Splits the stream into a read half (consumes Requests)
/// and a write half (sends Responses + Events). The write half is shared
/// between the request-handler branch and the event-pump branch via a
/// `Mutex<WriteHalf>`.
async fn handle_client(stream: UnixStream, state: SharedState) -> Result<()> {
    let (read_half, write_half) = stream.into_split();
    let writer = Arc::new(Mutex::new(write_half));
    let subs = Arc::new(Mutex::new(ClientSubs::default()));

    // Event pump: receives broadcast events, filters by topic, writes them.
    let event_writer = writer.clone();
    let event_subs = subs.clone();
    let mut event_rx = state.subscribe();
    let pump = tokio::spawn(async move {
        loop {
            match event_rx.recv().await {
                Ok(ev) => {
                    if event_subs.lock().await.matches(&ev.event)
                        && write_event(&event_writer, &ev).await.is_err()
                    {
                        // Client write failed — they're gone. Stop pumping.
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    warn!("client lagged {n} events; emitting state.reset hint");
                    let hint = proto::Event {
                        event: proto::topics::STATE.to_string(),
                        ts: now_ms(),
                        data: json!({"lagged": n}),
                    };
                    if write_event(&event_writer, &hint).await.is_err() {
                        break;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Request loop: read JSON lines, dispatch ops, write responses.
    let mut lines = BufReader::new(read_half).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let req: Request = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::err(
                    "<unparsed>",
                    format!("invalid request JSON: {e}"),
                );
                let _ = write_response(&writer, &resp).await;
                continue;
            }
        };
        let resp = dispatch_op(&state, &subs, &req).await;
        if write_response(&writer, &resp).await.is_err() {
            break;
        }
    }

    pump.abort();
    Ok(())
}

async fn write_response(
    writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
    resp: &Response,
) -> Result<()> {
    let line = serde_json::to_string(&ServerMessage::Response(resp.clone()))?;
    let mut w = writer.lock().await;
    w.write_all(line.as_bytes()).await?;
    w.write_all(b"\n").await?;
    Ok(())
}

async fn write_event(
    writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
    ev: &proto::Event,
) -> Result<()> {
    let line = serde_json::to_string(&ServerMessage::Event(ev.clone()))?;
    let mut w = writer.lock().await;
    w.write_all(line.as_bytes()).await?;
    w.write_all(b"\n").await?;
    Ok(())
}

// We need Clone on Response for write_response to take &Response. Add it.
impl Clone for Response {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            ok: self.ok,
            data: self.data.clone(),
            error: self.error.clone(),
        }
    }
}

// ─────────────────────────────── Op dispatch ──────────────────────────────────

async fn dispatch_op(
    state: &SharedState,
    subs: &Arc<Mutex<ClientSubs>>,
    req: &Request,
) -> Response {
    match handle_op(state, subs, req).await {
        Ok(data) => Response::ok(&req.id, data),
        Err(e) => Response::err(&req.id, format!("{e:#}")),
    }
}

async fn handle_op(
    state: &SharedState,
    subs: &Arc<Mutex<ClientSubs>>,
    req: &Request,
) -> Result<serde_json::Value> {
    match &req.op {
        Op::Noop => Ok(json!({})),

        Op::WindowsList => {
            let windows: Vec<_> = state
                .with(|s| s.windows.values().cloned().collect())
                .await;
            Ok(json!(windows))
        }
        Op::WindowsGet { id } => {
            let w = state.with(|s| s.windows.get(id).cloned()).await;
            Ok(json!(w))
        }
        Op::WorkspacesList => {
            let workspaces: Vec<_> = state
                .with(|s| s.workspaces.values().cloned().collect())
                .await;
            Ok(json!(workspaces))
        }
        Op::WorkspacesFocused => {
            let focused = state
                .with(|s| {
                    s.focused_workspace
                        .and_then(|id| s.workspaces.get(&id).cloned())
                })
                .await;
            Ok(json!(focused))
        }
        Op::StateSnapshot => {
            let snap = state
                .with(|s| {
                    json!({
                        "windows": s.windows.values().cloned().collect::<Vec<_>>(),
                        "workspaces": s.workspaces.values().cloned().collect::<Vec<_>>(),
                        "focused_window_id": s.focused_window,
                        "focused_workspace_id": s.focused_workspace,
                    })
                })
                .await;
            Ok(snap)
        }

        Op::WindowFocus { id } => {
            let action = Action::FocusWindow { id: *id };
            forward_action(action).await?;
            Ok(json!({}))
        }
        Op::WindowClose { id } => {
            let action = Action::CloseWindow { id: Some(*id) };
            forward_action(action).await?;
            Ok(json!({}))
        }
        Op::WindowMoveToWorkspace { id, workspace } => {
            // Resolve workspace ref → niri's WorkspaceReferenceArg.
            let ws_ref = match workspace {
                proto::WorkspaceRef::Id { id } => {
                    niri_ipc::WorkspaceReferenceArg::Id(*id)
                }
                proto::WorkspaceRef::Idx { idx } => {
                    niri_ipc::WorkspaceReferenceArg::Index(*idx)
                }
            };
            let action = Action::MoveWindowToWorkspace {
                window_id: Some(*id),
                reference: ws_ref,
                focus: true,
            };
            forward_action(action).await?;
            Ok(json!({}))
        }
        Op::WindowToggleFloating { id } => {
            let action = Action::ToggleWindowFloating { id: Some(*id) };
            forward_action(action).await?;
            Ok(json!({}))
        }

        Op::Subscribe { topics } => {
            let mut s = subs.lock().await;
            for t in topics {
                s.topics.insert(t.clone());
            }
            Ok(json!({"subscribed": s.topics.iter().collect::<Vec<_>>()}))
        }
        Op::Unsubscribe { topics } => {
            let mut s = subs.lock().await;
            for t in topics {
                s.topics.remove(t);
            }
            Ok(json!({"subscribed": s.topics.iter().collect::<Vec<_>>()}))
        }
    }
}

async fn forward_action(action: Action) -> Result<()> {
    let resp = query(NiriRequest::Action(action)).await?;
    match resp {
        NiriResponse::Handled => Ok(()),
        other => anyhow::bail!("unexpected niri response: {other:?}"),
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
