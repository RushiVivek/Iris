//! Owns the connection(s) to niri's IPC socket.
//!
//! niri-ipc 25.x exposes a *blocking* `Socket`. We use two of them:
//!  - one in a blocking thread that subscribes to `Request::EventStream`
//!    and pumps every event back to tokio via an mpsc channel; this drives
//!    the cached state and the broadcast fan-out.
//!  - a second one re-opened per query for `query()` (cheap; niri's UDS
//!    accept is fast and we do this only on action ops, not on hot paths).
//!
//! Reconnect: if the event-stream socket dies (niri restart, crash), we
//! sleep with backoff and reconnect. On reconnect we emit a synthetic
//! `state` event (`{"reset": true}`) so subscribers know to re-fetch.

#![allow(dead_code)]

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use niri_ipc::{Event as NiriEvent, Reply, Request, Response, socket::Socket};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use super::proto::{self, topics};
use super::state::SharedState;

/// Spawn the long-running task that owns the niri event stream + drives
/// `state`. Returns a `JoinHandle` that completes only on a fatal error
/// (we never give up — the loop reconnects forever).
pub async fn spawn_niri_loop(state: SharedState) -> Result<JoinHandle<()>> {
    Ok(tokio::spawn(async move {
        run_event_loop(state).await;
    }))
}

async fn run_event_loop(state: SharedState) {
    let mut attempt: u32 = 0;
    loop {
        match connect_and_pump(state.clone()).await {
            Ok(_) => {
                // Pump returned normally — niri closed the stream. Treat as disconnect.
                warn!("niri event stream ended; will reconnect");
            }
            Err(e) => {
                error!("niri event-stream error: {e:#}");
            }
        }
        // Exponential backoff capped at 5s, with mild jitter.
        let base = 1u64 << attempt.min(3); // 1, 2, 4, 8s capped below
        let secs = base.min(5);
        debug!("reconnecting to niri in {secs}s");
        sleep(Duration::from_secs(secs)).await;
        attempt = attempt.saturating_add(1);
    }
}

/// Open one event-stream socket, push events through a channel, drain into
/// `state`. Returns when the channel closes (= blocking thread ended).
async fn connect_and_pump(state: SharedState) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<NiriEvent>(256);

    // Blocking thread: opens Socket, subscribes, reads events forever,
    // forwards each to the tokio side via `tx.blocking_send`.
    let _reader = spawn_blocking(move || -> Result<()> {
        let mut sock = Socket::connect().context("connecting to NIRI_SOCKET")?;
        match sock.send(Request::EventStream)? {
            Ok(Response::Handled) => {}
            Ok(other) => return Err(anyhow!("unexpected event-stream reply: {other:?}")),
            Err(e) => return Err(anyhow!("niri rejected EventStream subscription: {e}")),
        }
        let mut read_event = sock.read_events();
        loop {
            match read_event() {
                Ok(ev) => {
                    if tx.blocking_send(ev).is_err() {
                        // Receiver dropped — caller went away.
                        return Ok(());
                    }
                }
                Err(e) => return Err(anyhow!("read_events: {e}")),
            }
        }
    });

    info!("subscribed to niri event stream");
    // Notify subscribers that bridge state is being (re)initialized.
    emit_state_reset(&state);

    while let Some(ev) = rx.recv().await {
        if let Err(e) = handle_niri_event(&state, ev).await {
            warn!("error handling niri event: {e:#}");
        }
    }
    Ok(())
}

/// One-shot blocking query helper. Used by op handlers that need a
/// fresh response from niri rather than the cached state.
pub async fn query(req: Request) -> Result<Response> {
    spawn_blocking(move || -> Result<Response> {
        let mut sock = Socket::connect().context("connecting to NIRI_SOCKET")?;
        let reply: Reply = sock.send(req)?;
        reply.map_err(|e| anyhow!("niri returned error: {e}"))
    })
    .await
    .context("query task panicked")?
}

async fn handle_niri_event(state: &SharedState, ev: NiriEvent) -> Result<()> {
    match ev {
        NiriEvent::WindowsChanged { windows } => {
            state
                .with_mut(|s| {
                    s.windows = windows
                        .into_iter()
                        .map(|w| (w.id, normalize_window(&w)))
                        .collect();
                })
                .await;
            emit(state, topics::WINDOWS, serde_json::json!({"changed": "all"}));
        }
        NiriEvent::WindowOpenedOrChanged { window } => {
            let id = window.id;
            let normalized = normalize_window(&window);
            state
                .with_mut(|s| {
                    s.windows.insert(id, normalized.clone());
                    if window.is_focused {
                        s.focused_window = Some(id);
                    }
                })
                .await;
            emit(
                state,
                topics::WINDOWS,
                serde_json::json!({"opened_or_changed": normalized}),
            );
        }
        NiriEvent::WindowClosed { id } => {
            state
                .with_mut(|s| {
                    s.windows.remove(&id);
                    if s.focused_window == Some(id) {
                        s.focused_window = None;
                    }
                })
                .await;
            emit(state, topics::WINDOWS, serde_json::json!({"closed": id}));
        }
        NiriEvent::WindowFocusChanged { id } => {
            state.with_mut(|s| s.focused_window = id).await;
            emit(state, topics::FOCUS, serde_json::json!({"focused_window_id": id}));
        }
        NiriEvent::WorkspacesChanged { workspaces } => {
            state
                .with_mut(|s| {
                    s.workspaces = workspaces
                        .into_iter()
                        .map(|w| (w.id, normalize_workspace(&w)))
                        .collect();
                    s.focused_workspace = s
                        .workspaces
                        .values()
                        .find(|w| w.is_focused)
                        .map(|w| w.id);
                })
                .await;
            emit(state, topics::WORKSPACES, serde_json::json!({"changed": "all"}));
        }
        NiriEvent::WorkspaceActivated { id, focused } => {
            state
                .with_mut(|s| {
                    if let Some(w) = s.workspaces.get_mut(&id) {
                        w.is_focused = focused;
                    }
                    if focused {
                        s.focused_workspace = Some(id);
                    }
                })
                .await;
            emit(
                state,
                topics::WORKSPACES,
                serde_json::json!({"activated": id, "focused": focused}),
            );
        }
        NiriEvent::WorkspaceActiveWindowChanged {
            workspace_id,
            active_window_id,
        } => {
            state
                .with_mut(|s| {
                    if let Some(w) = s.workspaces.get_mut(&workspace_id) {
                        w.active_window_id = active_window_id;
                    }
                })
                .await;
        }
        // Events we don't care about for v1 — just ignore.
        _ => {}
    }
    Ok(())
}

fn normalize_window(w: &niri_ipc::Window) -> proto::Window {
    proto::Window {
        id: w.id,
        app_id: w.app_id.clone(),
        title: w.title.clone(),
        pid: w.pid,
        workspace_id: w.workspace_id,
        is_focused: w.is_focused,
        is_floating: w.is_floating,
    }
}

fn normalize_workspace(w: &niri_ipc::Workspace) -> proto::Workspace {
    proto::Workspace {
        id: w.id,
        idx: w.idx,
        name: w.name.clone(),
        output: w.output.clone(),
        is_focused: w.is_focused,
        active_window_id: w.active_window_id,
    }
}

fn emit(state: &SharedState, topic: &str, data: serde_json::Value) {
    let ev = proto::Event {
        event: topic.to_string(),
        ts: now_ms(),
        data,
    };
    // Send is best-effort; if no one's subscribed, the broadcast just drops.
    let _ = state.events.send(ev);
}

fn emit_state_reset(state: &SharedState) {
    let ev = proto::Event {
        event: topics::STATE.to_string(),
        ts: now_ms(),
        data: serde_json::json!({"reset": true}),
    };
    let _ = state.events.send(ev);
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
