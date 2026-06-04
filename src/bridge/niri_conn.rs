//! Owns the connection(s) to niri's IPC socket.
//!
//! niri-ipc exposes a *blocking* `Socket`. We talk to niri two ways:
//!  - The event stream lives in a blocking thread that connects to
//!    `$NIRI_SOCKET` directly, sends `Request::EventStream`, then reads
//!    JSON-lines forever and pumps each into tokio via an mpsc channel.
//!    We bypass `Socket::read_events()` because its strict deserialize
//!    breaks the stream on any future niri event variant we haven't
//!    seen — see the comment on the spawn_blocking thread below.
//!  - `query()` opens a fresh niri-ipc `Socket` per call (cheap; niri's
//!    UDS accept is fast and we do this only on action ops).
//!
//! Reconnect: if the event-stream socket dies (niri restart, crash), we
//! sleep with backoff and reconnect. On reconnect we emit a synthetic
//! `state` event (`{"reset": true}`) so subscribers know to re-fetch.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use niri_ipc::socket::SOCKET_PATH_ENV;
use niri_ipc::{Event as NiriEvent, Reply, Request, Response, socket::Socket};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, spawn_blocking};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use super::activation::ActivationBroker;
use super::proto::{self, topics};
use super::state::SharedState;

/// Spawn the long-running task that owns the niri event stream + drives
/// `state`. Returns a `JoinHandle` that completes only on a fatal error
/// (we never give up — the loop reconnects forever).
///
/// `broker` is `Some` in production and `None` in unit tests that don't need
/// XDG-activation correlation; when present, this loop drains the broker's
/// pid → token map on each incoming `WindowOpenedOrChanged` and stamps the
/// matching token onto the emitted `windows` event.
pub async fn spawn_niri_loop(
    state: SharedState,
    broker: Option<ActivationBroker>,
) -> Result<JoinHandle<()>> {
    Ok(tokio::spawn(async move {
        run_event_loop(state, broker).await;
    }))
}

async fn run_event_loop(state: SharedState, broker: Option<ActivationBroker>) {
    let mut attempt: u32 = 0;
    loop {
        match connect_and_pump(state.clone(), broker.clone()).await {
            Ok(_) => {
                // Pump returned cleanly — the EventStream subscription
                // succeeded and niri eventually closed the connection
                // (compositor restart, etc.). Reset backoff so the next
                // reconnect doesn't carry the previous failure history.
                warn!("niri event stream ended; will reconnect");
                attempt = 0;
            }
            Err(e) => {
                error!("niri event-stream error: {e:#}");
                attempt = attempt.saturating_add(1);
            }
        }
        // Exponential backoff capped at 5s.
        let base = 1u64 << attempt.min(3); // 1, 2, 4, 8s capped below
        let secs = base.min(5);
        debug!("reconnecting to niri in {secs}s");
        sleep(Duration::from_secs(secs)).await;
    }
}

/// Open one event-stream socket, push events through a channel, drain into
/// `state`. Returns when the channel closes (= blocking thread ended).
async fn connect_and_pump(state: SharedState, broker: Option<ActivationBroker>) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<NiriEvent>(256);

    // Blocking thread: opens a raw UnixStream to NIRI_SOCKET, sends the
    // EventStream subscription request by hand, then reads JSON-lines
    // forever and forwards each event to the tokio side.
    //
    // Why bypass niri-ipc's `Socket::read_events()`? It deserializes each
    // incoming line into the strict `niri_ipc::Event` enum and bails on
    // any unknown variant. niri ships new variants in minor versions
    // (e.g. 26.04 added `CastsChanged`) — strict parsing means iris
    // breaks the moment niri ships an event we haven't seen, even though
    // we wouldn't act on it. Forward-compat with future niri releases is
    // worth ~25 LOC: log-and-skip on deserialize failure, keep reading.
    //
    // Tradeoffs documented at `~/.claude/plans/once-revisit-the-plan-noble-pond.md`
    // and the user's iris memory; this is the agreed approach.
    let reader = spawn_blocking(move || -> Result<()> {
        let socket_path = std::env::var_os(SOCKET_PATH_ENV).ok_or_else(|| {
            anyhow!("{SOCKET_PATH_ENV} is not set, are you running this within niri?")
        })?;
        let stream = UnixStream::connect(&socket_path)
            .with_context(|| format!("connecting to {}", socket_path.to_string_lossy()))?;

        // Hand-serialize the EventStream request through niri-ipc's
        // `Request` type so the JSON shape stays in lockstep with the
        // crate even when we own the framing. Same reasoning for the
        // ack `Reply` parse below.
        let mut writer = stream;
        let req = serde_json::to_string(&Request::EventStream)?;
        writer.write_all(req.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;

        let mut reader_buf = BufReader::new(writer);
        let mut buf = String::new();

        // First line is the ack. Anything other than Reply::Ok(Handled)
        // is fatal — caller will reconnect. Surface immediate EOF
        // (niri closed before responding) as a clear diagnostic rather
        // than a generic serde "EOF while parsing" error.
        if reader_buf.read_line(&mut buf)? == 0 {
            return Err(anyhow!("niri closed connection before EventStream ack"));
        }
        let reply: Reply = serde_json::from_str(&buf)
            .with_context(|| format!("parsing EventStream ack: {}", buf.trim()))?;
        match reply {
            Ok(Response::Handled) => {}
            Ok(other) => return Err(anyhow!("unexpected event-stream reply: {other:?}")),
            Err(e) => return Err(anyhow!("niri rejected EventStream subscription: {e}")),
        }

        let mut events_read: u64 = 0;
        let mut events_skipped: u64 = 0;
        loop {
            buf.clear();
            match reader_buf.read_line(&mut buf) {
                Ok(0) => {
                    // EOF — niri closed the stream.
                    debug!(events_read, events_skipped, "niri event stream EOF");
                    return Ok(());
                }
                Ok(_) => {}
                Err(e) => {
                    error!(events_read, "niri read_line failed: {e}; reader exiting");
                    return Err(anyhow!("read_line: {e}"));
                }
            }
            // Skip whitespace-only lines defensively. niri doesn't send
            // keepalives today, but a future protocol change could.
            if buf.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<NiriEvent>(&buf) {
                Ok(ev) => {
                    events_read += 1;
                    debug!(events_read, ?ev, "niri event read");
                    if tx.blocking_send(ev).is_err() {
                        debug!(events_read, "niri event channel closed; reader exiting cleanly");
                        return Ok(());
                    }
                }
                Err(e) => {
                    // Forward-compat: niri shipped a variant or field
                    // niri-ipc doesn't recognize. Log + skip; the events
                    // we DO understand still flow through.
                    events_skipped += 1;
                    warn!(
                        events_skipped,
                        error = %e,
                        raw = %buf.trim(),
                        "skipping unrecognized niri event"
                    );
                }
            }
        }
    });

    info!("subscribed to niri event stream");
    // Notify subscribers that bridge state is being (re)initialized.
    emit_state_reset(&state);

    while let Some(ev) = rx.recv().await {
        if let Err(e) = handle_niri_event(&state, broker.as_ref(), ev).await {
            warn!("error handling niri event: {e:#}");
        }
    }

    // The reader's `tx` was dropped — surface what happened to it.
    // Reachable only on a real failure mode now: the niri socket was
    // closed by the compositor, an IO error on the read side, or the
    // EventStream ack itself didn't parse. Unknown event variants are
    // warn-and-skipped inside the reader and don't end the loop.
    match reader.await {
        Ok(Ok(())) => debug!("niri reader thread exited cleanly"),
        Ok(Err(e)) => warn!("niri reader thread error: {e:#}"),
        Err(join_err) => warn!("niri reader thread join error: {join_err}"),
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

async fn handle_niri_event(
    state: &SharedState,
    broker: Option<&ActivationBroker>,
    ev: NiriEvent,
) -> Result<()> {
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
            // Match the spawn that produced this window via pid (niri-ipc
            // 25.11.0 doesn't surface activation tokens in events). The
            // broker drops the entry on first lookup, so a re-emit of
            // WindowOpenedOrChanged for the same window won't get a stale
            // token.
            let activation_token = broker
                .and_then(|b| window.pid.and_then(|pid| b.take_token_for_pid(pid)));
            debug!(
                window_id = id,
                window_pid = ?window.pid,
                app_id = ?window.app_id,
                title = ?window.title,
                stamped_token = ?activation_token,
                "WindowOpenedOrChanged"
            );
            state
                .with_mut(|s| {
                    s.windows.insert(id, normalized.clone());
                    if window.is_focused {
                        s.focused_window = Some(id);
                    }
                })
                .await;
            let mut payload = serde_json::json!({"opened_or_changed": normalized});
            if let Some(tok) = activation_token {
                payload["activation_token"] = serde_json::Value::String(tok);
            }
            emit(state, topics::WINDOWS, payload);
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
    // niri reports `pos_in_scrolling_layout` as 1-based (column, tile);
    // store 0-based so it round-trips through `MoveColumnToIndex` (which
    // is also 1-based, so we add 1 back on the way out). `None` covers
    // floating windows and any future "not in scrolling layout" state.
    let (column_index, position_in_column) = match w.layout.pos_in_scrolling_layout {
        Some((col, tile)) => (
            Some(col.saturating_sub(1) as u32),
            Some(tile.saturating_sub(1) as u32),
        ),
        None => (None, None),
    };
    let (tw, th) = w.layout.tile_size;
    let floating_position = if w.is_floating {
        w.layout
            .tile_pos_in_workspace_view
            .map(|(x, y)| proto::FloatingPosition { x, y })
    } else {
        None
    };
    proto::Window {
        id: w.id,
        app_id: w.app_id.clone(),
        title: w.title.clone(),
        pid: w.pid,
        workspace_id: w.workspace_id,
        is_focused: w.is_focused,
        is_floating: w.is_floating,
        column_index,
        position_in_column,
        width: tw.round() as i32,
        height: th.round() as i32,
        floating_position,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_window_extracts_layout_fields() {
        // Tiled window in column 3 (1-based niri index → 2 0-based) at
        // tile index 1 (1-based → 0 0-based), size 950×720.
        let mut w = niri_window(99, Some(1));
        w.layout.pos_in_scrolling_layout = Some((3, 1));
        w.layout.tile_size = (950.4, 720.6);
        let p = normalize_window(&w);
        assert_eq!(p.column_index, Some(2));
        assert_eq!(p.position_in_column, Some(0));
        assert_eq!(p.width, 950);
        assert_eq!(p.height, 721);
        assert_eq!(p.floating_position, None);
    }

    #[test]
    fn normalize_window_floating_captures_position() {
        let mut w = niri_window(7, Some(2));
        w.is_floating = true;
        w.layout.pos_in_scrolling_layout = None;
        w.layout.tile_size = (400.0, 300.0);
        w.layout.tile_pos_in_workspace_view = Some((150.5, 200.25));
        let p = normalize_window(&w);
        assert_eq!(p.column_index, None);
        assert_eq!(p.position_in_column, None);
        assert_eq!(p.width, 400);
        assert_eq!(p.height, 300);
        assert_eq!(
            p.floating_position,
            Some(proto::FloatingPosition { x: 150.5, y: 200.25 })
        );
    }

    /// Minimal `niri_ipc::Window` for tests. Fields not under test get
    /// defaults that satisfy the type signatures.
    fn niri_window(id: u64, pid: Option<i32>) -> niri_ipc::Window {
        niri_ipc::Window {
            id,
            title: Some("t".into()),
            app_id: Some("foot".into()),
            pid,
            workspace_id: Some(1),
            is_focused: false,
            is_floating: false,
            is_urgent: false,
            layout: niri_ipc::WindowLayout {
                pos_in_scrolling_layout: None,
                tile_size: (0.0, 0.0),
                window_size: (0, 0),
                tile_pos_in_workspace_view: None,
                window_offset_in_tile: (0.0, 0.0),
            },
            focus_timestamp: None,
        }
    }

    #[tokio::test]
    async fn window_opened_stamps_token_when_pid_matches() {
        let state = SharedState::new();
        let mut rx = state.subscribe();
        let broker = ActivationBroker::test_handle();
        broker.register_spawn(4242, "tok-spawn".into());

        handle_niri_event(
            &state,
            Some(&broker),
            NiriEvent::WindowOpenedOrChanged { window: niri_window(1, Some(4242)) },
        )
        .await
        .unwrap();

        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.event, "windows");
        assert_eq!(ev.data["activation_token"], "tok-spawn");
        assert_eq!(ev.data["opened_or_changed"]["id"], 1);
        // Broker entry consumed.
        assert!(broker.take_token_for_pid(4242).is_none());
    }

    #[tokio::test]
    async fn window_opened_without_match_omits_token() {
        let state = SharedState::new();
        let mut rx = state.subscribe();
        let broker = ActivationBroker::test_handle();

        handle_niri_event(
            &state,
            Some(&broker),
            NiriEvent::WindowOpenedOrChanged { window: niri_window(2, Some(99)) },
        )
        .await
        .unwrap();

        let ev = rx.recv().await.unwrap();
        // Field absent — null/missing both fine, but it must NOT be a string.
        assert!(
            ev.data.get("activation_token").is_none(),
            "unexpected activation_token: {ev:?}"
        );
    }

    #[tokio::test]
    async fn window_opened_without_pid_omits_token() {
        // Portal apps can have pid=None; we mustn't crash and mustn't stamp.
        let state = SharedState::new();
        let mut rx = state.subscribe();
        let broker = ActivationBroker::test_handle();
        broker.register_spawn(4242, "tok-orphan".into());

        handle_niri_event(
            &state,
            Some(&broker),
            NiriEvent::WindowOpenedOrChanged { window: niri_window(3, None) },
        )
        .await
        .unwrap();

        let ev = rx.recv().await.unwrap();
        assert!(ev.data.get("activation_token").is_none());
        // The 4242 registration is untouched — a future event with that pid
        // would still match.
        assert_eq!(broker.take_token_for_pid(4242).as_deref(), Some("tok-orphan"));
    }

    #[tokio::test]
    async fn window_opened_without_broker_omits_token() {
        let state = SharedState::new();
        let mut rx = state.subscribe();

        handle_niri_event(
            &state,
            None,
            NiriEvent::WindowOpenedOrChanged { window: niri_window(4, Some(4242)) },
        )
        .await
        .unwrap();

        let ev = rx.recv().await.unwrap();
        assert!(ev.data.get("activation_token").is_none());
    }
}
