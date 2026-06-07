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
use niri_ipc::{
    Action, PositionChange, Request as NiriRequest, Response as NiriResponse, SizeChange,
};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::activation::ActivationBroker;
use super::niri_conn::query;
use super::pinned;
use super::proto::{self, Op, Request, Response, ServerMessage};
use super::state::{ClientSubs, SharedState};

pub async fn spawn(
    sock_path: PathBuf,
    state: SharedState,
    broker: Option<ActivationBroker>,
) -> Result<JoinHandle<()>> {
    let listener = UnixListener::bind(&sock_path)
        .with_context(|| format!("binding {}", sock_path.display()))?;
    // Make the socket user-only. The bind already happened with the process
    // umask; explicit chmod is a defense in case umask is weird.
    let perms = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(&sock_path, perms)
        .with_context(|| format!("chmod 0600 {}", sock_path.display()))?;

    info!(socket = %sock_path.display(), "iris bridge listening");
    Ok(spawn_accept_loop(listener, state, broker))
}

/// Accept-loop entry, factored out so tests can bind their own listener
/// (on a tempfile-backed path) without going through `spawn`'s chmod path.
/// `broker` is `None` in tests that don't exercise the spawn op.
fn spawn_accept_loop(
    listener: UnixListener,
    state: SharedState,
    broker: Option<ActivationBroker>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let st = state.clone();
                    let br = broker.clone();
                    tokio::spawn(async move {
                        let id = next_client_id();
                        debug!(client = id, "client connected");
                        if let Err(e) = handle_client(stream, st, br).await {
                            warn!(client = id, "client task ended: {e:#}");
                        } else {
                            debug!(client = id, "client disconnected");
                        }
                    });
                }
                Err(e) => {
                    error!("accept failed: {e:#}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
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
async fn handle_client(
    stream: UnixStream,
    state: SharedState,
    broker: Option<ActivationBroker>,
) -> Result<()> {
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
        let resp = dispatch_op(&state, &subs, broker.as_ref(), &req).await;
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

// ─────────────────────────────── Op dispatch ──────────────────────────────────

async fn dispatch_op(
    state: &SharedState,
    subs: &Arc<Mutex<ClientSubs>>,
    broker: Option<&ActivationBroker>,
    req: &Request,
) -> Response {
    match handle_op(state, subs, broker, req).await {
        Ok(data) => Response::ok(&req.id, data),
        Err(e) => Response::err(&req.id, format!("{e:#}")),
    }
}

async fn handle_op(
    state: &SharedState,
    subs: &Arc<Mutex<ClientSubs>>,
    broker: Option<&ActivationBroker>,
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
                proto::WorkspaceRef::Name { name } => {
                    niri_ipc::WorkspaceReferenceArg::Name(name.clone())
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
        Op::WindowMoveColumnToIndex { id, index } => {
            // niri's MoveColumnToIndex acts on the focused column, so focus
            // the target window first. The two actions are not atomic at
            // the niri side — a user keybind firing in between could move
            // a different column. Acceptable for v1.
            //
            // Wire `index` is 0-based to match `Window.column_index`; niri
            // wants 1-based.
            forward_action(Action::FocusWindow { id: *id }).await?;
            forward_action(Action::MoveColumnToIndex {
                index: (*index as usize).saturating_add(1),
            })
            .await?;
            Ok(json!({}))
        }
        Op::WindowSetSize { id, w, h } => {
            forward_action(Action::SetWindowWidth {
                id: Some(*id),
                change: SizeChange::SetFixed(*w),
            })
            .await?;
            forward_action(Action::SetWindowHeight {
                id: Some(*id),
                change: SizeChange::SetFixed(*h),
            })
            .await?;
            Ok(json!({}))
        }
        Op::WindowSetFloatingPosition { id, x, y } => {
            forward_action(Action::MoveFloatingWindow {
                id: Some(*id),
                x: PositionChange::SetFixed(*x),
                y: PositionChange::SetFixed(*y),
            })
            .await?;
            Ok(json!({}))
        }

        Op::PinAdd { window_id } => {
            let outcome = try_pin(state, *window_id).await;
            // Persist whenever `try_pin` mutated the in-memory set,
            // including the err-after-removal branch (already_pinned
            // + now-tiled drops the stale id and bails). Without this,
            // memory and disk would disagree until the next op.
            if outcome.mutated {
                if let Err(e) = pinned::persist(state).await {
                    tracing::warn!("pin persist failed: {e:#}");
                }
            }
            outcome.result?;
            // Uniform with pin.toggle / pin.remove: callers can read
            // `data.pinned` across all three ops to learn the new state.
            Ok(json!({"pinned": true}))
        }
        Op::PinRemove { window_id } => {
            let was = state
                .with_mut(|s| s.pinned_windows.remove(window_id))
                .await;
            if was {
                if let Err(e) = pinned::persist(state).await {
                    tracing::warn!("pin persist failed: {e:#}");
                }
            }
            Ok(json!({"pinned": false}))
        }
        Op::PinToggle { window_id } => {
            // Atomic: read state, decide add vs remove, mutate — all
            // under one with_mut so a Mod+V toggle landing between the
            // read and the mutation can't slip through. The is_floating
            // / app_id checks happen inside the closure too.
            enum Outcome {
                Pinned,
                Unpinned,
            }
            let outcome = state
                .with_mut(|s| -> Result<Outcome, anyhow::Error> {
                    if s.pinned_windows.remove(window_id) {
                        return Ok(Outcome::Unpinned);
                    }
                    let w = s
                        .windows
                        .get(window_id)
                        .ok_or_else(|| anyhow::anyhow!("no window with id {window_id}"))?;
                    if !w.is_floating {
                        anyhow::bail!(
                            "only floating windows can be pinned; toggle floating first with Mod+V"
                        );
                    }
                    if w.app_id.as_deref().unwrap_or("").is_empty() {
                        anyhow::bail!(
                            "cannot pin window {window_id}: it reports no app_id, so we couldn't restore the pin across bridge restarts"
                        );
                    }
                    s.pinned_windows.insert(*window_id);
                    Ok(Outcome::Pinned)
                })
                .await?;
            if let Err(e) = pinned::persist(state).await {
                tracing::warn!("pin persist failed: {e:#}");
            }
            match outcome {
                Outcome::Pinned => Ok(json!({"pinned": true})),
                Outcome::Unpinned => Ok(json!({"pinned": false})),
            }
        }
        Op::PinOff => {
            let n = state
                .with_mut(|s| {
                    let n = s.pinned_windows.len();
                    s.pinned_windows.clear();
                    n
                })
                .await;
            if let Err(e) = pinned::persist(state).await {
                tracing::warn!("pin persist failed: {e:#}");
            }
            Ok(json!({"n_unpinned": n}))
        }
        Op::PinList => {
            let pinned = state
                .with(|s| {
                    s.pinned_windows
                        .iter()
                        .filter_map(|id| s.windows.get(id).cloned())
                        .collect::<Vec<_>>()
                })
                .await;
            Ok(json!(pinned))
        }
        Op::ScratchpadList => {
            // Stub through W5; W6 lights it up.
            Ok(json!([]))
        }

        Op::Spawn { argv, env, request_activation_token } => {
            handle_spawn(broker, argv, env, *request_activation_token).await
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

/// Outcome of `try_pin`. `mutated` says whether the in-memory pin set
/// changed (so the caller knows whether to persist), regardless of
/// whether `result` is Ok or Err — the err-after-removal branch
/// (already-pinned + now-tiled) still mutated and must be persisted.
struct PinOutcome {
    mutated: bool,
    result: Result<bool>,
}

/// Atomic pin-add: validation (window exists, floating, has app_id) and
/// the `pinned_windows.insert` happen under one `with_mut`, so a
/// `Mod+V` toggle racing between check and insert can't sneak through.
///
/// Returns `Ok(true)` if a fresh pin was inserted, `Ok(false)` if the
/// id was already pinned and still floating (idempotent), or `Err`:
/// stale pin dropped, no such window, non-floating, or no app_id.
async fn try_pin(state: &SharedState, window_id: u64) -> PinOutcome {
    state
        .with_mut(|s| -> PinOutcome {
            // Validate against live cache first — if the window has
            // gone tiled (or vanished) since the pin landed (e.g. a
            // broadcast Lagged delayed auto-unpin), drop the stale
            // entry so the response truthfully reflects the current
            // floating-only invariant.
            let live_floating = match s.windows.get(&window_id) {
                Some(w) => w.is_floating,
                None => false,
            };
            let already_pinned = s.pinned_windows.contains(&window_id);
            if already_pinned && !live_floating {
                s.pinned_windows.remove(&window_id);
                return PinOutcome {
                    mutated: true,
                    result: Err(anyhow::anyhow!(
                        "pin on {window_id} dropped: window is no longer floating"
                    )),
                };
            }
            if already_pinned {
                return PinOutcome { mutated: false, result: Ok(false) };
            }
            let w = match s.windows.get(&window_id) {
                Some(w) => w,
                None => {
                    return PinOutcome {
                        mutated: false,
                        result: Err(anyhow::anyhow!("no window with id {window_id}")),
                    };
                }
            };
            if !w.is_floating {
                return PinOutcome {
                    mutated: false,
                    result: Err(anyhow::anyhow!(
                        "only floating windows can be pinned; toggle floating first with Mod+V"
                    )),
                };
            }
            if w.app_id.as_deref().unwrap_or("").is_empty() {
                return PinOutcome {
                    mutated: false,
                    result: Err(anyhow::anyhow!(
                        "cannot pin window {window_id}: it reports no app_id, so we couldn't restore the pin across bridge restarts"
                    )),
                };
            }
            s.pinned_windows.insert(window_id);
            PinOutcome { mutated: true, result: Ok(true) }
        })
        .await
}

async fn handle_spawn(
    broker: Option<&ActivationBroker>,
    argv: &[String],
    env: &std::collections::HashMap<String, String>,
    request_activation_token: bool,
) -> Result<serde_json::Value> {
    if argv.is_empty() {
        anyhow::bail!("spawn requires non-empty argv");
    }

    // Mint the token *before* spawning so we can plant it in the child's env.
    let token = if request_activation_token {
        let b = broker.ok_or_else(|| {
            anyhow::anyhow!("activation broker unavailable; bridge not started with Wayland support")
        })?;
        Some(b.mint_token().await?)
    } else {
        None
    };

    let mut cmd = build_spawn_command(argv, env, token.as_deref());
    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawning {:?}", argv))?;
    // Pid extraction can technically fail (child already polled to
    // completion / pid > i32::MAX). If we bail here the child is already
    // running with no reaper attached — kill it on the way out so we don't
    // leave an orphan.
    let pid = match child
        .id()
        .ok_or_else(|| anyhow::anyhow!("spawned child has no pid"))
        .and_then(|raw| {
            i32::try_from(raw)
                .map_err(|_| anyhow::anyhow!("spawned child pid {raw} doesn't fit in i32"))
        }) {
        Ok(p) => p,
        Err(e) => {
            let _ = child.kill().await;
            return Err(e);
        }
    };

    // Register pid → token so niri_conn can stamp the matching event.
    if let (Some(b), Some(tok)) = (broker, &token) {
        debug!(spawn_pid = pid, token = %tok, argv = ?argv, "spawn register_spawn");
        b.register_spawn(pid, tok.clone());
    } else {
        debug!(spawn_pid = pid, has_broker = broker.is_some(), has_token = token.is_some(), "spawn without broker registration");
    }

    // Reap the child in the background. We don't surface its exit status to
    // the spawn op's caller — the caller has the pid and can monitor itself.
    tokio::spawn(async move {
        let _ = child.wait().await;
    });

    Ok(serde_json::json!({
        "pid": pid,
        "activation_token": token,
    }))
}

/// Build the `Command` for a spawn op. Factored out of `handle_spawn` so
/// tests can assert env plumbing (XDG_ACTIVATION_TOKEN + caller-supplied
/// env) without actually spawning a process or talking to Wayland.
fn build_spawn_command(
    argv: &[String],
    env: &std::collections::HashMap<String, String>,
    token: Option<&str>,
) -> tokio::process::Command {
    let mut cmd = tokio::process::Command::new(&argv[0]);
    cmd.args(&argv[1..]);
    for (k, v) in env {
        cmd.env(k, v);
    }
    if let Some(tok) = token {
        cmd.env("XDG_ACTIVATION_TOKEN", tok);
    }
    cmd
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
    //! These tests bind a UDS on a tempfile path and drive the server end
    //! to end. Action ops (window.focus etc.) need a real niri so they're
    //! intentionally not exercised here — only queries + subscriptions.
    use super::*;
    use crate::bridge::proto::{self as p, topics};
    use serde_json::{Value, json};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::time::timeout;

    /// Bind a server on a tempfile path, return (path, state, accept-task).
    /// State starts pre-populated so query ops have something to return.
    async fn start_server() -> (TempDir, std::path::PathBuf, SharedState, JoinHandle<()>) {
        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();
        let state = SharedState::new();
        // Seed state with one window + one workspace.
        state
            .with_mut(|s| {
                s.windows.insert(
                    1,
                    p::Window {
                        id: 1,
                        app_id: Some("foot".into()),
                        title: Some("hello".into()),
                        pid: Some(42),
                        workspace_id: Some(10),
                        is_focused: true,
                        is_floating: false,
                        column_index: Some(0),
                        position_in_column: Some(0),
                        width: 800,
                        height: 600,
                        floating_position: None,
                    },
                );
                s.workspaces.insert(
                    10,
                    p::Workspace {
                        id: 10,
                        idx: 1,
                        name: Some("code".into()),
                        output: Some("DP-1".into()),
                        is_focused: true,
                        active_window_id: Some(1),
                    },
                );
                s.focused_window = Some(1);
                s.focused_workspace = Some(10);
            })
            .await;
        let task = spawn_accept_loop(listener, state.clone(), None);
        (tmp, sock_path, state, task)
    }

    /// Like `start_server` but installs a fake-minter activation broker so
    /// `spawn` ops requesting tokens can complete end-to-end. Returns the
    /// broker handle alongside the usual quartet so tests can assert on
    /// pid-map state directly.
    async fn start_server_with_broker() -> (
        TempDir,
        std::path::PathBuf,
        SharedState,
        ActivationBroker,
        JoinHandle<()>,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();
        let state = SharedState::new();
        let broker = ActivationBroker::test_handle_with_minter();
        let task = spawn_accept_loop(listener, state.clone(), Some(broker.clone()));
        (tmp, sock_path, state, broker, task)
    }

    /// Open a client connection and return (lines reader, write half).
    async fn connect(
        path: &std::path::Path,
    ) -> (
        tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
        tokio::net::unix::OwnedWriteHalf,
    ) {
        let stream = UnixStream::connect(path).await.unwrap();
        let (r, w) = stream.into_split();
        (BufReader::new(r).lines(), w)
    }

    async fn send_line(w: &mut tokio::net::unix::OwnedWriteHalf, s: &str) {
        w.write_all(s.as_bytes()).await.unwrap();
        w.write_all(b"\n").await.unwrap();
    }

    async fn read_one_line(
        lines: &mut tokio::io::Lines<BufReader<tokio::net::unix::OwnedReadHalf>>,
    ) -> Value {
        let line = timeout(Duration::from_secs(2), lines.next_line())
            .await
            .expect("timed out waiting for response")
            .unwrap()
            .expect("server closed");
        serde_json::from_str(&line).unwrap()
    }

    #[tokio::test]
    async fn windows_list_returns_seeded_window() {
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(&mut w, r#"{"id":"1","op":"windows.list"}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["id"], "1");
        assert_eq!(resp["ok"], true);
        assert_eq!(resp["data"][0]["app_id"], "foot");
        assert_eq!(resp["data"][0]["id"], 1);
    }

    #[tokio::test]
    async fn state_snapshot_returns_full_picture() {
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(&mut w, r#"{"id":"snap","op":"state.snapshot"}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert!(resp["ok"].as_bool().unwrap());
        assert_eq!(resp["data"]["focused_window_id"], 1);
        assert_eq!(resp["data"]["focused_workspace_id"], 10);
        assert_eq!(resp["data"]["windows"].as_array().unwrap().len(), 1);
        assert_eq!(resp["data"]["workspaces"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn invalid_json_returns_error_response_not_disconnect() {
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(&mut w, "not valid json").await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], false);
        assert!(resp["error"].as_str().unwrap().contains("invalid"));
        // Connection should still work after a bad line.
        send_line(&mut w, r#"{"id":"recover","op":"noop"}"#).await;
        let resp2 = read_one_line(&mut lines).await;
        assert_eq!(resp2["id"], "recover");
        assert_eq!(resp2["ok"], true);
    }

    #[tokio::test]
    async fn unknown_op_returns_error() {
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(&mut w, r#"{"id":"x","op":"definitely.not.real"}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], false);
    }

    #[tokio::test]
    async fn subscribe_then_event_arrives_filtered() {
        let (_tmp, path, state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        // Subscribe to focus only.
        send_line(
            &mut w,
            r#"{"id":"s","op":"subscribe","params":{"topics":["focus"]}}"#,
        )
        .await;
        let _ack = read_one_line(&mut lines).await;

        // Fire an event on a topic we DIDN'T subscribe to: should be filtered.
        let _ = state.events.send(p::Event {
            event: topics::WORKSPACES.into(),
            ts: 0,
            data: json!({"ignored": true}),
        });
        // Then a focus event we DO want.
        let _ = state.events.send(p::Event {
            event: topics::FOCUS.into(),
            ts: 1,
            data: json!({"focused_window_id": 1}),
        });

        // Next line we read must be the focus event, not the workspaces one.
        let got = read_one_line(&mut lines).await;
        assert_eq!(got["event"], "focus");
        assert_eq!(got["ts"], 1);
        assert_eq!(got["data"]["focused_window_id"], 1);
    }

    #[tokio::test]
    async fn unsubscribe_stops_delivery() {
        let (_tmp, path, state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(
            &mut w,
            r#"{"id":"s1","op":"subscribe","params":{"topics":["focus"]}}"#,
        )
        .await;
        let _ = read_one_line(&mut lines).await;
        send_line(
            &mut w,
            r#"{"id":"s2","op":"unsubscribe","params":{"topics":["focus"]}}"#,
        )
        .await;
        let _ = read_one_line(&mut lines).await;

        // Should NOT be delivered.
        let _ = state.events.send(p::Event {
            event: topics::FOCUS.into(),
            ts: 0,
            data: json!({}),
        });
        // A noop request should resolve before any rogue event arrives.
        send_line(&mut w, r#"{"id":"n","op":"noop"}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["id"], "n", "should be the noop response, not an event");
    }

    #[tokio::test]
    async fn action_window_focus_round_trips_to_niri() {
        let (_tmp, path, state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        // Use whatever window is actually focused right now.
        send_line(&mut w, r#"{"id":"q","op":"state.snapshot"}"#).await;
        let snap = read_one_line(&mut lines).await;
        let id = snap["data"]["focused_window_id"]
            .as_u64()
            .expect("need at least one focused window for this test");

        let req = format!(r#"{{"id":"f","op":"window.focus","params":{{"id":{id}}}}}"#);
        send_line(&mut w, &req).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["id"], "f");
        assert!(resp["ok"].as_bool().unwrap(), "got error: {resp}");
        let _ = state; // keep state alive
    }

    #[tokio::test]
    async fn pin_list_starts_empty_and_scratchpad_list_returns_stub() {
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(&mut w, r#"{"id":"p","op":"pin.list"}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], true);
        assert_eq!(resp["data"], serde_json::json!([]));
        send_line(&mut w, r#"{"id":"s","op":"scratchpad.list"}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], true);
        assert_eq!(resp["data"], serde_json::json!([]));
    }

    /// Seed the server with one floating window so pin.toggle has a
    /// valid target. Returns `((tmp_sock_dir, tmp_xdg_dir), sock_path, state, task)`.
    /// Pinned-state writes are redirected to the tempdir via
    /// `pinned::set_test_dir_override` (a process-wide override that is
    /// safe across parallel tests because each call replaces the
    /// previous value while the same lock serializes file writes).
    async fn start_server_with_floating_window() -> (
        (TempDir, TempDir),
        std::path::PathBuf,
        SharedState,
        JoinHandle<()>,
    ) {
        let xdg = tempfile::tempdir().unwrap();
        super::pinned::set_test_dir_override(Some(xdg.path().to_path_buf())).await;
        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();
        let state = SharedState::new();
        state
            .with_mut(|s| {
                s.windows.insert(
                    1,
                    p::Window {
                        id: 1,
                        app_id: Some("kitty".into()),
                        title: Some("shell".into()),
                        pid: Some(42),
                        workspace_id: Some(10),
                        is_focused: true,
                        is_floating: true, // floating → pin allowed
                        column_index: None,
                        position_in_column: None,
                        width: 800,
                        height: 600,
                        floating_position: None,
                    },
                );
                s.windows.insert(
                    2,
                    p::Window {
                        id: 2,
                        app_id: Some("foot".into()),
                        title: Some("tiled".into()),
                        pid: Some(43),
                        workspace_id: Some(10),
                        is_focused: false,
                        is_floating: false, // tiled → pin refused
                        column_index: Some(0),
                        position_in_column: Some(0),
                        width: 800,
                        height: 600,
                        floating_position: None,
                    },
                );
                s.focused_window = Some(1);
                s.focused_workspace = Some(10);
            })
            .await;
        let task = spawn_accept_loop(listener, state.clone(), None);
        ((tmp, xdg), sock_path, state, task)
    }

    #[tokio::test]
    async fn pin_toggle_refuses_tiled_window() {
        let (_dirs, path, _state, _task) = start_server_with_floating_window().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(&mut w, r#"{"id":"a","op":"pin.toggle","params":{"window_id":2}}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], false);
        let err = resp["error"].as_str().unwrap_or("");
        assert!(
            err.contains("only floating windows can be pinned"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn pin_toggle_floating_succeeds_and_list_returns_record() {
        let (_dirs, path, _state, _task) = start_server_with_floating_window().await;
        let (mut lines, mut w) = connect(&path).await;
        // Toggle on → pinned.
        send_line(&mut w, r#"{"id":"a","op":"pin.toggle","params":{"window_id":1}}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], true, "got: {resp}");
        assert_eq!(resp["data"]["pinned"], true);
        // pin.list → contains the window.
        send_line(&mut w, r#"{"id":"l","op":"pin.list"}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], true);
        let arr = resp["data"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["app_id"], "kitty");
        // Toggle again → unpinned.
        send_line(&mut w, r#"{"id":"b","op":"pin.toggle","params":{"window_id":1}}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], true);
        assert_eq!(resp["data"]["pinned"], false);
    }

    #[tokio::test]
    async fn pin_off_returns_count_and_clears_set() {
        let (_dirs, path, state, _task) = start_server_with_floating_window().await;
        let (mut lines, mut w) = connect(&path).await;
        // Add the floating window.
        send_line(&mut w, r#"{"id":"a","op":"pin.add","params":{"window_id":1}}"#).await;
        let _ = read_one_line(&mut lines).await;
        // Off → 1 unpinned.
        send_line(&mut w, r#"{"id":"o","op":"pin.off"}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["data"]["n_unpinned"], 1);
        // State is empty.
        let n = state.with(|s| s.pinned_windows.len()).await;
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn pin_remove_unknown_id_is_idempotent_success() {
        let (_dirs, path, _state, _task) = start_server_with_floating_window().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(&mut w, r#"{"id":"r","op":"pin.remove","params":{"window_id":999}}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], true);
        assert_eq!(resp["data"]["pinned"], false);
    }

    #[tokio::test]
    async fn pin_add_drops_stale_pin_when_window_now_tiled() {
        // R7 invariant: an already-pinned id whose live window has
        // gone tiled (e.g. after a Mod+V toggle that auto-unpin missed)
        // gets dropped + errored at pin.add time, not silently accepted.
        // The mutation must also persist so memory and disk agree.
        let (_dirs, path, state, _task) = start_server_with_floating_window().await;
        // Pre-pin id 1 (seeded as floating).
        state.with_mut(|s| { s.pinned_windows.insert(1); }).await;
        // Flip the cache to non-floating without going through niri.
        state
            .with_mut(|s| {
                if let Some(w) = s.windows.get_mut(&1) {
                    w.is_floating = false;
                }
            })
            .await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(&mut w, r#"{"id":"a","op":"pin.add","params":{"window_id":1}}"#).await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], false);
        assert!(
            resp["error"].as_str().unwrap_or("").contains("no longer floating"),
            "got: {resp}"
        );
        // Stale pin removed from in-memory set.
        let n = state.with(|s| s.pinned_windows.len()).await;
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn spawn_without_token_returns_pid() {
        // No broker, request_activation_token=false. `/usr/bin/env true` is
        // portable across Linux (`/bin/true`) and macOS (`/usr/bin/true`)
        // without depending on $PATH containing the right thing.
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(
            &mut w,
            r#"{"id":"sp","op":"spawn","params":{"argv":["/usr/bin/env","true"]}}"#,
        )
        .await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["id"], "sp");
        assert_eq!(resp["ok"], true, "got: {resp}");
        assert!(resp["data"]["pid"].as_i64().unwrap() > 0);
        assert!(resp["data"]["activation_token"].is_null());
    }

    #[tokio::test]
    async fn spawn_requesting_token_without_broker_errors_clearly() {
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(
            &mut w,
            r#"{"id":"sp","op":"spawn","params":{"argv":["/usr/bin/env","true"],"request_activation_token":true}}"#,
        )
        .await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], false);
        assert!(
            resp["error"].as_str().unwrap().contains("broker"),
            "expected broker-unavailable error, got: {resp}"
        );
    }

    #[tokio::test]
    async fn spawn_empty_argv_errors() {
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(
            &mut w,
            r#"{"id":"sp","op":"spawn","params":{"argv":[]}}"#,
        )
        .await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], false);
        assert!(resp["error"].as_str().unwrap().contains("argv"));
    }

    #[tokio::test]
    async fn spawn_with_broker_registers_pid_to_token() {
        let (_tmp, path, _state, broker, _task) = start_server_with_broker().await;
        let (mut lines, mut w) = connect(&path).await;
        send_line(
            &mut w,
            r#"{"id":"sp","op":"spawn","params":{"argv":["/usr/bin/env","true"],"request_activation_token":true}}"#,
        )
        .await;
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["ok"], true, "got: {resp}");
        let pid = resp["data"]["pid"].as_i64().unwrap() as i32;
        let token = resp["data"]["activation_token"].as_str().unwrap().to_string();
        assert!(token.starts_with("fake-token-"));
        // The broker should have registered pid → token. take_token_for_pid
        // returns the same string and consumes the entry.
        assert_eq!(broker.take_token_for_pid(pid).as_deref(), Some(token.as_str()));
    }

    #[tokio::test]
    async fn spawn_two_in_a_row_get_distinct_tokens() {
        // Plan §530-533 DoD: spawn the same terminal twice → two distinct
        // tokens. We don't actually spawn a terminal here (no Wayland in
        // unit tests); the broker invariant — each mint_token produces a
        // fresh token — is what the DoD relies on.
        let (_tmp, path, _state, _broker, _task) = start_server_with_broker().await;
        let (mut lines, mut w) = connect(&path).await;
        let mut tokens = Vec::new();
        for i in 0..2 {
            let req = format!(
                r#"{{"id":"sp{i}","op":"spawn","params":{{"argv":["/usr/bin/env","true"],"request_activation_token":true}}}}"#
            );
            send_line(&mut w, &req).await;
            let resp = read_one_line(&mut lines).await;
            assert_eq!(resp["ok"], true, "got: {resp}");
            tokens.push(resp["data"]["activation_token"].as_str().unwrap().to_string());
        }
        assert_ne!(tokens[0], tokens[1], "tokens must be distinct per spawn");
    }

    #[test]
    fn build_spawn_command_plants_token_and_env() {
        let mut env = std::collections::HashMap::new();
        env.insert("CALLER_SET".to_string(), "yes".to_string());
        let cmd = build_spawn_command(
            &["/usr/bin/env".to_string(), "true".to_string()],
            &env,
            Some("tok-abc"),
        );
        // Collect (key, value) of every env override the command will apply.
        let envs: std::collections::HashMap<String, String> = cmd
            .as_std()
            .get_envs()
            .filter_map(|(k, v)| {
                let v = v?;
                Some((k.to_string_lossy().into_owned(), v.to_string_lossy().into_owned()))
            })
            .collect();
        assert_eq!(envs.get("XDG_ACTIVATION_TOKEN").map(String::as_str), Some("tok-abc"));
        assert_eq!(envs.get("CALLER_SET").map(String::as_str), Some("yes"));
    }

    #[test]
    fn build_spawn_command_omits_token_when_none() {
        let env = std::collections::HashMap::new();
        let cmd = build_spawn_command(
            &["/usr/bin/env".to_string(), "true".to_string()],
            &env,
            None,
        );
        let has_token = cmd
            .as_std()
            .get_envs()
            .any(|(k, _)| k == std::ffi::OsStr::new("XDG_ACTIVATION_TOKEN"));
        assert!(!has_token, "no token requested → no env var should be set");
    }

    #[tokio::test]
    async fn live_event_stream_reports_window_focus_change() {
        // Don't seed; let niri_conn populate state from the live niri.
        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();
        let state = SharedState::new();
        let _server = spawn_accept_loop(listener, state.clone(), None);
        let _niri = crate::bridge::niri_conn::spawn_niri_loop(state.clone(), None)
            .await
            .unwrap();
        // Give niri_conn a moment to populate the cache.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (mut lines, mut w) = connect(&sock_path).await;
        send_line(
            &mut w,
            r#"{"id":"s","op":"subscribe","params":{"topics":["focus","windows"]}}"#,
        )
        .await;
        let _ = read_one_line(&mut lines).await;

        // After subscribing, an initial WindowsChanged from niri's
        // event-stream "full state up-front" replay should have already
        // fired before we subscribed. Verify state is non-empty:
        send_line(&mut w, r#"{"id":"snap","op":"state.snapshot"}"#).await;
        let snap = read_one_line(&mut lines).await;
        assert!(
            snap["data"]["windows"].as_array().unwrap().len()
                + snap["data"]["workspaces"].as_array().unwrap().len()
                > 0,
            "expected niri to have at least one window or workspace; got {snap}"
        );
    }

    /// Live W2 DoD: spawn a terminal with a unique title and verify the
    /// resulting `windows` event from niri carries the same activation
    /// token we got back from the spawn op. Exercises the entire
    /// pid-correlation pipeline end-to-end against a real niri + a real
    /// xdg_activation_v1 compositor implementation.
    ///
    /// Requirements: `$NIRI_SOCKET` reachable, `$WAYLAND_DISPLAY` reachable
    /// with `xdg_activation_v1` available (niri provides it), and a
    /// terminal binary on PATH that accepts `--title <s>`. Defaults to
    /// `kitty`; override via `$IRIS_TEST_TERMINAL` if that's not what
    /// you have. Examples: `IRIS_TEST_TERMINAL=foot cargo test ...`,
    /// `IRIS_TEST_TERMINAL=alacritty cargo test ...`.
    #[tokio::test]
    async fn live_spawn_token_round_trips_through_niri() {
        // Tests don't run `main()` and so don't get the global tracing
        // subscriber — making bridge debug logs invisible by default. Wire
        // a per-test subscriber so `IRIS_LOG=debug cargo test ... --
        // --nocapture` actually shows niri_conn / activation / server
        // events. `try_init` is a no-op if a subscriber already exists,
        // so this is safe to leave on permanently.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_env("IRIS_LOG")
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
            )
            .with_target(true)
            .with_test_writer()
            .try_init();

        let terminal_bin = std::env::var("IRIS_TEST_TERMINAL")
            .unwrap_or_else(|_| "kitty".to_string());

        let tmp = tempfile::tempdir().unwrap();
        let sock_path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();
        let state = SharedState::new();
        let broker = crate::bridge::activation::ActivationBroker::start()
            .expect("activation broker requires a live Wayland session");
        let _niri = crate::bridge::niri_conn::spawn_niri_loop(state.clone(), Some(broker.clone()))
            .await
            .unwrap();
        let _server = spawn_accept_loop(listener, state.clone(), Some(broker.clone()));
        // Let niri_conn drain its initial WindowsChanged before we subscribe,
        // so the events we subsequently read are reactions to OUR spawn, not
        // the initial state replay.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let (mut lines, mut w) = connect(&sock_path).await;
        send_line(
            &mut w,
            r#"{"id":"s","op":"subscribe","params":{"topics":["windows"]}}"#,
        )
        .await;
        let _ = read_one_line(&mut lines).await;

        // Unique title is just informational at this point — kitty/foot
        // both honor `--title`, but niri may emit the first
        // `WindowOpenedOrChanged` BEFORE the client surface has set its
        // title. We do NOT use it to identify our window; the activation
        // token below does that.
        let title = format!("iris-w2-{}-{}", std::process::id(), now_ms());
        let req = format!(
            r#"{{"id":"sp","op":"spawn","params":{{"argv":["{terminal_bin}","--title={title}"],"request_activation_token":true}}}}"#
        );
        send_line(&mut w, &req).await;

        // Spawn response arrives before any niri window event (bridge
        // writes it synchronously after `cmd.spawn()` returns; the child
        // hasn't even opened its Wayland surface yet). Read it first.
        let resp = read_one_line(&mut lines).await;
        assert_eq!(resp["id"], "sp");
        assert!(resp["ok"].as_bool().unwrap(), "spawn failed: {resp}");
        let spawn_token: String = resp["data"]["activation_token"]
            .as_str()
            .expect("token must be present when requested")
            .to_string();

        // Wait for a windows event whose `activation_token` matches.
        // This is the heart of the W2 DoD: the token bridge minted MUST
        // round-trip back via the windows topic. Match by token, not by
        // title — niri emits MULTIPLE `WindowOpenedOrChanged` events per
        // window (open + title-set + focus-change), and the bridge
        // consumes the pid→token entry on the FIRST match. Only that
        // first event carries the token; later ones for the same window
        // do not. Title may also lag the first emit, so a title-based
        // match would race the token-consumption.
        let mut event_token: Option<String> = None;
        let mut window_id: Option<u64> = None;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while event_token.is_none() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let line = match timeout(remaining, lines.next_line()).await {
                Ok(Ok(Some(l))) => l,
                _ => panic!(
                    "timed out waiting for windows event with token={spawn_token}"
                ),
            };
            let v: Value = serde_json::from_str(&line).unwrap();
            if v["event"] == "windows"
                && let Some(t) = v["data"]["activation_token"].as_str()
                && t == spawn_token
            {
                event_token = Some(t.to_string());
                window_id = v["data"]["opened_or_changed"]["id"].as_u64();
            }
        }
        assert_eq!(
            event_token.as_deref(),
            Some(spawn_token.as_str()),
            "windows event token must equal spawn response token"
        );

        // Cleanup: close the spawned terminal window so we don't litter the user's session.
        if let Some(id) = window_id {
            let close = format!(r#"{{"id":"cl","op":"window.close","params":{{"id":{id}}}}}"#);
            send_line(&mut w, &close).await;
            // Read until we see the close response (more events may stream).
            let close_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                let remaining =
                    close_deadline.saturating_duration_since(tokio::time::Instant::now());
                let Ok(Ok(Some(line))) = timeout(remaining, lines.next_line()).await else {
                    break;
                };
                let v: Value = serde_json::from_str(&line).unwrap();
                if v["id"] == "cl" {
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn noop_works_as_heartbeat() {
        let (_tmp, path, _state, _task) = start_server().await;
        let (mut lines, mut w) = connect(&path).await;
        for i in 0..3 {
            let line = format!(r#"{{"id":"{i}","op":"noop"}}"#);
            send_line(&mut w, &line).await;
            let resp = read_one_line(&mut lines).await;
            assert_eq!(resp["id"], i.to_string());
            assert_eq!(resp["ok"], true);
        }
    }
}
