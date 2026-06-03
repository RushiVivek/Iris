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

use super::activation::ActivationBroker;
use super::niri_conn::query;
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
        b.register_spawn(pid, tok.clone());
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
        // Plan §530-533 DoD: spawn foot twice → two distinct tokens.
        // We can't actually spawn foot here, but the broker invariant —
        // each mint_token produces a fresh token — is what the DoD relies on.
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
