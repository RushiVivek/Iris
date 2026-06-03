//! Wire protocol between iris bridge and clients.
//!
//! Framing: one JSON object per line on the Unix socket. No length prefix.
//! The same `proto` types are reused by clients via `crate::client`.
//!
//! # Schema stability
//!
//! Bridge pins this schema; niri-ipc may shift between niri releases and
//! bridge translates. If a niri update changes a field bridge needs, only
//! bridge is rebuilt — clients keep working against this surface.

#![allow(dead_code)]

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ───────────────────────────── Top-level envelope ─────────────────────────────

/// Client → bridge.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    /// Client-chosen identifier; bridge echoes it back in the matching response.
    pub id: String,
    #[serde(flatten)]
    pub op: Op,
}

/// Bridge → client. Either a response to a request (has `id`), or a fan-out
/// event (has `event`, no `id`). Encoded as untagged so JSON is flat.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServerMessage {
    Response(Response),
    Event(Event),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    pub fn ok(id: impl Into<String>, data: serde_json::Value) -> Self {
        Self { id: id.into(), ok: true, data: Some(data), error: None }
    }
    pub fn err(id: impl Into<String>, error: impl Into<String>) -> Self {
        Self { id: id.into(), ok: false, data: None, error: Some(error.into()) }
    }
}

// ────────────────────────────────── Operations ────────────────────────────────

/// All operations the bridge accepts. Tagged on `op` field so JSON is:
///   {"id":"abc","op":"windows.list","params":{...}}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", content = "params")]
pub enum Op {
    // ── Queries ──────────────────────────────────────────────────────────────
    #[serde(rename = "windows.list")]
    WindowsList,
    #[serde(rename = "windows.get")]
    WindowsGet { id: u64 },
    #[serde(rename = "workspaces.list")]
    WorkspacesList,
    #[serde(rename = "workspaces.focused")]
    WorkspacesFocused,
    /// Single round-trip: windows + workspaces + focus state. Snapshot uses this.
    #[serde(rename = "state.snapshot")]
    StateSnapshot,

    // ── Actions (forwarded to niri) ──────────────────────────────────────────
    #[serde(rename = "window.focus")]
    WindowFocus { id: u64 },
    #[serde(rename = "window.close")]
    WindowClose { id: u64 },
    #[serde(rename = "window.move_to_workspace")]
    WindowMoveToWorkspace {
        id: u64,
        workspace: WorkspaceRef,
    },
    #[serde(rename = "window.toggle_floating")]
    WindowToggleFloating { id: u64 },

    /// Spawn a process. Bridge runs the command directly (NOT via niri's
    /// `Action::Spawn`) so the child inherits the user-session env that
    /// bridge itself was launched in. If `request_activation_token` is true
    /// the bridge mints an XDG activation token, sets `XDG_ACTIVATION_TOKEN`
    /// in the child's env, and returns the token in the response so the
    /// client can correlate the resulting `windows` event back to its spawn.
    #[serde(rename = "spawn")]
    Spawn {
        argv: Vec<String>,
        #[serde(default)]
        env: HashMap<String, String>,
        #[serde(default)]
        request_activation_token: bool,
    },

    // ── Subscription control ─────────────────────────────────────────────────
    #[serde(rename = "subscribe")]
    Subscribe { topics: Vec<String> },
    #[serde(rename = "unsubscribe")]
    Unsubscribe { topics: Vec<String> },

    // ── Liveness ─────────────────────────────────────────────────────────────
    /// No-op; client uses this as a heartbeat to detect a dead bridge.
    #[serde(rename = "noop")]
    Noop,
}

// ─────────────────────────────────── Events ───────────────────────────────────

/// Bridge → client fan-out. No `id`; topic discriminates.
/// `Clone` so the broadcast channel can hand each subscriber its own copy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub event: String,
    /// Epoch milliseconds at emit time.
    pub ts: i64,
    pub data: serde_json::Value,
}

/// Topic name constants — keep in sync with the plan's "Topics" list.
pub mod topics {
    pub const WINDOWS: &str = "windows";
    pub const WORKSPACES: &str = "workspaces";
    pub const FOCUS: &str = "focus";
    pub const FOCUS_SAMPLE: &str = "focus.sample";
    pub const FOCUS_SESSION_END: &str = "focus.session_end";
    pub const IDLE: &str = "idle";
    pub const STATE: &str = "state";
}

// ────────────────────────────── Domain types ─────────────────────────────────

/// Bridge's normalized window record. Kept narrower than niri-ipc's so we
/// can absorb upstream renames without breaking clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Window {
    pub id: u64,
    pub app_id: Option<String>,
    pub title: Option<String>,
    pub pid: Option<i32>,
    pub workspace_id: Option<u64>,
    pub is_focused: bool,
    pub is_floating: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: u64,
    pub idx: u8,
    pub name: Option<String>,
    pub output: Option<String>,
    pub is_focused: bool,
    pub active_window_id: Option<u64>,
}

/// Reference to a workspace. Mirrors niri's `WorkspaceReferenceArg`:
///   - `id`   — stable while the workspace is alive
///   - `idx`  — 1-based, matches niri's workspace-N keybinds; *changes* on reorder
///   - `name` — optional workspace name from niri config
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkspaceRef {
    Id { id: u64 },
    Idx { idx: u8 },
    Name { name: String },
}

// ─────────────────────────────────── Tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    /// Round-trip a Request through JSON and check the wire shape we
    /// document in the README is what serde actually produces.
    #[test]
    fn request_windows_list_wire_shape() {
        let req = Request {
            id: "abc".into(),
            op: Op::WindowsList,
        };
        let v: Value = serde_json::to_value(&req).unwrap();
        assert_eq!(v, json!({"id": "abc", "op": "windows.list"}));
    }

    #[test]
    fn request_with_params_wire_shape() {
        let req = Request {
            id: "1".into(),
            op: Op::WindowFocus { id: 42 },
        };
        let v: Value = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v,
            json!({"id": "1", "op": "window.focus", "params": {"id": 42}})
        );
    }

    #[test]
    fn move_to_workspace_with_each_ref_kind() {
        for (variant, expected) in [
            (
                WorkspaceRef::Id { id: 7 },
                json!({"id": 7}),
            ),
            (
                WorkspaceRef::Idx { idx: 3 },
                json!({"idx": 3}),
            ),
            (
                WorkspaceRef::Name { name: "code".into() },
                json!({"name": "code"}),
            ),
        ] {
            let req = Request {
                id: "x".into(),
                op: Op::WindowMoveToWorkspace {
                    id: 1,
                    workspace: variant,
                },
            };
            let v: Value = serde_json::to_value(&req).unwrap();
            assert_eq!(v["params"]["workspace"], expected);
        }
    }

    #[test]
    fn spawn_minimal_wire_shape() {
        let line = r#"{"id":"sp","op":"spawn","params":{"argv":["foot"]}}"#;
        let req: Request = serde_json::from_str(line).unwrap();
        match req.op {
            Op::Spawn { argv, env, request_activation_token } => {
                assert_eq!(argv, vec!["foot".to_string()]);
                assert!(env.is_empty(), "env defaults to empty map");
                assert!(!request_activation_token, "default false");
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn spawn_full_wire_shape_round_trip() {
        let mut env = HashMap::new();
        env.insert("FOO".into(), "bar".into());
        let req = Request {
            id: "sp".into(),
            op: Op::Spawn {
                argv: vec!["foot".into(), "-e".into(), "fish".into()],
                env,
                request_activation_token: true,
            },
        };
        let v: Value = serde_json::to_value(&req).unwrap();
        assert_eq!(v["op"], "spawn");
        assert_eq!(v["params"]["argv"], json!(["foot", "-e", "fish"]));
        assert_eq!(v["params"]["env"]["FOO"], "bar");
        assert_eq!(v["params"]["request_activation_token"], true);

        // Round-trip back to the typed form too.
        let parsed: Request = serde_json::from_value(v).unwrap();
        match parsed.op {
            Op::Spawn { argv, .. } => assert_eq!(argv.len(), 3),
            _ => panic!("lost variant after round-trip"),
        }
    }

    #[test]
    fn subscribe_topics_round_trip() {
        let line = r#"{"id":"s","op":"subscribe","params":{"topics":["windows","focus"]}}"#;
        let req: Request = serde_json::from_str(line).unwrap();
        match req.op {
            Op::Subscribe { topics } => {
                assert_eq!(topics, vec!["windows", "focus"]);
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn response_ok_serializes_with_data_no_error() {
        let r = Response::ok("42", json!({"foo": 1}));
        let v: Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["id"], "42");
        assert_eq!(v["ok"], true);
        assert_eq!(v["data"], json!({"foo": 1}));
        assert!(v.get("error").is_none(), "error must be omitted on ok");
    }

    #[test]
    fn response_err_serializes_with_error_no_data() {
        let r = Response::err("42", "boom");
        let v: Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"], "boom");
        assert!(v.get("data").is_none(), "data must be omitted on err");
    }

    #[test]
    fn server_message_event_round_trip() {
        let ev = Event {
            event: "windows".into(),
            ts: 1_717_420_800_123,
            data: json!({"closed": 7}),
        };
        let line = serde_json::to_string(&ServerMessage::Event(ev)).unwrap();
        let parsed: ServerMessage = serde_json::from_str(&line).unwrap();
        match parsed {
            ServerMessage::Event(e) => {
                assert_eq!(e.event, "windows");
                assert_eq!(e.ts, 1_717_420_800_123);
            }
            ServerMessage::Response(_) => panic!("expected event"),
        }
    }

    #[test]
    fn server_message_response_round_trip() {
        let line = r#"{"id":"1","ok":true,"data":[]}"#;
        let parsed: ServerMessage = serde_json::from_str(line).unwrap();
        match parsed {
            ServerMessage::Response(r) => {
                assert_eq!(r.id, "1");
                assert!(r.ok);
            }
            ServerMessage::Event(_) => panic!("expected response"),
        }
    }

    #[test]
    fn unknown_op_fails_to_deserialize() {
        // Defense-in-depth: unknown ops should be a parse error, not silently
        // matched as something else.
        let line = r#"{"id":"x","op":"definitely.not.a.real.op"}"#;
        let r: Result<Request, _> = serde_json::from_str(line);
        assert!(r.is_err());
    }

    #[test]
    fn topic_constants_match_documented_strings() {
        // README + plan reference these as plain strings; if they ever rename
        // here, clients break. Lock the names with this test.
        assert_eq!(topics::WINDOWS, "windows");
        assert_eq!(topics::WORKSPACES, "workspaces");
        assert_eq!(topics::FOCUS, "focus");
        assert_eq!(topics::FOCUS_SAMPLE, "focus.sample");
        assert_eq!(topics::FOCUS_SESSION_END, "focus.session_end");
        assert_eq!(topics::IDLE, "idle");
        assert_eq!(topics::STATE, "state");
    }
}
