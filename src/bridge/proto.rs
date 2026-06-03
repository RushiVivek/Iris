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

/// Reference to a workspace by id (stable while alive) or by index (1-based,
/// matches niri's workspace-N keybinds).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WorkspaceRef {
    Id { id: u64 },
    Idx { idx: u8 },
}
