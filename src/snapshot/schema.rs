//! On-disk snapshot format. v1 is layout-only — column placement, sizes,
//! floating geometry. App-specific hooks (terminal cwd, nvim sessions) get
//! a `[windows.hook]` table in W4; the schema is forward-compatible because
//! serde silently ignores unknown fields and `hook` is `Option`.
//!
//! Stability rule: bump `version` only on incompatible changes. Adding
//! optional fields is not incompatible. `version = 1` is forever-v1; if a
//! v2 ever happens, we refuse to load v1 with a clear error rather than
//! migrate.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::bridge::proto::FloatingPosition;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Snapshot {
    pub version: u32,
    pub name: String,
    pub saved_at: DateTime<Utc>,
    pub workspace: WorkspaceMeta,
    #[serde(default)]
    pub windows: Vec<WindowEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkspaceMeta {
    /// 1-based niri workspace index (matches user-visible `workspace-N` keybinds).
    pub index: u8,
    pub name: Option<String>,
    pub output: Option<String>,
    /// `save_id` of the window that was focused at save time. `None` means
    /// no window had focus on this workspace.
    pub focused_save_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowEntry {
    /// Snapshot-local id. niri's window id is lifetime-only and NEVER
    /// persisted (locked decision). `save_id` lets us reference the
    /// focused window without piggybacking on niri's id.
    pub save_id: u64,
    pub app_id: Option<String>,
    pub title: Option<String>,
    /// 0-based column index (`Window.column_index` semantics). `None` for
    /// floating windows.
    pub column_index: Option<u32>,
    /// 0-based tile index within the column. Captured but NOT restored in
    /// W3 (no per-window niri action exists for tile reordering); W4 may
    /// revisit. `None` for floating windows.
    pub position_in_column: Option<u32>,
    pub is_floating: bool,
    pub is_focused: bool,
    pub width: i32,
    pub height: i32,
    /// Present when `is_floating = true`. (x, y) in workspace-view coords.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub floating: Option<FloatingPosition>,
    /// Per-app hook data captured at save time. Used by `iris snapshot
    /// load` (default respawn mode) to reconstruct the spawn argv. W3
    /// snapshots have no `[windows.hook]` block; serde defaults to None
    /// and load falls back to GenericHook.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook: Option<HookData>,
}

/// What a hook captured at save time, sufficient to rebuild the spawn
/// argv at load time. Internally tagged on `kind` so the TOML reads as:
///
/// ```toml
/// [windows.hook]
/// kind = "terminal"
/// app_id = "foot"
/// cwd = "/home/rushi/code"
/// argv_fallback = ["foot"]
/// ```
///
/// Adding a new variant later is forward-compatible: existing snapshots
/// keep parsing; `serde(other)` would let unknown variants fall back to
/// a Generic-like catch-all, but for now we'd rather fail loudly than
/// silently drop hook data we don't understand. Snapshot version bump
/// only if a variant gets removed or its fields change incompatibly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HookData {
    /// Last-resort fallback. argv is run verbatim; cwd is honored if
    /// present (load wraps in `sh -lc 'cd <cwd> && exec <argv>'`).
    Generic {
        argv: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },
    /// foot/Alacritty/kitty/ghostty/wezterm. argv_fallback is the bare
    /// command if cwd capture failed (fallback to launching without
    /// `--working-directory`).
    Terminal {
        app_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        argv_fallback: Vec<String>,
    },
    /// Standalone neovim launched with `--listen`. session_path points
    /// at the sidecar `.vim` written by NeovimHook::capture; load runs
    /// `nvim -S <path>` which restores buffers, splits, marks.
    Neovim {
        session_path: PathBuf,
        argv_fallback: Vec<String>,
    },
    /// firefox/chromium/etc. Captured state is just the app_id; trust
    /// the browser's own session restore for tabs/state.
    Browser {
        app_id: String,
        argv_fallback: Vec<String>,
    },
    /// VS Code (code/Code/code-oss/code-insiders). Spawned with
    /// `--new-window <cwd>` so each saved entry gets its own window
    /// rather than merging into an existing instance (which wouldn't
    /// fire a fresh WindowOpenedOrChanged for token correlation).
    ///
    /// Wire form: `kind = "vscode"`. The default `snake_case` rename
    /// would produce `vs_code` which reads weird; explicit rename
    /// keeps it conventional. Locking this in before any snapshot
    /// ships — once on disk, this is a wire-compat decision.
    #[serde(rename = "vscode")]
    VsCode {
        app_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        argv_fallback: Vec<String>,
    },
}

impl Snapshot {
    /// Parse from a TOML string. Refuses to load a non-v1 snapshot — bump
    /// to v2 only if/when the format breaks compat (locked decision).
    pub fn from_toml(s: &str) -> Result<Self> {
        let snap: Snapshot =
            toml::from_str(s).context("parsing snapshot TOML")?;
        if snap.version != SCHEMA_VERSION {
            anyhow::bail!(
                "snapshot version {} not supported (expected {})",
                snap.version,
                SCHEMA_VERSION
            );
        }
        Ok(snap)
    }

    pub fn to_toml(&self) -> Result<String> {
        toml::to_string_pretty(self).context("serializing snapshot TOML")
    }

    pub fn from_path(path: &Path) -> Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        Self::from_toml(&s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Snapshot {
        Snapshot {
            version: 1,
            name: "test".into(),
            saved_at: DateTime::parse_from_rfc3339("2026-06-04T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            workspace: WorkspaceMeta {
                index: 1,
                name: Some("code".into()),
                output: Some("DP-1".into()),
                focused_save_id: Some(1),
            },
            windows: vec![
                WindowEntry {
                    save_id: 1,
                    app_id: Some("foot".into()),
                    title: Some("fish ~/code".into()),
                    column_index: Some(0),
                    position_in_column: Some(0),
                    is_floating: false,
                    is_focused: true,
                    width: 800,
                    height: 600,
                    floating: None,
                    hook: None,
                },
                WindowEntry {
                    save_id: 2,
                    app_id: Some("firefox".into()),
                    title: Some("github.com".into()),
                    column_index: None,
                    position_in_column: None,
                    is_floating: true,
                    is_focused: false,
                    width: 400,
                    height: 300,
                    floating: Some(FloatingPosition { x: 100.0, y: 200.0 }),
                    hook: None,
                },
            ],
        }
    }

    #[test]
    fn round_trip_preserves_fields() {
        let s = sample();
        let toml = s.to_toml().unwrap();
        let parsed = Snapshot::from_toml(&toml).unwrap();
        assert_eq!(parsed, s);
    }

    #[test]
    fn floating_block_omitted_for_tiled_windows() {
        let s = sample();
        let toml = s.to_toml().unwrap();
        // The tiled window (save_id=1) must not have a [windows.floating]
        // sub-table; the floating one (save_id=2) must.
        // Quick textual check: count "floating =" or "[[windows.floating"
        // — but TOML serializes Option<struct> as a nested table only when
        // present. Verify by parsing back and inspecting.
        let parsed = Snapshot::from_toml(&toml).unwrap();
        assert!(parsed.windows[0].floating.is_none());
        assert!(parsed.windows[1].floating.is_some());
    }

    #[test]
    fn rejects_unsupported_version() {
        let mut s = sample();
        s.version = 99;
        let toml = s.to_toml().unwrap();
        let err = Snapshot::from_toml(&toml).unwrap_err();
        assert!(format!("{err:#}").contains("version"));
    }

    #[test]
    fn hook_block_round_trips_for_each_variant() {
        // Each HookData variant should serialize → deserialize back to
        // an equal value. The TOML wire shape is `kind = "<variant>"`
        // followed by variant-specific fields.
        let variants = vec![
            HookData::Generic {
                argv: vec!["echo".into(), "hi".into()],
                cwd: Some("/tmp".into()),
            },
            HookData::Terminal {
                app_id: "foot".into(),
                cwd: Some("/home/rushi/code".into()),
                argv_fallback: vec!["foot".into()],
            },
            HookData::Neovim {
                session_path: PathBuf::from("/var/lib/iris/sessions/foo.vim"),
                argv_fallback: vec!["nvim".into()],
            },
            HookData::Browser {
                app_id: "firefox".into(),
                argv_fallback: vec!["firefox".into()],
            },
            HookData::VsCode {
                app_id: "code".into(),
                cwd: Some("/home/rushi/proj".into()),
                argv_fallback: vec!["code".into()],
            },
        ];
        for hook in variants {
            let mut s = sample();
            s.windows[0].hook = Some(hook.clone());
            let toml = s.to_toml().unwrap();
            let parsed = Snapshot::from_toml(&toml).unwrap();
            assert_eq!(parsed.windows[0].hook, Some(hook));
        }
    }

    #[test]
    fn hook_kind_wire_form_is_locked_per_variant() {
        // Round-trip tests pass even if a `#[serde(rename = ...)]` is
        // accidentally dropped — the produced TOML would just shift
        // (`vs_code` instead of `vscode`) and parse back fine. This
        // test pins the literal `kind = "..."` string each variant
        // emits, so a refactor that touches the rename or the
        // `rename_all = "snake_case"` strategy fails loudly.
        let cases: Vec<(HookData, &str)> = vec![
            (
                HookData::Generic { argv: vec!["x".into()], cwd: None },
                r#"kind = "generic""#,
            ),
            (
                HookData::Terminal {
                    app_id: "foot".into(),
                    cwd: None,
                    argv_fallback: vec!["foot".into()],
                },
                r#"kind = "terminal""#,
            ),
            (
                HookData::Neovim {
                    session_path: PathBuf::from("/tmp/x.vim"),
                    argv_fallback: vec!["nvim".into()],
                },
                r#"kind = "neovim""#,
            ),
            (
                HookData::Browser {
                    app_id: "firefox".into(),
                    argv_fallback: vec!["firefox".into()],
                },
                r#"kind = "browser""#,
            ),
            (
                HookData::VsCode {
                    app_id: "code".into(),
                    cwd: None,
                    argv_fallback: vec!["code".into()],
                },
                r#"kind = "vscode""#,
            ),
        ];
        for (hook, expected_kind) in cases {
            let mut s = sample();
            s.windows[0].hook = Some(hook);
            let toml = s.to_toml().unwrap();
            assert!(
                toml.contains(expected_kind),
                "expected {expected_kind} in serialized TOML, got:\n{toml}"
            );
            // Also confirm the auto-snake_case form for VsCode does NOT
            // appear — guards against accidental removal of the rename.
            assert!(
                !toml.contains(r#"kind = "vs_code""#),
                "vs_code wire form leaked through; check #[serde(rename)] on VsCode"
            );
        }
    }

    #[test]
    fn w3_snapshot_without_hook_block_still_loads() {
        // Forward-compat: a snapshot written by W3 has no `hook` field
        // and no `[windows.hook]` table. Loading it via W4's schema
        // should yield `hook: None`, NOT an error.
        let toml = r#"
version = 1
name = "w3-old"
saved_at = "2026-06-04T00:00:00Z"

[workspace]
index = 1

[[windows]]
save_id = 1
app_id = "foot"
title = "old"
column_index = 0
position_in_column = 0
is_floating = false
is_focused = true
width = 800
height = 600
"#;
        let parsed = Snapshot::from_toml(toml).unwrap();
        assert_eq!(parsed.windows.len(), 1);
        assert!(parsed.windows[0].hook.is_none());
    }

    #[test]
    fn empty_windows_round_trip() {
        let mut s = sample();
        s.windows.clear();
        s.workspace.focused_save_id = None;
        let toml = s.to_toml().unwrap();
        let parsed = Snapshot::from_toml(&toml).unwrap();
        assert_eq!(parsed, s);
    }
}
