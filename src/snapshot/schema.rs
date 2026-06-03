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

use std::path::Path;

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
    fn empty_windows_round_trip() {
        let mut s = sample();
        s.windows.clear();
        s.workspace.focused_save_id = None;
        let toml = s.to_toml().unwrap();
        let parsed = Snapshot::from_toml(&toml).unwrap();
        assert_eq!(parsed, s);
    }
}
