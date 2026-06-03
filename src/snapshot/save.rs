//! `iris snapshot save NAME [--workspace IDX|NAME] [--force]`
//!
//! Flow:
//!   1. Snapshot bridge state (`state.snapshot`) — single round-trip,
//!      gives us the cached windows + workspaces.
//!   2. Resolve target workspace_id: `--workspace IDX|NAME` lookup against
//!      the snapshot's workspace list, or default to the focused workspace.
//!   3. Filter windows to that workspace_id.
//!   4. Translate `proto::Window` → `WindowEntry` (assigning save_id by
//!      index so the focused window can be referenced by id).
//!   5. Atomic write.

#![allow(dead_code)]

use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;

use crate::bridge::proto::{Op, Window, Workspace};
use crate::client::IrisClient;

use super::schema::{Snapshot, WindowEntry, WorkspaceMeta};
use super::store;

/// `iris snapshot save` entry. CLI-level flags map onto the params.
pub async fn run(
    client: &IrisClient,
    name: String,
    workspace: Option<String>,
    force: bool,
) -> Result<()> {
    // Single state.snapshot — windows + workspaces + focused ids in one trip.
    let snap_v = client
        .request(Op::StateSnapshot)
        .await
        .context("requesting state.snapshot from bridge")?;
    let live = parse_state_snapshot(&snap_v)?;

    let target = resolve_target(&live, workspace.as_deref())?;
    let snap = build_snapshot(&name, target, &live);
    store::write_snapshot(&name, &snap, force)?;
    eprintln!(
        "saved snapshot {name} ({} windows from workspace idx {})",
        snap.windows.len(),
        snap.workspace.index
    );
    Ok(())
}

/// Parsed `state.snapshot` response. Splitting parse from logic so the
/// tests can hand-build a `LiveState` without round-tripping JSON.
#[derive(Debug)]
struct LiveState {
    windows: Vec<Window>,
    workspaces: Vec<Workspace>,
    focused_window_id: Option<u64>,
    focused_workspace_id: Option<u64>,
}

fn parse_state_snapshot(v: &Value) -> Result<LiveState> {
    Ok(LiveState {
        windows: serde_json::from_value(v["windows"].clone())
            .context("windows array in state.snapshot")?,
        workspaces: serde_json::from_value(v["workspaces"].clone())
            .context("workspaces array in state.snapshot")?,
        focused_window_id: v["focused_window_id"].as_u64(),
        focused_workspace_id: v["focused_workspace_id"].as_u64(),
    })
}

#[derive(Debug, Clone, Copy)]
struct Target<'a> {
    id: u64,
    idx: u8,
    name: Option<&'a str>,
    output: Option<&'a str>,
}

fn resolve_target<'a>(live: &'a LiveState, requested: Option<&str>) -> Result<Target<'a>> {
    let ws = match requested {
        None => {
            // Default: focused workspace.
            let id = live
                .focused_workspace_id
                .ok_or_else(|| anyhow::anyhow!(
                    "no workspace is currently focused; pass --workspace explicitly"
                ))?;
            live.workspaces
                .iter()
                .find(|w| w.id == id)
                .ok_or_else(|| anyhow::anyhow!("focused workspace id {id} not in cache"))?
        }
        Some(spec) => {
            // Try numeric idx first, then fall back to name.
            if let Ok(idx) = spec.parse::<u8>() {
                live.workspaces
                    .iter()
                    .find(|w| w.idx == idx)
                    .ok_or_else(|| anyhow::anyhow!("no workspace with idx {idx}"))?
            } else {
                live.workspaces
                    .iter()
                    .find(|w| w.name.as_deref() == Some(spec))
                    .ok_or_else(|| anyhow::anyhow!("no workspace named {spec:?}"))?
            }
        }
    };
    Ok(Target {
        id: ws.id,
        idx: ws.idx,
        name: ws.name.as_deref(),
        output: ws.output.as_deref(),
    })
}

fn build_snapshot(name: &str, target: Target<'_>, live: &LiveState) -> Snapshot {
    // Windows on the target workspace, stable-ordered by (column_index,
    // position_in_column, id) so save_id assignment is deterministic
    // across runs that produce the same set.
    let mut wins: Vec<&Window> = live
        .windows
        .iter()
        .filter(|w| w.workspace_id == Some(target.id))
        .collect();
    wins.sort_by_key(|w| (
        w.column_index.unwrap_or(u32::MAX),
        w.position_in_column.unwrap_or(u32::MAX),
        w.id,
    ));

    let mut focused_save_id = None;
    let entries: Vec<WindowEntry> = wins
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let save_id = (i as u64) + 1;
            // Cross-check: the workspace's focused window is whichever
            // window has is_focused=true on that workspace. Use the
            // workspace's own focus, NOT the global focused_window_id, so
            // saving a non-focused workspace still records its last-active
            // window if niri tracks it — but for v1 we just use is_focused.
            if w.is_focused {
                focused_save_id = Some(save_id);
            }
            WindowEntry {
                save_id,
                app_id: w.app_id.clone(),
                title: w.title.clone(),
                column_index: w.column_index,
                position_in_column: w.position_in_column,
                is_floating: w.is_floating,
                is_focused: w.is_focused,
                width: w.width,
                height: w.height,
                floating: w.floating_position,
            }
        })
        .collect();

    Snapshot {
        version: super::schema::SCHEMA_VERSION,
        name: name.to_string(),
        saved_at: Utc::now(),
        workspace: WorkspaceMeta {
            index: target.idx,
            name: target.name.map(str::to_string),
            output: target.output.map(str::to_string),
            focused_save_id,
        },
        windows: entries,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::proto::{FloatingPosition, Window, Workspace};

    fn mk_window(
        id: u64,
        workspace_id: u64,
        app_id: &str,
        col: Option<u32>,
        pos: Option<u32>,
        is_floating: bool,
        is_focused: bool,
    ) -> Window {
        Window {
            id,
            app_id: Some(app_id.into()),
            title: Some(format!("title-{id}")),
            pid: Some(1000 + id as i32),
            workspace_id: Some(workspace_id),
            is_focused,
            is_floating,
            column_index: col,
            position_in_column: pos,
            width: 800,
            height: 600,
            floating_position: if is_floating {
                Some(FloatingPosition { x: 50.0, y: 60.0 })
            } else {
                None
            },
        }
    }

    fn mk_workspace(id: u64, idx: u8, name: Option<&str>, focused: bool) -> Workspace {
        Workspace {
            id,
            idx,
            name: name.map(String::from),
            output: Some("DP-1".into()),
            is_focused: focused,
            active_window_id: None,
        }
    }

    #[test]
    fn resolve_default_picks_focused_workspace() {
        let live = LiveState {
            windows: vec![],
            workspaces: vec![
                mk_workspace(10, 1, Some("a"), false),
                mk_workspace(20, 2, Some("b"), true),
            ],
            focused_window_id: None,
            focused_workspace_id: Some(20),
        };
        let t = resolve_target(&live, None).unwrap();
        assert_eq!(t.id, 20);
        assert_eq!(t.idx, 2);
    }

    #[test]
    fn resolve_explicit_idx() {
        let live = LiveState {
            windows: vec![],
            workspaces: vec![
                mk_workspace(10, 1, None, false),
                mk_workspace(20, 2, None, true),
            ],
            focused_window_id: None,
            focused_workspace_id: Some(20),
        };
        let t = resolve_target(&live, Some("1")).unwrap();
        assert_eq!(t.id, 10);
    }

    #[test]
    fn resolve_explicit_name() {
        let live = LiveState {
            windows: vec![],
            workspaces: vec![mk_workspace(10, 1, Some("code"), true)],
            focused_window_id: None,
            focused_workspace_id: Some(10),
        };
        let t = resolve_target(&live, Some("code")).unwrap();
        assert_eq!(t.id, 10);
    }

    #[test]
    fn resolve_missing_idx_errors() {
        let live = LiveState {
            windows: vec![],
            workspaces: vec![mk_workspace(10, 1, None, true)],
            focused_window_id: None,
            focused_workspace_id: Some(10),
        };
        let err = resolve_target(&live, Some("9")).unwrap_err();
        assert!(format!("{err:#}").contains("idx"));
    }

    #[test]
    fn resolve_no_focus_no_workspace_errors() {
        let live = LiveState {
            windows: vec![],
            workspaces: vec![mk_workspace(10, 1, None, false)],
            focused_window_id: None,
            focused_workspace_id: None,
        };
        let err = resolve_target(&live, None).unwrap_err();
        assert!(format!("{err:#}").contains("--workspace"));
    }

    #[test]
    fn build_filters_to_target_workspace_and_assigns_save_ids() {
        let live = LiveState {
            windows: vec![
                mk_window(1, 10, "foot", Some(0), Some(0), false, false),
                mk_window(2, 99, "wrong-ws", Some(0), Some(0), false, false),
                mk_window(3, 10, "firefox", Some(1), Some(0), false, true),
            ],
            workspaces: vec![mk_workspace(10, 1, None, true)],
            focused_window_id: Some(3),
            focused_workspace_id: Some(10),
        };
        let target = resolve_target(&live, None).unwrap();
        let snap = build_snapshot("test", target, &live);
        assert_eq!(snap.windows.len(), 2);
        // Stable ordering: by column_index then id. id=1 col=0, id=3 col=1
        // → save_id 1 first, save_id 2 second.
        assert_eq!(snap.windows[0].app_id.as_deref(), Some("foot"));
        assert_eq!(snap.windows[0].save_id, 1);
        assert_eq!(snap.windows[1].app_id.as_deref(), Some("firefox"));
        assert_eq!(snap.windows[1].save_id, 2);
        assert_eq!(snap.workspace.focused_save_id, Some(2));
    }

    #[test]
    fn build_handles_floating_window_capture() {
        let live = LiveState {
            windows: vec![mk_window(1, 10, "foot", None, None, true, true)],
            workspaces: vec![mk_workspace(10, 1, None, true)],
            focused_window_id: Some(1),
            focused_workspace_id: Some(10),
        };
        let target = resolve_target(&live, None).unwrap();
        let snap = build_snapshot("f", target, &live);
        assert_eq!(snap.windows.len(), 1);
        assert!(snap.windows[0].is_floating);
        assert_eq!(
            snap.windows[0].floating,
            Some(FloatingPosition { x: 50.0, y: 60.0 })
        );
    }

    #[test]
    fn build_for_non_focused_workspace_yields_no_focused_save_id() {
        // niri's `is_focused` on `Window` is globally exclusive — only one
        // window in the whole compositor is focused at a time. Saving a
        // non-focused workspace therefore produces focused_save_id = None.
        // Locking in that behavior so callers don't accidentally come to
        // rely on per-workspace last-focused tracking that doesn't exist.
        let live = LiveState {
            windows: vec![
                // ws 10 (the one we're saving): no window is focused.
                mk_window(1, 10, "foot", Some(0), Some(0), false, false),
                mk_window(2, 10, "firefox", Some(1), Some(0), false, false),
                // ws 20 (focused workspace): its window has is_focused=true.
                mk_window(3, 20, "other", Some(0), Some(0), false, true),
            ],
            workspaces: vec![
                mk_workspace(10, 1, None, false),
                mk_workspace(20, 2, None, true),
            ],
            focused_window_id: Some(3),
            focused_workspace_id: Some(20),
        };
        // Save ws 10 explicitly even though ws 20 is focused.
        let target = resolve_target(&live, Some("1")).unwrap();
        let snap = build_snapshot("non-focused", target, &live);
        assert_eq!(snap.windows.len(), 2);
        assert_eq!(snap.workspace.focused_save_id, None);
    }

    #[test]
    fn build_empty_workspace_has_no_focused_save_id() {
        let live = LiveState {
            windows: vec![],
            workspaces: vec![mk_workspace(10, 1, None, true)],
            focused_window_id: None,
            focused_workspace_id: Some(10),
        };
        let target = resolve_target(&live, None).unwrap();
        let snap = build_snapshot("empty", target, &live);
        assert!(snap.windows.is_empty());
        assert_eq!(snap.workspace.focused_save_id, None);
    }
}
