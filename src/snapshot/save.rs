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

use std::collections::HashSet;

use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::Value;
use tracing::warn;

use crate::bridge::proto::{Op, Window, Workspace};
use crate::client::IrisClient;

use super::hooks::{self, AppHook, GenericHook};
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

    // Pin/scratchpad windows are global UI furniture, not workspace-bound
    // state; saving them would create duplicates when load respawns.
    // Both ops return [] in W3 (stubs); W5/W6 fill in. Forward-compat
    // with no client-side change required.
    let protected_ids = fetch_protected_ids(client).await?;

    let target = resolve_target(&live, workspace.as_deref())?;
    let mut snap = build_snapshot(&name, target, &live, &protected_ids);

    // Hook capture is async + side-effecting (reads /proc, runs nvim
    // RPC, etc.). Split from the pure layout build so test fixtures
    // can construct a snapshot without a real /proc.
    attach_hooks(&mut snap, &live).await;

    store::write_snapshot(&name, &snap, force)?;
    eprintln!(
        "saved snapshot {name} ({} windows from workspace idx {})",
        snap.windows.len(),
        snap.workspace.index
    );
    Ok(())
}

/// Query the bridge's pin and scratchpad lists, union'd into a HashSet.
/// Empty in W3 (stubs). On error, log + treat as empty so save still
/// works in degraded mode.
async fn fetch_protected_ids(client: &IrisClient) -> Result<HashSet<u64>> {
    let mut ids = HashSet::new();
    for op in [Op::PinList, Op::ScratchpadList] {
        match client.request(op).await {
            Ok(Value::Array(arr)) => {
                ids.extend(arr.into_iter().filter_map(|v| v.as_u64()));
            }
            Ok(other) => {
                warn!("expected array from pin/scratchpad list, got: {other}");
            }
            Err(e) => {
                warn!("pin/scratchpad list failed: {e:#}; treating as empty");
            }
        }
    }
    Ok(ids)
}

/// Dispatch per-window hook capture, with downgrade-to-Generic on
/// failure. Mutates `snap.windows[i].hook` in place.
///
/// Pairs entries to live windows by `save_id`-was-derived-from-stable-sort
/// order, but more directly: we walk `live.windows` filtered to the
/// target workspace in the same order `build_snapshot` did. Re-deriving
/// is safe because `build_snapshot` is pure and the input hasn't changed.
async fn attach_hooks(snap: &mut Snapshot, live: &LiveState) {
    // We need to re-pair WindowEntry → live Window so the hook's
    // capture() sees app_id, pid, etc. The pairing logic here is
    // structurally identical to `load::match_pairs`: walk saved
    // entries in order, find the first-unused live window with
    // matching `(app_id, title)`. The only reason this isn't a
    // shared helper:
    //   - Save sorts `live_on_target` by `(column_index,
    //     position_in_column, id)` BEFORE matching, so the iteration
    //     order on the live side mirrors `build_snapshot`'s sort.
    //     This makes dup-pair behavior deterministic against the
    //     same sort key snapshot.windows was built against.
    //   - Load doesn't have a useful sort key — fresh windows.list
    //     is unsorted relative to the saved layout — so it walks
    //     declaration order on both sides.
    // If `build_snapshot`'s sort key ever changes, update the sort
    // here in lockstep or duplicate-pair behavior may diverge.
    let target_id = live
        .workspaces
        .iter()
        .find(|w| w.idx == snap.workspace.index)
        .map(|w| w.id);

    // Sort live windows by the SAME key build_snapshot uses, so the
    // i-th entry in snap.windows corresponds to the i-th entry here.
    // Without this matched ordering, duplicate (app_id, title) entries
    // could re-pair to a different window than build_snapshot intended,
    // misassigning hook data.
    let mut live_on_target: Vec<&Window> = live
        .windows
        .iter()
        .filter(|w| w.workspace_id == target_id)
        .collect();
    live_on_target.sort_by_key(|w| (
        w.column_index.unwrap_or(u32::MAX),
        w.position_in_column.unwrap_or(u32::MAX),
        w.id,
    ));

    // Track which live entries have already been paired so duplicate
    // (app_id, title) saved entries (e.g. two `kitty` terminals with the
    // same default title) pair to DIFFERENT live windows. Same pattern
    // as `load::match_pairs`. Belt-and-suspenders against both
    // duplicate-pair issues at once: matched ordering (above) + first-
    // unused match (below).
    let mut used = vec![false; live_on_target.len()];

    for entry in snap.windows.iter_mut() {
        let pair = live_on_target.iter().enumerate().find(|(i, w)| {
            !used[*i] && w.app_id == entry.app_id && w.title == entry.title
        });
        let Some((i, live_w)) = pair else {
            // Couldn't re-pair — should be impossible since `entry`
            // came from this same `live` set, but be defensive.
            continue;
        };
        used[i] = true;

        let hook = hooks::dispatch(live_w.app_id.as_deref());
        let captured = match hook.capture(live_w).await {
            Ok(data) => Some(data),
            Err(e) => {
                warn!(
                    app_id = ?live_w.app_id,
                    "hook {} capture failed: {e:#}; downgrading to generic",
                    hook.name(),
                );
                // Downgrade: try GenericHook. If that also fails (no
                // pid, no /proc/<pid>/cmdline, etc.), give up and store
                // None — load will fall back to argv-only spawning.
                match GenericHook.capture(live_w).await {
                    Ok(data) => Some(data),
                    Err(e2) => {
                        warn!(
                            app_id = ?live_w.app_id,
                            "generic capture also failed: {e2:#}; window will respawn argv-less",
                        );
                        None
                    }
                }
            }
        };
        entry.hook = captured;
    }
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

fn build_snapshot(
    name: &str,
    target: Target<'_>,
    live: &LiveState,
    protected_ids: &HashSet<u64>,
) -> Snapshot {
    // Windows on the target workspace, MINUS pinned + scratchpadded
    // (global UI furniture, see `run`). Stable-ordered by
    // (column_index, position_in_column, id) so save_id assignment is
    // deterministic across runs that produce the same set.
    let mut wins: Vec<&Window> = live
        .windows
        .iter()
        .filter(|w| w.workspace_id == Some(target.id))
        .filter(|w| !protected_ids.contains(&w.id))
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
                // Hook capture happens in W4. v1 build_snapshot doesn't
                // populate it; the W4 patch adds an async dispatch step
                // that fills this in.
                hook: None,
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
        let snap = build_snapshot("test", target, &live, &HashSet::new());
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
        let snap = build_snapshot("f", target, &live, &HashSet::new());
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
        let snap = build_snapshot("non-focused", target, &live, &HashSet::new());
        assert_eq!(snap.windows.len(), 2);
        assert_eq!(snap.workspace.focused_save_id, None);
    }

    #[test]
    fn build_excludes_protected_ids() {
        // Pinned/scratchpadded windows should be filtered out of save.
        // Symmetric with --clear's load-side exclusion: they're global
        // UI furniture, not workspace-bound state.
        let live = LiveState {
            windows: vec![
                mk_window(1, 10, "foot", Some(0), Some(0), false, false),
                mk_window(2, 10, "Slack", Some(1), Some(0), false, false),
                mk_window(3, 10, "firefox", Some(2), Some(0), false, true),
            ],
            workspaces: vec![mk_workspace(10, 1, None, true)],
            focused_window_id: Some(3),
            focused_workspace_id: Some(10),
        };
        let target = resolve_target(&live, None).unwrap();
        let mut protected = HashSet::new();
        protected.insert(2); // Slack is pinned
        let snap = build_snapshot("p", target, &live, &protected);
        assert_eq!(snap.windows.len(), 2, "Slack should be filtered out");
        let app_ids: Vec<&str> = snap
            .windows
            .iter()
            .filter_map(|w| w.app_id.as_deref())
            .collect();
        assert_eq!(app_ids, vec!["foot", "firefox"]);
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
        let snap = build_snapshot("empty", target, &live, &HashSet::new());
        assert!(snap.windows.is_empty());
        assert_eq!(snap.workspace.focused_save_id, None);
    }
}
