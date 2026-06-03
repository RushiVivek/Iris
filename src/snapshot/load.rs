//! `iris snapshot load NAME [--workspace IDX|NAME] [--clear] [--timeout SECS]`
//!
//! Layout-only restore: assumes saved apps are already running, rearranges
//! them. Respawning + per-app hooks are W4.
//!
//! Order of operations matters because niri auto-redistributes column
//! widths and tile heights on insert/remove. The plan calls for:
//!
//!   PLACE → RECONCILE_FLOATING → ORDER_COLUMNS → SIZE → POSITION_FLOATING → FOCUS
//!
//! Sizes go LAST so niri's redistribution doesn't undo them. Floating
//! position goes after `set_size` so it doesn't get clamped against a
//! still-default size.
//!
//! Pure logic (matcher, op-sequence builder, clear-set computation) is
//! split out of `run` so it's tested without an IrisClient.

#![allow(dead_code)]

use std::collections::HashSet;

use anyhow::{Context, Result};
use serde_json::Value;

use crate::bridge::proto::{Op, Window, WorkspaceRef};
use crate::client::IrisClient;

use super::schema::{Snapshot, WindowEntry};
use super::store;

/// `iris snapshot load` entry. CLI-level flags map onto the params.
pub async fn run(
    client: &IrisClient,
    name: String,
    workspace: Option<String>,
    clear: bool,
    _timeout: Option<u64>,
) -> Result<()> {
    let snap = store::read_snapshot(&name)
        .with_context(|| format!("loading snapshot {name}"))?;

    // Resolve target workspace from --workspace flag or saved index.
    let target = resolve_target(client, workspace.as_deref(), snap.workspace.index).await?;

    // Match BEFORE clearing so --clear doesn't accidentally close the
    // saved windows themselves. We use the same windows.list snapshot for
    // both deciding what to close and what to place; subsequent layout
    // ops still resolve against live ids that survive the clear.
    let live = fetch_windows(client).await?;
    let pairs = match_pairs(&snap.windows, &live);
    let matched_ids: HashSet<u64> = pairs.iter().map(|(_, w)| w.id).collect();

    if clear {
        let pinned = fetch_protected_set(client, Op::PinList).await?;
        let scratch = fetch_protected_set(client, Op::ScratchpadList).await?;
        let close_ids =
            compute_clear_set(&live, target.id, &pinned, &scratch, &matched_ids);
        for id in close_ids {
            client.request(Op::WindowClose { id }).await
                .with_context(|| format!("closing window {id} for --clear"))?;
        }
    }

    apply_layout(client, &snap, &pairs, target.id).await?;

    let unmatched = snap.windows.len() - pairs.len();
    eprintln!(
        "loaded snapshot {name}: {}/{} windows placed{}",
        pairs.len(),
        snap.windows.len(),
        if unmatched > 0 { format!(", {unmatched} unmatched") } else { String::new() }
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct Target {
    id: u64,
    idx: u8,
}

async fn resolve_target(
    client: &IrisClient,
    requested: Option<&str>,
    saved_idx: u8,
) -> Result<Target> {
    let v = client.request(Op::WorkspacesList).await
        .context("fetching workspaces.list")?;
    let workspaces: Vec<crate::bridge::proto::Workspace> =
        serde_json::from_value(v).context("parsing workspaces.list")?;

    let pick = match requested {
        None => workspaces.iter().find(|w| w.idx == saved_idx)
            .ok_or_else(|| anyhow::anyhow!("workspace idx {saved_idx} from snapshot not present on this niri"))?,
        Some(spec) => {
            if let Ok(idx) = spec.parse::<u8>() {
                workspaces.iter().find(|w| w.idx == idx)
                    .ok_or_else(|| anyhow::anyhow!("no workspace with idx {idx}"))?
            } else {
                workspaces.iter().find(|w| w.name.as_deref() == Some(spec))
                    .ok_or_else(|| anyhow::anyhow!("no workspace named {spec:?}"))?
            }
        }
    };
    Ok(Target { id: pick.id, idx: pick.idx })
}

async fn fetch_windows(client: &IrisClient) -> Result<Vec<Window>> {
    let v = client.request(Op::WindowsList).await.context("windows.list")?;
    serde_json::from_value(v).context("parsing windows.list")
}

async fn fetch_protected_set(client: &IrisClient, op: Op) -> Result<HashSet<u64>> {
    let v = client.request(op).await?;
    let ids: Vec<u64> = match v {
        Value::Array(arr) => arr.into_iter().filter_map(|x| x.as_u64()).collect(),
        _ => Vec::new(),
    };
    Ok(ids.into_iter().collect())
}

/// Pure: which window ids should `--clear` close. Anything on the target
/// workspace that isn't pinned, scratchpadded, OR a live match for one of
/// the snapshot's saved windows. The last exclusion stops `--clear` from
/// closing the very windows we're about to rearrange.
fn compute_clear_set(
    live: &[Window],
    target_id: u64,
    pinned: &HashSet<u64>,
    scratch: &HashSet<u64>,
    matched: &HashSet<u64>,
) -> Vec<u64> {
    live.iter()
        .filter(|w| w.workspace_id == Some(target_id))
        .map(|w| w.id)
        .filter(|id| {
            !pinned.contains(id) && !scratch.contains(id) && !matched.contains(id)
        })
        .collect()
}

/// Pure: pair saved entries with live windows by (app_id, title). Stable
/// pair-by-encounter-order: duplicates pair in the order they appear in
/// each list. Saved entries that don't match any live window are dropped
/// (caller can see the count delta to surface "N unmatched").
fn match_pairs<'s, 'l>(
    saved: &'s [WindowEntry],
    live: &'l [Window],
) -> Vec<(&'s WindowEntry, &'l Window)> {
    let mut used = vec![false; live.len()];
    let mut pairs = Vec::with_capacity(saved.len());
    for s in saved {
        let idx = live.iter().enumerate().find(|(i, w)| {
            !used[*i] && w.app_id == s.app_id && w.title == s.title
        });
        if let Some((i, w)) = idx {
            used[i] = true;
            pairs.push((s, w));
        }
    }
    pairs
}

/// Issue the actual layout actions for matched pairs. See module docs for
/// the ordering rationale.
async fn apply_layout(
    client: &IrisClient,
    snap: &Snapshot,
    pairs: &[(&WindowEntry, &Window)],
    target_id: u64,
) -> Result<()> {
    let target_ref = WorkspaceRef::Id { id: target_id };

    // 1. PLACE.
    for (_, live) in pairs {
        client.request(Op::WindowMoveToWorkspace {
            id: live.id,
            workspace: target_ref.clone(),
        }).await.with_context(|| format!("move id={} to workspace", live.id))?;
    }

    // 2. RECONCILE FLOATING STATE.
    for (saved, live) in pairs {
        if saved.is_floating != live.is_floating {
            client.request(Op::WindowToggleFloating { id: live.id }).await
                .with_context(|| format!("toggle floating id={}", live.id))?;
        }
    }

    // 3. ORDER COLUMNS.
    //
    // niri's MoveColumnToIndex(N) moves the focused column to absolute slot
    // N, but its shift semantics for OTHER columns aren't documented and
    // we'd rather not depend on them. Instead, restore the column order
    // by walking unique saved column_indexes in DESCENDING order and
    // moving each window's column to slot 1 (first). After all moves, the
    // window processed last (saved col 0) sits at column 1; the previous
    // (saved col 1) is now at column 2 (pushed by the col-0 move); and
    // so on. The relative order of unique columns is therefore restored
    // regardless of how niri treats intermediate positions.
    //
    // Same-column duplicates (multi-tile columns) are a known v1
    // limitation: we issue the column move only for one representative
    // window per saved column_index. The other tiles end up in their
    // own columns rather than stacked. Restoring stacked columns needs
    // an id-targeted "consume into column" action niri doesn't expose
    // in 25.11.0; revisit when (if) it does.
    use std::collections::BTreeMap;
    let mut by_col: BTreeMap<u32, &Window> = BTreeMap::new();
    for (saved, live) in pairs {
        if saved.is_floating {
            continue;
        }
        if let Some(col) = saved.column_index {
            // First saved window for this col wins; subsequent ones are
            // dropped from column-ordering (the limitation noted above).
            by_col.entry(col).or_insert(*live);
        }
    }
    // Iterate descending so col-0 ends up first, col-1 second (pushed
    // right by the col-0 move), etc.
    for (_col, live) in by_col.iter().rev() {
        client.request(Op::WindowMoveColumnToIndex {
            id: live.id,
            index: 0, // 0-based "first" — bridge translates to niri's 1
        })
        .await
        .with_context(|| format!("move column id={} to first", live.id))?;
    }

    // 4. SIZE — every matched window. Done after column ordering so
    //    niri's auto-redistribution doesn't undo it.
    for (saved, live) in pairs {
        client.request(Op::WindowSetSize {
            id: live.id,
            w: saved.width,
            h: saved.height,
        }).await.with_context(|| format!("size id={}", live.id))?;
    }

    // 5. POSITION FLOATING.
    for (saved, live) in pairs {
        if let (true, Some(pos)) = (saved.is_floating, saved.floating) {
            client.request(Op::WindowSetFloatingPosition {
                id: live.id,
                x: pos.x,
                y: pos.y,
            }).await.with_context(|| format!("floating-position id={}", live.id))?;
        }
    }

    // 6. FOCUS the saved focused window if any.
    if let Some(focused_save_id) = snap.workspace.focused_save_id {
        if let Some((_, live)) = pairs.iter().find(|(s, _)| s.save_id == focused_save_id) {
            client.request(Op::WindowFocus { id: live.id }).await
                .with_context(|| format!("focus id={}", live.id))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::proto::FloatingPosition;

    fn mk_live(id: u64, ws: u64, app: &str, title: &str) -> Window {
        Window {
            id,
            app_id: Some(app.into()),
            title: Some(title.into()),
            pid: None,
            workspace_id: Some(ws),
            is_focused: false,
            is_floating: false,
            column_index: Some(0),
            position_in_column: Some(0),
            width: 100,
            height: 100,
            floating_position: None,
        }
    }

    fn mk_saved(save_id: u64, app: &str, title: &str) -> WindowEntry {
        WindowEntry {
            save_id,
            app_id: Some(app.into()),
            title: Some(title.into()),
            column_index: Some(0),
            position_in_column: Some(0),
            is_floating: false,
            is_focused: false,
            width: 100,
            height: 100,
            floating: None,
        }
    }

    #[test]
    fn clear_set_excludes_pin_and_scratchpad() {
        let live = vec![
            mk_live(1, 10, "a", "x"),
            mk_live(2, 10, "b", "y"),
            mk_live(3, 10, "c", "z"),
            mk_live(99, 99, "wrong-ws", "n/a"), // different workspace
        ];
        let pinned: HashSet<u64> = [2u64].into_iter().collect();
        let scratch: HashSet<u64> = [3u64].into_iter().collect();
        let matched: HashSet<u64> = HashSet::new();
        let close = compute_clear_set(&live, 10, &pinned, &scratch, &matched);
        assert_eq!(close, vec![1]);
    }

    #[test]
    fn clear_set_empty_when_workspace_empty() {
        let live: Vec<Window> = vec![];
        let close = compute_clear_set(
            &live,
            10,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert!(close.is_empty());
    }

    #[test]
    fn clear_set_excludes_matched_windows() {
        // Regression: --clear must not close the very windows we're about
        // to place. Without the matched-set exclusion, every saved window
        // already on the target workspace gets nuked first.
        let live = vec![
            mk_live(1, 10, "foot", "shell"),
            mk_live(2, 10, "stray-app", "x"),
        ];
        let matched: HashSet<u64> = [1u64].into_iter().collect();
        let close = compute_clear_set(
            &live,
            10,
            &HashSet::new(),
            &HashSet::new(),
            &matched,
        );
        assert_eq!(close, vec![2]);
    }

    #[test]
    fn match_pairs_simple() {
        let saved = vec![
            mk_saved(1, "foot", "a"),
            mk_saved(2, "firefox", "b"),
        ];
        let live = vec![
            mk_live(10, 1, "firefox", "b"),
            mk_live(20, 1, "foot", "a"),
        ];
        let pairs = match_pairs(&saved, &live);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0.save_id, 1);
        assert_eq!(pairs[0].1.id, 20);
        assert_eq!(pairs[1].0.save_id, 2);
        assert_eq!(pairs[1].1.id, 10);
    }

    #[test]
    fn match_pairs_duplicates_pair_in_order() {
        // Two foots in saved + two in live. Each saved entry should pair
        // with a distinct live one in encounter order.
        let saved = vec![
            mk_saved(1, "foot", "same"),
            mk_saved(2, "foot", "same"),
        ];
        let live = vec![
            mk_live(100, 1, "foot", "same"),
            mk_live(200, 1, "foot", "same"),
        ];
        let pairs = match_pairs(&saved, &live);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].1.id, 100);
        assert_eq!(pairs[1].1.id, 200);
    }

    #[test]
    fn match_pairs_unmatched_dropped() {
        let saved = vec![
            mk_saved(1, "foot", "a"),
            mk_saved(2, "missing-app", "b"),
        ];
        let live = vec![mk_live(10, 1, "foot", "a")];
        let pairs = match_pairs(&saved, &live);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0.save_id, 1);
    }

    #[test]
    fn match_pairs_more_live_than_saved_leaves_extras() {
        let saved = vec![mk_saved(1, "foot", "a")];
        let live = vec![
            mk_live(10, 1, "foot", "a"),
            mk_live(20, 1, "foot", "a"),
        ];
        let pairs = match_pairs(&saved, &live);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1.id, 10);
    }

    #[test]
    fn match_pairs_title_mismatch_is_unmatched() {
        let saved = vec![mk_saved(1, "foot", "a")];
        let live = vec![mk_live(10, 1, "foot", "b")];
        let pairs = match_pairs(&saved, &live);
        assert!(pairs.is_empty());
    }

    #[test]
    fn match_pairs_handles_none_app_id() {
        // Some apps don't set app_id (None == None should still match).
        let mut s = mk_saved(1, "foot", "a");
        s.app_id = None;
        let mut l = mk_live(10, 1, "foot", "a");
        l.app_id = None;
        let saved = [s];
        let live = [l];
        let pairs = match_pairs(&saved, &live);
        assert_eq!(pairs.len(), 1);
    }

    // ─── apply_layout op-sequence tests via a recording fake bridge ───
    //
    // We spin up a UDS-backed fake bridge that responds OK to every
    // request and records the (op_name, params) tuple per line. Then
    // apply_layout drives an IrisClient against it, and the test asserts
    // both the SET of ops issued and their RELATIVE ORDER (PLACE before
    // SIZE before FOCUS).

    use crate::client::IrisClient;
    use crate::snapshot::schema::{Snapshot as Snap, WorkspaceMeta};
    use std::sync::Arc;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixListener;
    use tokio::sync::Mutex as TokioMutex;

    /// Spin up a fake bridge that records every request line and replies
    /// `{"ok":true,"data":{}}` to each. Returns the connected client and
    /// a handle to the recorded list (mutex-guarded, populated by the
    /// background task).
    async fn recording_fake_bridge() -> (
        tempfile::TempDir,
        IrisClient,
        Arc<TokioMutex<Vec<serde_json::Value>>>,
    ) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("iris.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let recorded = Arc::new(TokioMutex::new(Vec::new()));
        let recorded_for_task = recorded.clone();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (r, mut w) = stream.into_split();
            let mut lines = BufReader::new(r).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let v: serde_json::Value = serde_json::from_str(&line).unwrap();
                let id = v["id"].as_str().unwrap_or("").to_string();
                recorded_for_task.lock().await.push(v);
                let resp = serde_json::json!({"id": id, "ok": true, "data": {}});
                let s = serde_json::to_string(&resp).unwrap();
                if w.write_all(s.as_bytes()).await.is_err() {
                    break;
                }
                if w.write_all(b"\n").await.is_err() {
                    break;
                }
            }
        });

        let client = IrisClient::connect_at(path).await.unwrap();
        (tmp, client, recorded)
    }

    fn op_names(rs: &[serde_json::Value]) -> Vec<&str> {
        rs.iter().map(|v| v["op"].as_str().unwrap_or("")).collect()
    }

    fn mini_snap(focused_save_id: Option<u64>) -> Snap {
        Snap {
            version: 1,
            name: "t".into(),
            saved_at: chrono::Utc::now(),
            workspace: WorkspaceMeta {
                index: 1,
                name: None,
                output: None,
                focused_save_id,
            },
            windows: vec![],
        }
    }

    #[tokio::test]
    async fn apply_layout_emits_ops_in_correct_order() {
        let (_tmp, client, recorded) = recording_fake_bridge().await;
        let saved_a = WindowEntry {
            save_id: 1,
            app_id: Some("foot".into()),
            title: Some("a".into()),
            column_index: Some(0),
            position_in_column: Some(0),
            is_floating: false,
            is_focused: true,
            width: 800,
            height: 600,
            floating: None,
        };
        let saved_b = WindowEntry {
            save_id: 2,
            app_id: Some("foot".into()),
            title: Some("b".into()),
            column_index: Some(1),
            position_in_column: Some(0),
            is_floating: false,
            is_focused: false,
            width: 700,
            height: 500,
            floating: None,
        };
        let live_a = mk_live(10, 99, "foot", "a"); // currently elsewhere
        let live_b = mk_live(20, 99, "foot", "b");
        let pairs: Vec<(&WindowEntry, &Window)> =
            vec![(&saved_a, &live_a), (&saved_b, &live_b)];
        let snap = Snap {
            workspace: WorkspaceMeta {
                index: 1,
                name: None,
                output: None,
                focused_save_id: Some(1),
            },
            windows: vec![saved_a.clone(), saved_b.clone()],
            ..mini_snap(Some(1))
        };

        apply_layout(&client, &snap, &pairs, /* target_id */ 1).await.unwrap();

        let recs = recorded.lock().await;
        let ops = op_names(&recs);
        // Expected sequence (no toggle_floating since both saved+live are tiled):
        //   move_to_workspace, move_to_workspace,
        //   window.move_column_to_index, window.move_column_to_index,
        //   set_size, set_size,
        //   window.focus
        let move_ws = ops
            .iter()
            .position(|o| *o == "window.move_to_workspace")
            .unwrap();
        let move_col = ops
            .iter()
            .position(|o| *o == "window.move_column_to_index")
            .unwrap();
        let set_size = ops.iter().position(|o| *o == "window.set_size").unwrap();
        let focus = ops.iter().position(|o| *o == "window.focus").unwrap();
        assert!(move_ws < move_col, "PLACE before ORDER_COLUMNS, got {ops:?}");
        assert!(move_col < set_size, "ORDER_COLUMNS before SIZE, got {ops:?}");
        assert!(set_size < focus, "SIZE before FOCUS, got {ops:?}");
        // Two windows -> two move_to_workspace, two set_size, one focus.
        assert_eq!(ops.iter().filter(|o| **o == "window.move_to_workspace").count(), 2);
        assert_eq!(ops.iter().filter(|o| **o == "window.set_size").count(), 2);
        assert_eq!(ops.iter().filter(|o| **o == "window.focus").count(), 1);
    }

    #[tokio::test]
    async fn apply_layout_toggles_floating_only_when_state_differs() {
        let (_tmp, client, recorded) = recording_fake_bridge().await;
        let saved_floating = WindowEntry {
            save_id: 1,
            app_id: Some("foot".into()),
            title: Some("a".into()),
            column_index: None,
            position_in_column: None,
            is_floating: true,
            is_focused: false,
            width: 400,
            height: 300,
            floating: Some(FloatingPosition { x: 10.0, y: 20.0 }),
        };
        let live_already_floating = {
            let mut w = mk_live(10, 1, "foot", "a");
            w.is_floating = true;
            w
        };
        let pairs: Vec<(&WindowEntry, &Window)> =
            vec![(&saved_floating, &live_already_floating)];
        let snap = Snap {
            windows: vec![saved_floating.clone()],
            ..mini_snap(None)
        };

        apply_layout(&client, &snap, &pairs, 1).await.unwrap();

        let recs = recorded.lock().await;
        let ops = op_names(&recs);
        // No toggle_floating because saved.is_floating == live.is_floating.
        assert!(
            !ops.contains(&"window.toggle_floating"),
            "should not toggle when floating state already matches: {ops:?}"
        );
        // Floating windows skip column ordering and DO get a position set.
        assert!(
            !ops.contains(&"window.move_column_to_index"),
            "floating windows shouldn't be column-ordered: {ops:?}"
        );
        assert!(
            ops.contains(&"window.set_floating_position"),
            "floating window must get position set: {ops:?}"
        );
    }

    #[tokio::test]
    async fn apply_layout_orders_columns_descending_to_first() {
        // Three windows at saved cols 0, 1, 2. We expect three
        // move_column_to_index calls, ALL with index=0, and in descending
        // saved-col order (so col 0 ends up first, then col 1 above it,
        // etc.). This is the bug-fix for the multi-column shift problem.
        let (_tmp, client, recorded) = recording_fake_bridge().await;
        let mk = |sid: u64, col: u32, title: &str| WindowEntry {
            save_id: sid,
            app_id: Some("foot".into()),
            title: Some(title.into()),
            column_index: Some(col),
            position_in_column: Some(0),
            is_floating: false,
            is_focused: false,
            width: 800,
            height: 600,
            floating: None,
        };
        let s0 = mk(1, 0, "a");
        let s1 = mk(2, 1, "b");
        let s2 = mk(3, 2, "c");
        let l0 = mk_live(10, 1, "foot", "a");
        let l1 = mk_live(20, 1, "foot", "b");
        let l2 = mk_live(30, 1, "foot", "c");
        let pairs: Vec<(&WindowEntry, &Window)> = vec![
            (&s0, &l0),
            (&s1, &l1),
            (&s2, &l2),
        ];
        let snap = Snap {
            windows: vec![s0.clone(), s1.clone(), s2.clone()],
            ..mini_snap(None)
        };

        apply_layout(&client, &snap, &pairs, 1).await.unwrap();

        let recs = recorded.lock().await;
        let column_moves: Vec<&serde_json::Value> = recs
            .iter()
            .filter(|v| v["op"] == "window.move_column_to_index")
            .collect();
        assert_eq!(column_moves.len(), 3);
        // All targeted at index 0.
        for m in &column_moves {
            assert_eq!(m["params"]["index"], 0);
        }
        // Descending order by saved col → ids in order 30, 20, 10.
        let ids: Vec<u64> = column_moves
            .iter()
            .map(|v| v["params"]["id"].as_u64().unwrap())
            .collect();
        assert_eq!(ids, vec![30, 20, 10]);
    }

    #[tokio::test]
    async fn apply_layout_dedups_same_column_to_one_move() {
        // Multi-tile column: two saved entries with column_index=0. v1
        // limitation: emit ONE column-move (the first encounter wins),
        // not two. The other tile is logged but not column-targeted.
        let (_tmp, client, recorded) = recording_fake_bridge().await;
        let mk = |sid: u64, title: &str, pos: u32| WindowEntry {
            save_id: sid,
            app_id: Some("foot".into()),
            title: Some(title.into()),
            column_index: Some(0),
            position_in_column: Some(pos),
            is_floating: false,
            is_focused: false,
            width: 800,
            height: 300,
            floating: None,
        };
        let s0 = mk(1, "top", 0);
        let s1 = mk(2, "bot", 1);
        let l0 = mk_live(10, 1, "foot", "top");
        let l1 = mk_live(20, 1, "foot", "bot");
        let pairs: Vec<(&WindowEntry, &Window)> = vec![(&s0, &l0), (&s1, &l1)];
        let snap = Snap {
            windows: vec![s0.clone(), s1.clone()],
            ..mini_snap(None)
        };

        apply_layout(&client, &snap, &pairs, 1).await.unwrap();

        let recs = recorded.lock().await;
        let column_moves = recs
            .iter()
            .filter(|v| v["op"] == "window.move_column_to_index")
            .count();
        assert_eq!(column_moves, 1, "same-column duplicates collapse to one move");
    }

    #[test]
    fn schema_floating_position_round_trips_through_match() {
        // Sanity: a saved floating window matches a live one regardless of
        // its current floating state — the load flow toggles it later.
        let mut s = mk_saved(1, "foot", "a");
        s.is_floating = true;
        s.floating = Some(FloatingPosition { x: 10.0, y: 20.0 });
        let l = mk_live(10, 1, "foot", "a"); // currently tiled
        let saved = [s];
        let live = [l];
        let pairs = match_pairs(&saved, &live);
        assert_eq!(pairs.len(), 1);
        assert!(pairs[0].0.is_floating);
        assert!(!pairs[0].1.is_floating);
    }
}
