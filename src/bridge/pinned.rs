//! Pin set persistence + auto-unpin watcher.
//!
//! On bridge startup, the pin set is restored from
//! `${XDG_STATE_HOME}/iris/pinned.toml` via a `(app_id, title)` ladder:
//! filter live windows by exact `app_id`; one match → pin; multiple →
//! require exact title match; otherwise drop with one info log line.
//! The file is rewritten post-resolution so subsequent restarts start
//! clean. Persistence is rewritten on every pin-set mutation.
//!
//! The auto-unpin watcher subscribes to bridge's own `windows` topic
//! and drops a pinned window from the set if it transitions to
//! non-floating (per the W5 locked decision: only floating windows
//! can be pinned, so a manual toggle to tiled must release the pin).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::proto::{Window, topics};
use super::state::SharedState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedPin {
    pub app_id: String,
    pub title: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PinFile {
    pub version: u8,
    pub pinned: Vec<PersistedPin>,
}

impl Default for PinFile {
    fn default() -> Self {
        Self { version: 1, pinned: Vec::new() }
    }
}

/// Serialize all writes to `pinned.toml`. `with_mut` releases the
/// state lock before we hit the filesystem, so concurrent callers
/// (e.g. a server op + the auto-unpin watcher) could otherwise race
/// on the rename and leave the on-disk state out of sync with memory.
fn write_lock() -> &'static AsyncMutex<()> {
    static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| AsyncMutex::new(()))
}

/// Tests use this to point `pinned.toml` at a tempdir without
/// mutating `XDG_STATE_HOME` process-wide (which would race other
/// tests that read `state_dir()`).
#[cfg(test)]
static TEST_DIR_OVERRIDE: OnceLock<AsyncMutex<Option<PathBuf>>> = OnceLock::new();

#[cfg(test)]
pub async fn set_test_dir_override(path: Option<PathBuf>) {
    let m = TEST_DIR_OVERRIDE.get_or_init(|| AsyncMutex::new(None));
    *m.lock().await = path;
}

async fn pinned_path() -> Result<PathBuf> {
    #[cfg(test)]
    {
        if let Some(m) = TEST_DIR_OVERRIDE.get() {
            if let Some(p) = m.lock().await.as_ref() {
                return Ok(p.join("pinned.toml"));
            }
        }
    }
    Ok(crate::paths::state_dir()?.join("pinned.toml"))
}

pub fn read_file(path: &Path) -> Result<PinFile> {
    if !path.exists() {
        return Ok(PinFile::default());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    match toml::from_str::<PinFile>(&raw) {
        Ok(f) => Ok(f),
        Err(e) => {
            warn!("invalid pinned.toml ({e}); starting empty");
            Ok(PinFile::default())
        }
    }
}

pub fn write_file(path: &Path, file: &PinFile) -> Result<()> {
    let toml = toml::to_string_pretty(file)
        .context("serializing pin file")?;
    crate::paths::write_atomic(path, toml.as_bytes())
}

/// Resolve a `PersistedPin` to a live window id via the W5 ladder:
///   1. Filter `windows` by exact `app_id` AND `is_floating == true`.
///   2. If 1 candidate → use it.
///   3. If >1 → require exact title match (1 candidate after that → use; else drop).
///   4. If 0 → drop.
///
/// The `is_floating` filter enforces the locked decision "only floating
/// windows can be pinned" at restore time too. Otherwise a window the
/// user manually tiled between bridge sessions would silently re-pin
/// as tiled (no future transition event would retire it).
///
/// Pure function — testable with hand-built `&[Window]`.
pub fn resolve_pin(p: &PersistedPin, windows: &[Window]) -> Option<u64> {
    let by_app: Vec<&Window> = windows
        .iter()
        .filter(|w| w.is_floating && w.app_id.as_deref() == Some(p.app_id.as_str()))
        .collect();
    match by_app.len() {
        0 => None,
        1 => Some(by_app[0].id),
        _ => {
            let by_title: Vec<&Window> = by_app
                .iter()
                .copied()
                .filter(|w| w.title.as_deref() == Some(p.title.as_str()))
                .collect();
            if by_title.len() == 1 {
                Some(by_title[0].id)
            } else {
                None
            }
        }
    }
}

/// Read the pin file, resolve each entry against live windows, populate
/// `state.pinned_windows`, and rewrite the file with only the resolved
/// entries (so subsequent restarts start clean). Drops are logged once
/// each at info level.
pub async fn restore_on_startup(state: &SharedState) -> Result<()> {
    let _g = write_lock().lock().await;
    let path = pinned_path().await?;
    let file = read_file(&path)?;
    if file.pinned.is_empty() {
        return Ok(());
    }
    let live: Vec<Window> = state.with(|s| s.windows.values().cloned().collect()).await;

    let mut resolved: HashSet<u64> = HashSet::new();
    let mut kept: Vec<PersistedPin> = Vec::new();
    for p in &file.pinned {
        match resolve_pin(p, &live) {
            Some(id) => {
                resolved.insert(id);
                kept.push(p.clone());
            }
            None => {
                info!(
                    app_id = %p.app_id,
                    title = %p.title,
                    "pinned window not found at startup; dropping"
                );
            }
        }
    }
    state.with_mut(|s| s.pinned_windows = resolved).await;
    write_file(&path, &PinFile { version: 1, pinned: kept })?;
    Ok(())
}

/// Walk the in-memory pin set, resolve each id back to a `Window` for
/// the persisted (app_id, title) pair, and atomically rewrite the file.
/// Ids whose Window has vanished are silently dropped — they'd fail to
/// resolve on next startup anyway.
///
/// The write lock serializes against concurrent callers (server ops +
/// auto-unpin watcher) so the on-disk file always reflects whichever
/// memory state was most recent at the time of write_file.
pub async fn persist(state: &SharedState) -> Result<()> {
    let _g = write_lock().lock().await;
    let pinned: Vec<PersistedPin> = state
        .with(|s| {
            s.pinned_windows
                .iter()
                .filter_map(|id| s.windows.get(id))
                .map(|w| PersistedPin {
                    app_id: w.app_id.clone().unwrap_or_default(),
                    title: w.title.clone().unwrap_or_default(),
                })
                .collect()
        })
        .await;
    write_file(&pinned_path().await?, &PinFile { version: 1, pinned })
}

/// Background task: subscribe to the `windows` topic and drop any
/// pinned id whose `WindowOpenedOrChanged` payload reports
/// `is_floating: false`. Persists + toasts on each auto-unpin.
///
/// The `state.with_mut(remove)` call returns whether the id was
/// actually present, so a flurry of `WindowOpenedOrChanged` events for
/// the same id only triggers one persist + one toast.
pub fn spawn_auto_unpin(state: SharedState) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut rx = state.subscribe();
        loop {
            match rx.recv().await {
                Ok(ev) if ev.event == topics::WINDOWS => {
                    let Some(payload) = ev.data.get("opened_or_changed") else {
                        continue;
                    };
                    let Some(id) = payload.get("id").and_then(|v| v.as_u64()) else {
                        continue;
                    };
                    let is_floating = payload
                        .get("is_floating")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);
                    if is_floating {
                        continue;
                    }
                    // Re-check against live state: a rapid Mod+V cycle
                    // (tile then float) could queue a stale-tiled event
                    // that the bridge's own state has already moved past.
                    // Drop only if the current cache agrees the window
                    // is non-floating (or gone).
                    let removed = state
                        .with_mut(|s| {
                            let live_tiled = s
                                .windows
                                .get(&id)
                                .is_none_or(|w| !w.is_floating);
                            if live_tiled {
                                s.pinned_windows.remove(&id)
                            } else {
                                false
                            }
                        })
                        .await;
                    if !removed {
                        continue;
                    }
                    let app_id = payload
                        .get("app_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?")
                        .to_string();
                    if let Err(e) = persist(&state).await {
                        warn!("auto-unpin persist failed: {e:#}");
                    }
                    crate::notify::info(
                        "auto-unpinned",
                        &format!("{app_id}: window left floating mode"),
                    )
                    .await;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // We may have missed a "this pinned window went tiled"
                    // event in the lagged batch. Resync by walking the
                    // current pin set against live state.windows and
                    // dropping any id that's now non-floating (or gone).
                    warn!(lagged = n, "auto-unpin lagged; resyncing pin set against live state");
                    resync_after_lag(&state).await;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

/// Walk the current pin set against `state.windows` and drop any id
/// that's no longer floating (or has been closed). Called after a
/// broadcast `Lagged` so a missed `is_floating: false` event still
/// retires the pin.
///
/// The filter + removal happens in one `with_mut` so a fresh
/// `WindowOpenedOrChanged { is_floating: true }` for the same id
/// can't process between the snapshot and the drop, falsely
/// retiring a re-floated pin.
async fn resync_after_lag(state: &SharedState) {
    let to_drop: Vec<(u64, String)> = state
        .with_mut(|s| {
            let drop_ids: Vec<(u64, String)> = s
                .pinned_windows
                .iter()
                .filter_map(|id| match s.windows.get(id) {
                    Some(w) if w.is_floating => None, // still pinnable, keep
                    other => {
                        let app_id = other
                            .and_then(|w| w.app_id.clone())
                            .unwrap_or_else(|| "?".into());
                        Some((*id, app_id))
                    }
                })
                .collect();
            for (id, _) in &drop_ids {
                s.pinned_windows.remove(id);
            }
            drop_ids
        })
        .await;
    if to_drop.is_empty() {
        return;
    }
    if let Err(e) = persist(state).await {
        warn!("auto-unpin (lag-resync) persist failed: {e:#}");
    }
    for (_, app_id) in to_drop {
        crate::notify::info(
            "auto-unpinned",
            &format!("{app_id}: window left floating mode"),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::proto::Window;

    fn mk_window(id: u64, app_id: &str, title: &str) -> Window {
        Window {
            id,
            app_id: Some(app_id.into()),
            title: Some(title.into()),
            pid: None,
            workspace_id: Some(1),
            is_focused: false,
            is_floating: true,
            column_index: None,
            position_in_column: None,
            width: 800,
            height: 600,
            floating_position: None,
        }
    }

    #[test]
    fn resolve_pin_unique_app_id() {
        let live = vec![mk_window(1, "kitty", "shell")];
        let p = PersistedPin { app_id: "kitty".into(), title: "stale".into() };
        assert_eq!(resolve_pin(&p, &live), Some(1));
    }

    #[test]
    fn resolve_pin_multi_match_resolved_by_title() {
        let live = vec![
            mk_window(1, "kitty", "/home/rushi"),
            mk_window(2, "kitty", "/home/rushi/code"),
        ];
        let p = PersistedPin { app_id: "kitty".into(), title: "/home/rushi/code".into() };
        assert_eq!(resolve_pin(&p, &live), Some(2));
    }

    #[test]
    fn resolve_pin_multi_match_no_title_drops() {
        let live = vec![
            mk_window(1, "kitty", "shell"),
            mk_window(2, "kitty", "shell"),
        ];
        let p = PersistedPin { app_id: "kitty".into(), title: "shell".into() };
        // 2 app_id candidates → require title; 2 title candidates → drop.
        assert_eq!(resolve_pin(&p, &live), None);
    }

    #[test]
    fn resolve_pin_no_app_id_match_drops() {
        let live = vec![mk_window(1, "kitty", "shell")];
        let p = PersistedPin { app_id: "firefox".into(), title: "x".into() };
        assert_eq!(resolve_pin(&p, &live), None);
    }

    #[test]
    fn resolve_pin_drops_tiled_match() {
        // Locked decision: only floating windows can be pinned. If the
        // user tiled the previously-pinned window between bridge sessions,
        // restore must drop the entry rather than re-pinning it as tiled.
        let mut tiled = mk_window(1, "kitty", "shell");
        tiled.is_floating = false;
        let live = vec![tiled];
        let p = PersistedPin { app_id: "kitty".into(), title: "shell".into() };
        assert_eq!(resolve_pin(&p, &live), None);
    }

    #[test]
    fn pin_file_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pinned.toml");
        let original = PinFile {
            version: 1,
            pinned: vec![
                PersistedPin { app_id: "kitty".into(), title: "shell".into() },
                PersistedPin { app_id: "firefox".into(), title: "GH".into() },
            ],
        };
        write_file(&path, &original).unwrap();
        let read = read_file(&path).unwrap();
        assert_eq!(read.version, 1);
        assert_eq!(read.pinned, original.pinned);
    }

    #[test]
    fn read_file_missing_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.toml");
        let f = read_file(&path).unwrap();
        assert!(f.pinned.is_empty());
    }

    #[test]
    fn read_file_invalid_toml_returns_default_and_does_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        // Valid UTF-8 but malformed TOML — read_to_string succeeds,
        // toml::from_str fails, function falls back to default.
        std::fs::write(&path, "this is not valid toml = = =").unwrap();
        let f = read_file(&path).unwrap();
        assert!(f.pinned.is_empty());
    }
}
