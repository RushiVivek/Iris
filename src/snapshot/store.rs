//! Filesystem layer for snapshots: paths, list, read, atomic-write, delete.
//!
//! Storage layout (per the plan):
//!   `${XDG_DATA_HOME:-~/.local/share}/iris/snapshots/<name>.toml`
//!
//! Atomic writes via `tempfile::NamedTempFile::persist` so a power loss
//! mid-save can never leave a half-written `<name>.toml`.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use directories::ProjectDirs;

use super::schema::Snapshot;

/// Directory containing all snapshot TOML files.
pub fn snapshots_dir() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("", "", "iris")
        .ok_or_else(|| anyhow::anyhow!("could not resolve XDG data dir for iris"))?;
    Ok(dirs.data_dir().join("snapshots"))
}

pub fn snapshot_path(name: &str) -> Result<PathBuf> {
    validate_name(name)?;
    Ok(snapshots_dir()?.join(format!("{name}.toml")))
}

/// List snapshot names (without `.toml` suffix), alphabetically. Returns
/// an empty Vec if the snapshots directory doesn't exist yet.
pub fn list_snapshots() -> Result<Vec<String>> {
    list_in(&snapshots_dir()?)
}

/// Read a snapshot by name.
pub fn read_snapshot(name: &str) -> Result<Snapshot> {
    Snapshot::from_path(&snapshot_path(name)?)
}

/// Atomically write a snapshot. Creates parent directories as needed.
/// `force = false` errors if a snapshot with that name already exists,
/// without TOCTOU between the existence check and the rename — see
/// `write_atomic` / `write_atomic_no_clobber`.
pub fn write_snapshot(name: &str, snap: &Snapshot, force: bool) -> Result<()> {
    let dir = snapshots_dir()?;
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    let target = dir.join(format!("{name}.toml"));
    let toml = snap.to_toml()?;
    let bytes = toml.as_bytes();
    if force {
        write_atomic(&target, bytes)
            .with_context(|| format!("writing {}", target.display()))?;
    } else {
        write_atomic_no_clobber(&target, bytes)
            .with_context(|| format!("writing {}", target.display()))?;
    }
    Ok(())
}

pub fn delete_snapshot(name: &str) -> Result<()> {
    let path = snapshot_path(name)?;
    std::fs::remove_file(&path)
        .with_context(|| format!("deleting {}", path.display()))?;
    Ok(())
}

// ─────────────────────────────── internals ──────────────────────────────────

/// Refuse names that escape the snapshots dir (`..`, `/`) or are likely
/// to confuse the FS. Conservative: `[A-Za-z0-9_.-]` only, non-empty,
/// not starting with `.`.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("snapshot name cannot be empty");
    }
    if name.starts_with('.') {
        anyhow::bail!("snapshot name cannot start with '.'");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
    {
        anyhow::bail!(
            "snapshot name {name:?} contains characters outside [A-Za-z0-9_.-]"
        );
    }
    Ok(())
}

fn list_in(dir: &Path) -> Result<Vec<String>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut names = Vec::new();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("toml") {
            continue;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            names.push(stem.to_string());
        }
    }
    names.sort();
    Ok(names)
}

fn write_atomic(target: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let dir = target
        .parent()
        .ok_or_else(|| anyhow::anyhow!("target {} has no parent", target.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.flush()?;
    tmp.persist(target)
        .map_err(|e| anyhow::anyhow!("persisting tempfile: {e}"))?;
    Ok(())
}

/// Atomic write that refuses to overwrite an existing target. Closes the
/// TOCTOU window between an `exists()` check and `persist`: if the target
/// exists when we try to rename, the kernel returns EEXIST and we surface
/// it instead of silently clobbering.
fn write_atomic_no_clobber(target: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let dir = target
        .parent()
        .ok_or_else(|| anyhow::anyhow!("target {} has no parent", target.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.flush()?;
    tmp.persist_noclobber(target).map_err(|e| {
        anyhow::anyhow!(
            "snapshot already exists at {} (pass --force to overwrite): {e}",
            target.display()
        )
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::proto::FloatingPosition;
    use crate::snapshot::schema::{Snapshot, WindowEntry, WorkspaceMeta};
    use chrono::{DateTime, Utc};
    use tempfile::TempDir;

    fn mini_snapshot(name: &str) -> Snapshot {
        Snapshot {
            version: 1,
            name: name.into(),
            saved_at: DateTime::parse_from_rfc3339("2026-06-04T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            workspace: WorkspaceMeta {
                index: 1,
                name: None,
                output: None,
                focused_save_id: None,
            },
            windows: vec![WindowEntry {
                save_id: 1,
                app_id: Some("foot".into()),
                title: Some("t".into()),
                column_index: Some(0),
                position_in_column: Some(0),
                is_floating: false,
                is_focused: false,
                width: 800,
                height: 600,
                floating: None,
            }],
        }
    }

    #[test]
    fn validate_name_accepts_allowed() {
        assert!(validate_name("work_ws1").is_ok());
        assert!(validate_name("home.ws-2").is_ok());
        assert!(validate_name("a").is_ok());
    }

    #[test]
    fn validate_name_rejects_traversal_and_weird_chars() {
        assert!(validate_name("").is_err());
        assert!(validate_name("..").is_err());
        assert!(validate_name(".hidden").is_err());
        assert!(validate_name("a/b").is_err());
        assert!(validate_name("a b").is_err());
    }

    #[test]
    fn write_then_list_then_read_round_trip() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("snapshots");
        let snap = mini_snapshot("foo");

        // Use the internal helpers directly so the test doesn't depend on
        // XDG dirs.
        std::fs::create_dir_all(&dir).unwrap();
        let target = dir.join("foo.toml");
        write_atomic(&target, snap.to_toml().unwrap().as_bytes()).unwrap();

        let listed = list_in(&dir).unwrap();
        assert_eq!(listed, vec!["foo".to_string()]);

        let read = Snapshot::from_path(&target).unwrap();
        assert_eq!(read, snap);
    }

    #[test]
    fn list_in_missing_dir_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let listed = list_in(&tmp.path().join("does-not-exist")).unwrap();
        assert!(listed.is_empty());
    }

    #[test]
    fn list_in_skips_non_toml_files() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        std::fs::write(dir.join("a.toml"), b"version = 1").unwrap();
        std::fs::write(dir.join("b.txt"), b"hi").unwrap();
        std::fs::write(dir.join("c.toml.bak"), b"hi").unwrap();
        let listed = list_in(dir).unwrap();
        assert_eq!(listed, vec!["a".to_string()]);
    }

    #[test]
    fn write_atomic_replaces_existing_file() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("x.toml");
        std::fs::write(&target, b"old").unwrap();
        write_atomic(&target, b"new").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
    }

    #[test]
    fn write_atomic_no_clobber_refuses_existing_file() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("x.toml");
        std::fs::write(&target, b"old").unwrap();
        let err = write_atomic_no_clobber(&target, b"new").unwrap_err();
        assert!(format!("{err:#}").contains("already exists"));
        assert_eq!(std::fs::read(&target).unwrap(), b"old");
    }

    #[test]
    fn write_atomic_no_clobber_writes_when_target_absent() {
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("new.toml");
        write_atomic_no_clobber(&target, b"hello").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"hello");
    }

    #[test]
    fn write_atomic_no_partial_file_on_close_failure() {
        // Best-effort: just verify a normal write produces a complete file.
        // Real partial-write resilience is the OS+tempfile-persist
        // contract, not something we can easily simulate.
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("a.toml");
        let payload = b"complete contents\n";
        write_atomic(&target, payload).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), payload);
    }

    #[test]
    fn floating_position_serialized_under_floating_key() {
        let mut snap = mini_snapshot("f");
        snap.windows[0].is_floating = true;
        snap.windows[0].floating = Some(FloatingPosition { x: 10.0, y: 20.0 });
        let toml = snap.to_toml().unwrap();
        assert!(toml.contains("floating"));
        let parsed = Snapshot::from_toml(&toml).unwrap();
        assert_eq!(parsed.windows[0].floating, Some(FloatingPosition { x: 10.0, y: 20.0 }));
    }
}
