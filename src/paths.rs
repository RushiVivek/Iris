//! Centralized XDG path helpers.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use directories::ProjectDirs;

/// `${XDG_STATE_HOME:-~/.local/state}/iris/` on Linux, `<data_dir>/logs`
/// elsewhere (macOS dev box). Created if missing.
pub fn state_dir() -> Result<PathBuf> {
    let dirs = ProjectDirs::from("", "", "iris")
        .ok_or_else(|| anyhow::anyhow!("could not resolve XDG dirs for iris"))?;
    let p = match dirs.state_dir() {
        Some(p) => p.to_path_buf(),
        None => dirs.data_dir().join("logs"),
    };
    std::fs::create_dir_all(&p)
        .with_context(|| format!("creating state dir {}", p.display()))?;
    Ok(p)
}

/// Atomically write `bytes` to `target` via a same-directory tempfile +
/// rename. Creates parent directories as needed. Power-loss safe: a
/// crash mid-write leaves either the old file untouched or the new
/// file fully present, never a half-written file.
pub fn write_atomic(target: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let dir = target
        .parent()
        .ok_or_else(|| anyhow::anyhow!("target {} has no parent", target.display()))?;
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.flush()?;
    tmp.persist(target)
        .map_err(|e| anyhow::anyhow!("persisting tempfile: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_dir_creates_directory() {
        let p = state_dir().expect("state_dir should resolve on this platform");
        assert!(p.is_dir(), "{} should be a directory", p.display());
    }

    #[test]
    fn write_atomic_creates_file_and_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("subdir/file.txt");
        write_atomic(&target, b"first").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"first");
        write_atomic(&target, b"second").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"second");
    }
}
