//! Centralized XDG path helpers.

use std::path::PathBuf;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_dir_creates_directory() {
        let p = state_dir().expect("state_dir should resolve on this platform");
        assert!(p.is_dir(), "{} should be a directory", p.display());
    }
}
