//! Standalone neovim windows. Captures via nvim's RPC socket: `nvim
//! --server <sock> --remote-expr 'execute("mksession! /path/to/sidecar")'`.
//! Restores via `nvim -S /path/to/sidecar`.
//!
//! REQUIREMENT: the user must launch nvim with `--listen <socket>` (or
//! the equivalent vim-config snippet) for capture to succeed. Without
//! it, no socket exists on disk to talk to and capture errors out;
//! save.rs falls back to GenericHook for that window. Documented as a
//! known limitation in the README/plan.
//!
//! Match scope: `app_id == "nvim"`. Neovide and other GUI Neovim
//! frontends report a different app_id and aren't handled here.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use tokio::process::Command;

use crate::bridge::proto::Window;
use crate::snapshot::schema::HookData;

use super::AppHook;

pub struct NeovimHook;

#[async_trait]
impl AppHook for NeovimHook {
    fn name(&self) -> &'static str {
        "neovim"
    }

    fn matches(&self, app_id: Option<&str>) -> bool {
        app_id == Some("nvim")
    }

    async fn capture(&self, w: &Window) -> Result<HookData> {
        let pid = w
            .pid
            .ok_or_else(|| anyhow!("nvim window has no pid; can't probe RPC socket"))?;
        let socket = find_nvim_socket(pid)
            .await
            .with_context(|| format!("locating nvim RPC socket for pid {pid}"))?;
        let session_path = sessions_dir()?.join(format!("nvim-{pid}-{}.vim", random_suffix()));
        // Make sure the dir exists before mksession runs — nvim won't
        // create parents and mksession would silently fail.
        if let Some(parent) = session_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        write_session(&socket, &session_path)
            .await
            .with_context(|| format!("running mksession via {}", socket.display()))?;
        Ok(HookData::Neovim {
            session_path,
            argv_fallback: vec!["nvim".into()],
        })
    }

    fn build_argv(&self, data: &HookData) -> Result<Vec<String>> {
        let HookData::Neovim { session_path, argv_fallback } = data else {
            anyhow::bail!("NeovimHook can't build argv from non-Neovim variant");
        };
        if session_path.as_os_str().is_empty() {
            if argv_fallback.is_empty() {
                anyhow::bail!("Neovim HookData has no session_path AND no argv_fallback");
            }
            return Ok(argv_fallback.clone());
        }
        // `nvim -S <session>` opens the session. We don't re-add
        // `--listen` here — the user can configure that in their vim
        // config if they want the loaded nvim to be capturable on the
        // NEXT save (i.e. `vim.opt.serverlist` or autocmd-driven).
        Ok(vec![
            "nvim".into(),
            "-S".into(),
            session_path.to_string_lossy().into_owned(),
        ])
    }
}

/// Locate an nvim RPC socket owned by `pid`. nvim's default socket lives
/// under `$XDG_RUNTIME_DIR/nvim.<pid>.<n>`. Older or differently-configured
/// setups may use other paths; we try the canonical location first and
/// fall back to scanning the runtime dir for any nvim.* file.
async fn find_nvim_socket(pid: i32) -> Result<PathBuf> {
    let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")
        .ok_or_else(|| anyhow!("XDG_RUNTIME_DIR not set"))?;
    let runtime_dir = PathBuf::from(runtime_dir);

    // Canonical: nvim.<pid>.0
    let canonical = runtime_dir.join(format!("nvim.{pid}.0"));
    if tokio::fs::metadata(&canonical).await.is_ok() {
        return Ok(canonical);
    }

    // Fall back: any nvim.<pid>.* match.
    let mut entries = tokio::fs::read_dir(&runtime_dir)
        .await
        .with_context(|| format!("reading {}", runtime_dir.display()))?;
    let prefix = format!("nvim.{pid}.");
    while let Some(entry) = entries.next_entry().await? {
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with(&prefix) {
                return Ok(entry.path());
            }
        }
    }

    anyhow::bail!(
        "no nvim socket found for pid {pid} in {}; is nvim launched with `--listen`?",
        runtime_dir.display()
    )
}

async fn write_session(socket: &Path, session_path: &Path) -> Result<()> {
    let session_str = session_path
        .to_str()
        .ok_or_else(|| anyhow!("session path is not valid UTF-8: {}", session_path.display()))?;
    // mksession! needs single-quoting safe for vim. Vim's literal
    // string syntax: paired single-quotes with internal '' escape.
    let escaped = session_str.replace('\'', "''");
    let expr = format!("execute('mksession! {}')", escaped);
    let status = Command::new("nvim")
        .arg("--server")
        .arg(socket)
        .arg("--remote-expr")
        .arg(&expr)
        .status()
        .await
        .with_context(|| "spawning nvim for --remote-expr")?;
    if !status.success() {
        anyhow::bail!("nvim --remote-expr exited with {status}");
    }
    // mksession is best-effort; a window with no buffers will write an
    // empty session, which is fine. We could verify the file exists
    // after the call but loading-time errors will surface clearly via
    // `nvim -S`.
    Ok(())
}

fn sessions_dir() -> Result<PathBuf> {
    let dirs = directories::ProjectDirs::from("", "", "iris")
        .ok_or_else(|| anyhow!("could not resolve XDG data dir for iris"))?;
    Ok(dirs.data_dir().join("sessions"))
}

/// Process-local monotonic counter to disambiguate sidecars when two
/// nvim captures happen back-to-back. Genuinely monotonic (unlike a
/// `subsec_nanos`-based suffix which resets each second), and free of
/// time-of-day weirdness like wall-clock jumps.
fn random_suffix() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{n:09}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_only_nvim() {
        assert!(NeovimHook.matches(Some("nvim")));
        assert!(!NeovimHook.matches(Some("Neovide")));
        assert!(!NeovimHook.matches(Some("vim")));
        assert!(!NeovimHook.matches(None));
    }

    #[test]
    fn build_argv_emits_nvim_dash_s() {
        let data = HookData::Neovim {
            session_path: PathBuf::from("/var/lib/iris/sessions/foo.vim"),
            argv_fallback: vec!["nvim".into()],
        };
        let argv = NeovimHook.build_argv(&data).unwrap();
        assert_eq!(
            argv,
            vec![
                "nvim".to_string(),
                "-S".to_string(),
                "/var/lib/iris/sessions/foo.vim".to_string(),
            ]
        );
    }

    #[test]
    fn build_argv_empty_session_path_falls_back() {
        let data = HookData::Neovim {
            session_path: PathBuf::new(),
            argv_fallback: vec!["nvim".into()],
        };
        let argv = NeovimHook.build_argv(&data).unwrap();
        assert_eq!(argv, vec!["nvim".to_string()]);
    }

    #[test]
    fn build_argv_empty_session_path_no_fallback_errors() {
        let data = HookData::Neovim {
            session_path: PathBuf::new(),
            argv_fallback: vec![],
        };
        assert!(NeovimHook.build_argv(&data).is_err());
    }

    #[test]
    fn build_argv_wrong_variant_errors() {
        let data = HookData::Browser {
            app_id: "firefox".into(),
            argv_fallback: vec!["firefox".into()],
        };
        assert!(NeovimHook.build_argv(&data).is_err());
    }
}
