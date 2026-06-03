//! `iris bridge` — long-running daemon that owns the niri IPC connection
//! and serves a JSON-lines protocol over a Unix socket. All other iris
//! subcommands talk to this daemon, never to niri directly.

pub mod proto;
mod niri_conn;
mod server;
mod state;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use directories::BaseDirs;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{info, warn};

#[derive(Args, Debug)]
pub struct BridgeArgs {
    /// Optional log file path. Stderr is always used; this adds a second sink.
    #[arg(long)]
    pub log_file: Option<PathBuf>,
}

pub async fn run(_args: BridgeArgs) -> Result<()> {
    let runtime_dir = runtime_dir()?;
    let pid_path = runtime_dir.join("iris.pid");
    let sock_path = runtime_dir.join("iris.sock");

    // Refuse to start if another bridge is already running.
    ensure_single_instance(&pid_path)?;
    write_pid_file(&pid_path)?;

    // Stale socket file from an unclean previous shutdown — remove it so
    // bind() doesn't fail with EADDRINUSE.
    if sock_path.exists() {
        std::fs::remove_file(&sock_path).context("removing stale iris.sock")?;
    }

    info!(pid = std::process::id(), socket = %sock_path.display(), "iris bridge starting");

    // Wire up niri connection + state cache + server. Each runs as its own
    // tokio task; we wait until any of them exits or we receive SIGINT/SIGTERM.
    let state = state::SharedState::new();
    let niri = niri_conn::spawn_niri_loop(state.clone()).await?;
    let server = server::spawn(sock_path.clone(), state.clone()).await?;

    // Graceful shutdown: SIGINT (Ctrl-C) or SIGTERM.
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

    tokio::select! {
        _ = sigint.recv() => info!("SIGINT received; shutting down"),
        _ = sigterm.recv() => info!("SIGTERM received; shutting down"),
        res = niri => {
            warn!("niri connection task exited: {res:?}");
        }
        res = server => {
            warn!("server task exited: {res:?}");
        }
    }

    // Best-effort cleanup. If we crashed earlier, the next start handles
    // a stale socket via the remove_file above.
    let _ = std::fs::remove_file(&sock_path);
    let _ = std::fs::remove_file(&pid_path);
    Ok(())
}

fn runtime_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        return Ok(PathBuf::from(dir));
    }
    if let Some(dirs) = BaseDirs::new() {
        if let Some(rd) = dirs.runtime_dir() {
            return Ok(rd.to_path_buf());
        }
    }
    bail!("XDG_RUNTIME_DIR not set and no fallback available")
}

/// Refuse to start if there's a live bridge already running. Stale PID files
/// (process is gone) are silently overwritten by `write_pid_file`.
fn ensure_single_instance(pid_path: &std::path::Path) -> Result<()> {
    let Ok(contents) = std::fs::read_to_string(pid_path) else {
        return Ok(()); // No pid file ⇒ no prior instance.
    };
    let Ok(pid) = contents.trim().parse::<i32>() else {
        return Ok(()); // Garbage ⇒ overwrite.
    };
    // kill -0 — does the process exist?
    if unsafe { libc::kill(pid, 0) } == 0 {
        bail!(
            "another iris bridge is already running (pid {pid}); refusing to start"
        );
    }
    Ok(())
}

fn write_pid_file(pid_path: &std::path::Path) -> Result<()> {
    if let Some(parent) = pid_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(pid_path, std::process::id().to_string())
        .with_context(|| format!("writing PID file at {}", pid_path.display()))?;
    Ok(())
}
