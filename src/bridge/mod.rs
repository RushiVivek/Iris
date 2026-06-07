//! `iris bridge` — long-running daemon that owns the niri IPC connection
//! and serves a JSON-lines protocol over a Unix socket. All other iris
//! subcommands talk to this daemon, never to niri directly.

pub mod proto;
mod activation;
mod niri_conn;
mod pinned;
mod sampler;
mod server;
mod state;

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use directories::BaseDirs;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{info, warn};

/// CLI args for `iris bridge`. Currently empty — file-logging via
/// `--log-file` was planned but is deferred to W4.5 (along with the
/// rolling log appender + desktop notifications). Keeping the struct
/// here so adding a flag later doesn't churn the dispatch surface.
#[derive(Args, Debug)]
pub struct BridgeArgs {}

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
    //
    // The activation broker is best-effort: if Wayland connect or the
    // xdg_activation_v1 bind fails (no compositor reachable, missing global,
    // …) we log + continue without it. The spawn op then refuses with a
    // clear "broker unavailable" error, while every other op keeps working.
    let cfg = crate::config::load();
    info!(
        sample_interval_ms = cfg.focus.sample_interval_ms,
        "loaded config"
    );

    let state = state::SharedState::new();
    let broker = match activation::ActivationBroker::start() {
        Ok(b) => Some(b),
        Err(e) => {
            warn!("activation broker disabled: {e:#}");
            None
        }
    };
    let niri = niri_conn::spawn_niri_loop(state.clone(), broker.clone()).await?;

    // Wait for niri to publish at least one `windows` event so the cache
    // has something to resolve persisted pins against. 3s is generous
    // for a healthy niri; if it doesn't fire, log + skip restore (the
    // first pin op will mutate the empty set and persist).
    wait_for_cache_warm(&state).await;
    if let Err(e) = pinned::restore_on_startup(&state).await {
        warn!("pin restore failed: {e:#}");
    }

    let auto_unpin = pinned::spawn_auto_unpin(state.clone());
    let sampler = tokio::spawn(sampler::run(
        state.clone(),
        std::time::Duration::from_millis(cfg.focus.sample_interval_ms),
    ));

    let server = server::spawn(sock_path.clone(), state.clone(), broker.clone()).await?;

    // Graceful shutdown: SIGINT (Ctrl-C) or SIGTERM.
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;

    tokio::select! {
        _ = sigint.recv() => info!("SIGINT received; shutting down"),
        _ = sigterm.recv() => info!("SIGTERM received; shutting down"),
        res = niri => warn!("niri connection task exited: {res:?}"),
        res = server => warn!("server task exited: {res:?}"),
    }

    // Stop background tasks BEFORE the final persist so they can't
    // race a concurrent persist + write. abort() drops their broadcast
    // receivers; tokio's mutex inside `pinned::write_lock` then
    // serializes our final write against any persist that might have
    // already been in flight when we arrived here.
    sampler.abort();
    auto_unpin.abort();
    if let Err(e) = pinned::persist(&state).await {
        warn!("final pin persist failed: {e:#}");
    }

    // Best-effort cleanup. If we crashed earlier, the next start handles
    // a stale socket via the remove_file above.
    let _ = std::fs::remove_file(&sock_path);
    let _ = std::fs::remove_file(&pid_path);
    Ok(())
}

/// Block (up to 3s) until niri has populated the cache enough that
/// pin restore can resolve persisted entries. Specifically: wait for
/// at least one `windows` event AND `state.windows` to be non-empty,
/// then a 250ms quiet period so a multi-event replay (one per
/// workspace, etc.) settles before we walk the cache.
///
/// If we time out without seeing windows, proceed anyway — pin
/// restore will see an empty live set and drop everything, which is
/// the best we can do without holding startup hostage on a
/// misbehaving niri.
async fn wait_for_cache_warm(state: &state::SharedState) {
    use std::time::Duration;
    let mut rx = state.subscribe();
    let res = tokio::time::timeout(Duration::from_secs(3), async {
        // Phase 1: wait for the first `windows` event. The replay
        // itself is the warmth signal — even an empty session emits
        // `WindowsChanged{windows: []}`, which is a legitimate "cache
        // is now authoritative" marker. Upfront probe handles the
        // race where niri's event was processed before we subscribed.
        if state.with(|s| s.windows.is_empty()).await {
            // Empty so far — wait for a windows event to arrive.
            loop {
                match rx.recv().await {
                    Ok(ev) if ev.event == proto::topics::WINDOWS => break,
                    Ok(_) => continue,
                    Err(_) => return, // closed/lagged — bail
                }
            }
        }
        // Phase 2: 250ms quiet period — drain further events but bail
        // out of the inner select once nothing arrives for 250ms.
        loop {
            match tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
                Ok(Ok(_)) => continue, // more events still flowing
                Ok(Err(_)) => return,  // channel closed/lagged
                Err(_) => return,      // 250ms quiet — settled
            }
        }
    })
    .await;
    if res.is_err() {
        warn!(
            "niri cache did not warm in 3s; pin restore will see whatever the cache holds now"
        );
    }
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
