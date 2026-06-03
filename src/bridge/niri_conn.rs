//! Owns the connection to niri's IPC socket. Drives the cached state in
//! `bridge::state::SharedState` and emits events to the broadcast channel.
//!
//! v0: stub. Real implementation lands as the next step.

#![allow(dead_code)]

use anyhow::Result;
use tokio::task::JoinHandle;

use super::state::SharedState;

pub async fn spawn_niri_loop(_state: SharedState) -> Result<JoinHandle<()>> {
    let handle = tokio::spawn(async move {
        // Placeholder: parks the task so the daemon stays alive for now.
        // Real loop subscribes to niri events + drives the cache.
        std::future::pending::<()>().await;
    });
    Ok(handle)
}
