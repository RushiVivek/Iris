//! Unix-socket server. Accepts client connections, runs one tokio task per
//! client, dispatches `Op`s to the appropriate handler, fans out events.
//!
//! v0: stub. Real implementation in the next step.

#![allow(dead_code)]

use std::path::PathBuf;

use anyhow::Result;
use tokio::task::JoinHandle;

use super::state::SharedState;

pub async fn spawn(_sock_path: PathBuf, _state: SharedState) -> Result<JoinHandle<()>> {
    let handle = tokio::spawn(async move {
        std::future::pending::<()>().await;
    });
    Ok(handle)
}
