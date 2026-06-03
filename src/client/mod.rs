//! Shared bridge-client used by every non-bridge subcommand.
//!
//! Connects to `$XDG_RUNTIME_DIR/iris.sock`, sends JSON-lines requests,
//! reads responses correlated by `id`, multiplexes events to a stream.
//!
//! v0: types only — implementation lands in weekend 3 when snapshot needs it.

#![allow(dead_code)]

use anyhow::Result;

use crate::bridge::proto;

pub struct IrisClient {
    // Filled in weekend 3.
}

impl IrisClient {
    pub async fn connect() -> Result<Self> {
        anyhow::bail!("IrisClient::connect not implemented yet");
    }

    pub async fn request(&mut self, _op: proto::Op) -> Result<proto::Response> {
        anyhow::bail!("IrisClient::request not implemented yet");
    }
}
