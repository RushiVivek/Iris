//! `iris pin {toggle, list, off}` CLI.
//!
//! Server-side validates everything (must be floating, no-such-window,
//! etc.); the CLI just dispatches and renders. Toast feedback piggybacks
//! on the W4.5 `notify::info` path — no toast on read-only `list`.

use anyhow::{Context, Result, anyhow};
use clap::{Args, Subcommand};

use crate::bridge::proto::Op;
use crate::client::IrisClient;

#[derive(Args, Debug)]
pub struct PinArgs {
    #[command(subcommand)]
    pub command: PinCmd,
}

#[derive(Subcommand, Debug)]
pub enum PinCmd {
    /// Toggle pin on the focused window.
    Toggle,
    /// List pinned windows.
    List,
    /// Unpin all windows.
    Off,
}

pub async fn run(args: PinArgs) -> Result<()> {
    let client = IrisClient::connect()
        .await
        .context("connecting to iris bridge (is `iris bridge` running?)")?;
    match args.command {
        PinCmd::Toggle => toggle(&client).await,
        PinCmd::List => list(&client).await,
        PinCmd::Off => off(&client).await,
    }
}

async fn toggle(client: &IrisClient) -> Result<()> {
    let snap = client
        .request(Op::StateSnapshot)
        .await
        .context("requesting state.snapshot from bridge")?;
    let id = snap["focused_window_id"]
        .as_u64()
        .ok_or_else(|| anyhow!("no focused window"))?;
    let app_id = snap["windows"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|w| w["id"].as_u64() == Some(id))
        .and_then(|w| w["app_id"].as_str())
        .unwrap_or("?")
        .to_string();
    let resp = client.request(Op::PinToggle { window_id: id }).await?;
    let pinned = resp["pinned"].as_bool().unwrap_or(false);
    let verb = if pinned { "pinned" } else { "unpinned" };
    eprintln!("{verb} {app_id} ({id})");
    crate::notify::info("pin toggled", &format!("{app_id}: {verb}")).await;
    Ok(())
}

async fn list(client: &IrisClient) -> Result<()> {
    let data = client.request(Op::PinList).await?;
    let arr = data.as_array().cloned().unwrap_or_default();
    if arr.is_empty() {
        println!("(no pinned windows)");
        return Ok(());
    }
    for w in arr {
        println!(
            "{:>5}  {:<20}  {}",
            w["id"],
            w["app_id"].as_str().unwrap_or("?"),
            w["title"].as_str().unwrap_or(""),
        );
    }
    Ok(())
}

async fn off(client: &IrisClient) -> Result<()> {
    let resp = client.request(Op::PinOff).await?;
    let n = resp["n_unpinned"].as_u64().unwrap_or(0);
    eprintln!("unpinned {n} windows");
    if n > 0 {
        crate::notify::info("all windows unpinned", &format!("{n} windows")).await;
    }
    Ok(())
}
