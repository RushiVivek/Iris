//! iris — niri toolkit. See README.md and the project plan for design rationale.

mod cli;
mod bridge;
mod client;
mod snapshot;
mod pin;
mod scratchpad;
mod time;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        Command::Bridge(args) => bridge::run(args).await,
        Command::Snapshot(args) => snapshot::run(args).await,
        Command::Pin(args) => pin::run(args).await,
        Command::Scratchpad(args) => scratchpad::run(args).await,
        Command::Time(args) => time::run(args).await,
    }
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt};
    let filter = EnvFilter::try_from_env("IRIS_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}
