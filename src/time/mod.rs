use anyhow::{Result, bail};
use clap::{Args, Subcommand};

#[derive(Args, Debug)]
pub struct TimeArgs {
    #[command(subcommand)]
    pub command: TimeCmd,
}

#[derive(Subcommand, Debug)]
pub enum TimeCmd {
    /// Today's per-app totals.
    Today {
        /// Show per-(app_id, title) rows instead of grouped by app.
        #[arg(long)]
        by_title: bool,
    },
    /// Last 7 days, grouped per app.
    Week {
        #[arg(long)]
        by_title: bool,
    },
    /// Time series for one app, broken down by title.
    App {
        app_id: String,
        #[arg(long, default_value_t = 7)]
        days: u32,
    },
    /// Dump raw rows for piping.
    Raw {
        #[arg(long)]
        since: Option<String>,
    },
    /// Long-running: subscribe to bridge events, write to SQLite.
    Watch,
}

pub async fn run(_args: TimeArgs) -> Result<()> {
    bail!("`iris time` not implemented yet (planned for weekend 7)");
}
