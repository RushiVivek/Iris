//! `iris snapshot` — tmux-resurrect for niri workspaces.
//!
//! v1 (W3): layout-only — assumes saved apps are already running, rearranges
//! them into the saved layout. W4 lands respawn + per-app hooks.

pub mod load;
pub mod save;
pub mod schema;
pub mod store;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};

use crate::client::IrisClient;

#[derive(Args, Debug)]
pub struct SnapshotArgs {
    #[command(subcommand)]
    pub command: SnapshotCmd,
}

#[derive(Subcommand, Debug)]
pub enum SnapshotCmd {
    /// Save the current workspace under NAME.
    Save {
        name: String,
        /// Workspace to save (numeric idx or name). Defaults to focused.
        #[arg(long)]
        workspace: Option<String>,
        /// Overwrite an existing snapshot with the same name.
        #[arg(long)]
        force: bool,
    },
    /// Load NAME into the current (or --workspace) workspace.
    Load {
        name: String,
        /// Workspace to load into. Defaults to the snapshot's saved index.
        #[arg(long)]
        workspace: Option<String>,
        /// Close existing windows on the destination workspace first
        /// (excluding pinned and scratchpadded windows).
        #[arg(long)]
        clear: bool,
        /// Per-spawn timeout in seconds (W4 only — accepted but unused in W3).
        #[arg(long)]
        timeout: Option<u64>,
    },
    /// List saved snapshots.
    List,
    /// Pretty-print a saved snapshot.
    Show { name: String },
    /// Delete a saved snapshot.
    Delete {
        name: String,
        /// Skip the interactive confirmation prompt.
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

pub async fn run(args: SnapshotArgs) -> Result<()> {
    match args.command {
        SnapshotCmd::Save { name, workspace, force } => {
            let client = IrisClient::connect()
                .await
                .context("connecting to iris bridge (is `iris bridge` running?)")?;
            save::run(&client, name, workspace, force).await
        }
        SnapshotCmd::Load { name, workspace, clear, timeout } => {
            let client = IrisClient::connect()
                .await
                .context("connecting to iris bridge (is `iris bridge` running?)")?;
            load::run(&client, name, workspace, clear, timeout).await
        }
        SnapshotCmd::List => list(),
        SnapshotCmd::Show { name } => show(&name),
        SnapshotCmd::Delete { name, yes } => delete(&name, yes),
    }
}

fn list() -> Result<()> {
    let names = store::list_snapshots()?;
    if names.is_empty() {
        println!("(no snapshots)");
    } else {
        for n in names {
            println!("{n}");
        }
    }
    Ok(())
}

fn show(name: &str) -> Result<()> {
    let snap = store::read_snapshot(name)?;
    print!("{}", snap.to_toml()?);
    Ok(())
}

fn delete(name: &str, yes: bool) -> Result<()> {
    if !yes && !confirm(&format!("delete snapshot {name}?"))? {
        eprintln!("aborted");
        return Ok(());
    }
    store::delete_snapshot(name)?;
    eprintln!("deleted {name}");
    Ok(())
}

fn confirm(prompt: &str) -> Result<bool> {
    use std::io::{BufRead, Write};
    eprint!("{prompt} [y/N] ");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "YES"))
}
