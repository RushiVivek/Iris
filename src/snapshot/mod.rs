use anyhow::{Result, bail};
use clap::{Args, Subcommand};

#[derive(Args, Debug)]
pub struct SnapshotArgs {
    #[command(subcommand)]
    pub command: SnapshotCmd,
}

#[derive(Subcommand, Debug)]
pub enum SnapshotCmd {
    /// Save the current workspace under NAME.
    Save { name: String },
    /// Load NAME into the current (or --workspace) workspace.
    Load { name: String },
    /// List saved snapshots.
    List,
    /// Pretty-print a saved snapshot.
    Show { name: String },
    /// Delete a saved snapshot.
    Delete { name: String },
}

pub async fn run(_args: SnapshotArgs) -> Result<()> {
    bail!("`iris snapshot` not implemented yet (planned for weekend 3-4)");
}
