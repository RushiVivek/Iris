use anyhow::{Result, bail};
use clap::{Args, Subcommand};

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

pub async fn run(_args: PinArgs) -> Result<()> {
    bail!("`iris pin` not implemented yet (planned for weekend 5)");
}
