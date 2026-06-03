use anyhow::{Result, bail};
use clap::{Args, Subcommand};

#[derive(Args, Debug)]
pub struct ScratchpadArgs {
    #[command(subcommand)]
    pub command: ScratchpadCmd,
}

#[derive(Subcommand, Debug)]
pub enum ScratchpadCmd {
    /// Add the focused window to the scratchpad peek strip.
    Add,
    /// Remove the focused (or named) window from the scratchpad.
    Remove,
    /// List scratchpadded windows + their slot index.
    List,
    /// Expand the focused scratchpad window (usually triggered by clicking it).
    PeekToggle,
    /// Focus next (or --reverse, prev) scratchpadded window.
    Cycle {
        #[arg(long)]
        reverse: bool,
    },
}

pub async fn run(_args: ScratchpadArgs) -> Result<()> {
    bail!("`iris scratchpad` not implemented yet (planned for weekend 6)");
}
