//! Top-level CLI parsing. Per-subcommand args live in each module's `mod.rs`
//! and are re-exported here as the variants of `Command`.

use clap::{Parser, Subcommand};

use crate::bridge::BridgeArgs;
use crate::pin::PinArgs;
use crate::scratchpad::ScratchpadArgs;
use crate::snapshot::SnapshotArgs;
use crate::time::TimeArgs;

#[derive(Parser, Debug)]
#[command(name = "iris", version, about = "niri toolkit", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Force desktop notifications on, regardless of stderr TTY.
    #[arg(long, global = true, conflicts_with = "no_toast")]
    pub toast: bool,

    /// Force desktop notifications off, regardless of stderr TTY.
    #[arg(long = "no-toast", global = true)]
    pub no_toast: bool,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the bridge daemon (long-running; required by all other subcommands).
    Bridge(BridgeArgs),
    /// Save / load / list per-workspace named snapshots.
    Snapshot(SnapshotArgs),
    /// Pin a floating window so it follows you across workspace switches.
    Pin(PinArgs),
    /// Send a window to a peek-strip on the right edge; click to expand.
    Scratchpad(ScratchpadArgs),
    /// Per-app focus-time tracking (run `iris time watch` in the background).
    Time(TimeArgs),
}
