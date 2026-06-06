//! `iris snapshot` — tmux-resurrect for niri workspaces.
//!
//! v1 (W3): layout-only — assumes saved apps are already running, rearranges
//! them into the saved layout. W4 lands respawn + per-app hooks.

pub mod hooks;
pub mod load;
pub mod matcher;
pub mod save;
pub mod schema;
pub mod store;
pub mod template;

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
        /// Skip respawning; just rearrange already-running windows
        /// (W3 behavior). Default is to respawn each saved entry via
        /// its hook and correlate via activation token.
        #[arg(long)]
        no_respawn: bool,
        /// Per-spawn timeout in seconds (default 5). Only meaningful
        /// without `--no-respawn`.
        #[arg(long)]
        timeout: Option<u64>,
        /// Bind a template variable: `--var KEY=VALUE` (repeatable).
        /// Substituted into the snapshot TOML before parsing —
        /// `{{KEY}}` placeholders, `{{KEY:default}}` falls back when
        /// not provided.
        #[arg(long = "var", value_parser = parse_var, action = clap::ArgAction::Append)]
        vars: Vec<(String, String)>,
    },
    /// List saved snapshots.
    List,
    /// Pretty-print a saved snapshot.
    Show { name: String },
    /// Open a saved snapshot in `$EDITOR` for manual customization.
    /// After save+exit, the file is re-validated against the schema —
    /// bad edits surface here, not at next load.
    Edit { name: String },
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
        SnapshotCmd::Load { name, workspace, clear, no_respawn, timeout, vars } => {
            let client = IrisClient::connect()
                .await
                .context("connecting to iris bridge (is `iris bridge` running?)")?;
            let vars_map: std::collections::HashMap<String, String> = vars.into_iter().collect();
            load::run(&client, name, workspace, clear, no_respawn, timeout, vars_map).await
        }
        SnapshotCmd::List => list(),
        SnapshotCmd::Show { name } => show(&name),
        SnapshotCmd::Edit { name } => edit(&name),
        SnapshotCmd::Delete { name, yes } => delete(&name, yes).await,
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

/// Open the snapshot's TOML file in `$EDITOR` for manual editing. Editor
/// resolution: `$EDITOR` → `$VISUAL` → first of `nvim`/`vim`/`vi`/`nano`
/// found on PATH. After the editor exits successfully, re-parse the
/// file via `Snapshot::from_path` to surface schema mistakes
/// immediately rather than at next load.
fn edit(name: &str) -> Result<()> {
    let path = store::snapshot_path(name)?;
    if !path.exists() {
        anyhow::bail!("snapshot {name} does not exist at {}", path.display());
    }
    let editor = resolve_editor()?;
    let status = std::process::Command::new(&editor)
        .arg(&path)
        .status()
        .with_context(|| format!("running editor {editor}"))?;
    if !status.success() {
        anyhow::bail!("editor {editor} exited with {status}");
    }
    // Validate the post-edit file. A corrupted snapshot caught here is
    // friendlier than at `iris snapshot load NAME` time.
    schema::Snapshot::from_path(&path)
        .with_context(|| format!("validating snapshot {name} after edit"))?;
    Ok(())
}

/// Pick an editor binary. `$EDITOR` wins, then `$VISUAL`, then the
/// first of `nvim`/`vim`/`vi`/`nano` that's on PATH. Returns the
/// program name (for `Command::new`) — caller's responsibility to
/// pass `path` as an arg.
fn resolve_editor() -> Result<String> {
    if let Ok(e) = std::env::var("EDITOR") {
        if !e.is_empty() {
            return Ok(e);
        }
    }
    if let Ok(v) = std::env::var("VISUAL") {
        if !v.is_empty() {
            return Ok(v);
        }
    }
    for candidate in ["nvim", "vim", "vi", "nano"] {
        if which_on_path(candidate) {
            return Ok(candidate.to_string());
        }
    }
    anyhow::bail!(
        "no editor found: set $EDITOR or install one of nvim/vim/vi/nano on PATH"
    )
}

/// Cheap PATH lookup. Avoids pulling in the `which` crate for one
/// usage. Walks `$PATH` colon-separated entries and checks if `<entry>/<bin>`
/// exists and is a file.
fn which_on_path(bin: &str) -> bool {
    let Some(path_var) = std::env::var_os("PATH") else {
        return false;
    };
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return true;
        }
    }
    false
}

async fn delete(name: &str, yes: bool) -> Result<()> {
    if !yes && !confirm(&format!("delete snapshot {name}?"))? {
        eprintln!("aborted");
        return Ok(());
    }
    store::delete_snapshot(name)?;
    eprintln!("deleted {name}");
    crate::notify::info("snapshot deleted", name).await;
    Ok(())
}

/// `--var KEY=VALUE` parser for clap. Splits on the first `=` so values
/// containing `=` (e.g. `--var query=a=b`) round-trip correctly.
/// Empty value is allowed (`--var foo=` → `foo` substitutes to empty);
/// missing `=` errors. Keys must match `\w+` (ASCII alphanumeric +
/// underscore) to align with the template regex — without this guard,
/// a `--var some-key=foo` would silently do nothing because
/// `{{some-key}}` doesn't match `\w+` in the substitution regex.
fn parse_var(s: &str) -> std::result::Result<(String, String), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("--var value {s:?} must be KEY=VALUE"))?;
    if k.is_empty() {
        return Err(format!("--var value {s:?} has empty key"));
    }
    if !k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "--var key {k:?} must match [A-Za-z0-9_]+ (template placeholders \
             only recognize \\w+ keys; hyphens or dots silently fail to substitute)"
        ));
    }
    Ok((k.to_string(), v.to_string()))
}

fn confirm(prompt: &str) -> Result<bool> {
    use std::io::{BufRead, Write};
    eprint!("{prompt} [y/N] ");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "YES"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_var_basic_keyvalue() {
        assert_eq!(
            parse_var("wrkdir=/home/rushi").unwrap(),
            ("wrkdir".to_string(), "/home/rushi".to_string())
        );
    }

    #[test]
    fn parse_var_empty_value_allowed() {
        assert_eq!(
            parse_var("foo=").unwrap(),
            ("foo".to_string(), "".to_string())
        );
    }

    #[test]
    fn parse_var_value_with_equals_split_on_first() {
        // `--var query=a=b` → key "query", value "a=b". Useful for
        // values that genuinely contain `=` (URLs, search queries).
        assert_eq!(
            parse_var("query=a=b").unwrap(),
            ("query".to_string(), "a=b".to_string())
        );
    }

    #[test]
    fn parse_var_no_equals_errors() {
        let err = parse_var("nokey").unwrap_err();
        assert!(err.contains("KEY=VALUE"));
    }

    #[test]
    fn parse_var_empty_key_errors() {
        let err = parse_var("=value").unwrap_err();
        assert!(err.contains("empty key"));
    }

    #[test]
    fn parse_var_rejects_hyphenated_key() {
        // Without this guard, `--var some-key=value` would silently
        // no-op: the template regex only recognizes \w+ placeholders,
        // so {{some-key}} in the TOML would never substitute.
        let err = parse_var("some-key=value").unwrap_err();
        assert!(err.contains("[A-Za-z0-9_]"));
    }

    #[test]
    fn parse_var_rejects_dotted_key() {
        let err = parse_var("a.b=value").unwrap_err();
        assert!(err.contains("[A-Za-z0-9_]"));
    }

    #[test]
    fn parse_var_accepts_underscore_and_digits() {
        assert_eq!(
            parse_var("wrk_dir2=/x").unwrap(),
            ("wrk_dir2".to_string(), "/x".to_string())
        );
    }
}
