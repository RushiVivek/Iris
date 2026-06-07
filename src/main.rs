//! iris — niri toolkit. See README.md and the project plan for design rationale.

mod bridge;
mod cli;
mod client;
mod config;
mod notify;
mod paths;
mod pin;
mod scratchpad;
mod snapshot;
mod time;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, Command};
use tracing::error;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // Parse CLI BEFORE init_tracing: clap calls process::exit() on
    // --help/--version/parse error, which skips destructors. Setting
    // up the file appender first would leak its WorkerGuard and lose
    // any buffered records on a parse-failure exit.
    let cli = Cli::parse();

    // Guard for the rolling-file appender's writer thread; must outlive
    // every log call so we hold it for the whole `main`.
    let _file_guard = init_tracing();

    notify::set_mode(match (cli.toast, cli.no_toast) {
        (true, false) => notify::EmitMode::Force,
        (false, true) => notify::EmitMode::Never,
        _ => notify::EmitMode::Auto,
    });

    let result = match cli.command {
        Command::Bridge(args) => bridge::run(args).await,
        Command::Snapshot(args) => snapshot::run(args).await,
        Command::Pin(args) => pin::run(args).await,
        Command::Scratchpad(args) => scratchpad::run(args).await,
        Command::Time(args) => time::run(args).await,
    };

    if let Err(e) = &result {
        eprintln!("error: {e:#}");
        // notify::error caps the D-Bus round-trip internally so a hung
        // daemon can't delay exit — the user already saw the eprintln.
        notify::error("iris: command failed", &format!("{e:#}")).await;
        error!("command failed: {e:#}");
    }
    result
}

/// Returns a guard that must be kept alive for the lifetime of the
/// program — dropping it flushes the non-blocking writer's buffer.
/// `None` means file logging failed to initialize; stderr-only.
fn init_tracing() -> Option<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};

    let filter = EnvFilter::try_from_env("IRIS_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    let stderr_layer = fmt::layer()
        .with_target(false)
        .with_writer(std::io::stderr);

    let (file_layer, guard) = match build_file_layer() {
        Ok((layer, g)) => (Some(layer), Some(g)),
        Err(e) => {
            eprintln!("iris: file logging disabled: {e:#}");
            (None, None)
        }
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer)
        .with(file_layer)
        .init();
    guard
}

type FileLayer<S> = Box<dyn tracing_subscriber::Layer<S> + Send + Sync>;

fn build_file_layer<S>() -> anyhow::Result<(FileLayer<S>, tracing_appender::non_blocking::WorkerGuard)>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    use tracing_subscriber::Layer;

    let dir = paths::state_dir()?;
    // Pruning failure shouldn't disable logging — the appender works
    // regardless of whether old files got cleaned up. Just complain once.
    if let Err(e) = prune_old_logs(&dir, 7) {
        eprintln!("iris: log pruning failed (file logging still active): {e:#}");
    }
    let appender = tracing_appender::rolling::daily(&dir, "iris.log");
    let (nb, guard) = tracing_appender::non_blocking(appender);
    let layer = tracing_subscriber::fmt::layer()
        .with_writer(nb)
        .with_ansi(false)
        .with_target(false);
    Ok((layer.boxed(), guard))
}

/// Keep only the `keep` most-recently-modified files matching `iris.log*`
/// in `dir`. Run at startup; cheap (one readdir + sort) and version-agnostic
/// (avoids depending on tracing-appender's builder having `max_log_files`).
fn prune_old_logs(dir: &std::path::Path, keep: usize) -> anyhow::Result<()> {
    let mut entries: Vec<(std::path::PathBuf, std::time::SystemTime)> = std::fs::read_dir(dir)?
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("iris.log"))
        .filter_map(|e| Some((e.path(), e.metadata().ok()?.modified().ok()?)))
        .collect();
    // Newest mtime first; tiebreak by filename descending so equal-
    // mtime entries (filesystems with second-resolution clocks can
    // produce these on a fast roll boundary) prune deterministically.
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)));
    for (p, _) in entries.into_iter().skip(keep) {
        let _ = std::fs::remove_file(p);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::time::{Duration, SystemTime};
    use tempfile::tempdir;

    #[test]
    fn prune_old_logs_keeps_most_recent_n() {
        let dir = tempdir().unwrap();
        let now = SystemTime::now();
        // i=0 is oldest (mtime - 1000s), i=9 is newest (mtime - 100s).
        // After prune-7, files i=3..9 should remain (dates 04..10).
        for i in 0..10 {
            let path = dir.path().join(format!("iris.log.2026-06-{:02}", i + 1));
            File::create(&path).unwrap();
            let mtime = now - Duration::from_secs((10 - i) as u64 * 100);
            let f = File::open(&path).unwrap();
            f.set_modified(mtime).unwrap();
        }
        File::create(dir.path().join("unrelated.txt")).unwrap();

        prune_old_logs(dir.path(), 7).unwrap();

        let remaining: std::collections::BTreeSet<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("iris.log"))
            .collect();
        // The 7 newest are dates 04..10 (i = 3..9, names "iris.log.2026-06-04" .. "iris.log.2026-06-10").
        let expected: std::collections::BTreeSet<String> = (3..10)
            .map(|i| format!("iris.log.2026-06-{:02}", i + 1))
            .collect();
        assert_eq!(remaining, expected, "should keep exactly the 7 newest by mtime");
        // Unrelated file untouched.
        assert!(dir.path().join("unrelated.txt").exists());
    }

    #[test]
    fn prune_old_logs_keep_more_than_present_is_noop() {
        let dir = tempdir().unwrap();
        for i in 0..3 {
            File::create(dir.path().join(format!("iris.log.{i}"))).unwrap();
        }
        prune_old_logs(dir.path(), 7).unwrap();
        let count = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(count, 3);
    }
}
