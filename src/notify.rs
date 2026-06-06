//! Desktop toasts via the FDO notification spec.
//!
//! On Linux: D-Bus call to `org.freedesktop.Notifications` (whatever
//! daemon the user runs — mako, swaync, dankMaterialShell, etc.).
//! On macOS: NSUserNotificationCenter via `notify-rust` — used in the
//! dev box only; production target is Linux.
//!
//! Failures (no daemon, D-Bus error) are logged at warn and swallowed —
//! a missing notification path must never break the calling command.

use std::io::IsTerminal;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use notify_rust::Notification;
#[cfg(all(unix, not(target_os = "macos")))]
use notify_rust::Urgency;
use tracing::warn;

/// Cap every D-Bus round-trip so a wedged notification daemon can't
/// stall the calling command. The daemon owns toast lifetime after
/// `Notify` is delivered; we just need the request to land or fail fast.
const NOTIFY_TIMEOUT: Duration = Duration::from_secs(2);

/// Auto-detect via stderr TTY, force on, or force off. Set in `main()`
/// before dispatch via `--toast` / `--no-toast`. Tests use `Never` so
/// we never touch D-Bus from `cargo test`.
///
/// Backed by `AtomicU8` (not `OnceLock`) so tests can flip modes between
/// cases without the first-write-wins footgun.
#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub enum EmitMode {
    Auto = 0,
    Force = 1,
    Never = 2,
}

// Production default is Auto — gated by stderr TTY in `should_emit`.
// Tests default to Never so a future test that hits a notify path
// can't accidentally emit a real D-Bus call (cargo captures stderr,
// so Auto would mistakenly evaluate to "emit").
#[cfg(not(test))]
static EMIT_MODE: AtomicU8 = AtomicU8::new(EmitMode::Auto as u8);
#[cfg(test)]
static EMIT_MODE: AtomicU8 = AtomicU8::new(EmitMode::Never as u8);

pub fn set_mode(m: EmitMode) {
    EMIT_MODE.store(m as u8, Ordering::Relaxed);
}

fn should_emit() -> bool {
    let raw = EMIT_MODE.load(Ordering::Relaxed);
    match raw {
        x if x == EmitMode::Force as u8 => true,
        x if x == EmitMode::Auto as u8 => !std::io::stderr().is_terminal(),
        // Never, or any unrecognized future variant: do not emit. The
        // safe default for an unknown mode is silence, not D-Bus traffic.
        _ => false,
    }
}

/// Normal-urgency toast for successful state changes.
pub async fn info(summary: &str, body: &str) {
    if !should_emit() {
        return;
    }
    let mut n = Notification::new();
    n.appname("iris").summary(summary).body(body);
    #[cfg(all(unix, not(target_os = "macos")))]
    n.urgency(Urgency::Normal);
    deliver("info", n).await;
}

/// Critical toast for errors. `expire-timeout = 0` keeps it on screen
/// until the user dismisses it (libnotify spec). Urgency is XDG-only;
/// on macOS the platform handles persistence its own way.
pub async fn error(summary: &str, body: &str) {
    if !should_emit() {
        return;
    }
    let mut n = Notification::new();
    n.appname("iris").summary(summary).body(body);
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        n.urgency(Urgency::Critical).timeout(0);
    }
    deliver("error", n).await;
}

/// Run `show()` under `NOTIFY_TIMEOUT` and translate every failure mode
/// (timeout, daemon error, no daemon) into a single warn-and-continue.
/// On macOS `show()` is synchronous and the timeout can't actually
/// preempt it — but in practice NSUserNotificationCenter returns
/// immediately, and the timeout still guards against future regressions
/// if the platform code ever blocks.
async fn deliver(kind: &'static str, n: Notification) {
    match tokio::time::timeout(NOTIFY_TIMEOUT, show(&n)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("notify::{kind} failed: {e}"),
        Err(_) => warn!("notify::{kind} timed out after {:?}", NOTIFY_TIMEOUT),
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
async fn show(n: &Notification) -> notify_rust::error::Result<()> {
    n.show_async().await.map(|_| ())
}

#[cfg(target_os = "macos")]
async fn show(n: &Notification) -> notify_rust::error::Result<()> {
    n.show().map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn never_mode_short_circuits() {
        set_mode(EmitMode::Never);
        // Both should return without panicking and without attempting
        // a D-Bus call (proven by the test not hanging or erroring).
        info("test", "body").await;
        error("test", "body").await;
    }

    #[test]
    fn anyhow_chain_format_matches_expected() {
        let e = anyhow::anyhow!("a").context("b").context("c");
        assert_eq!(format!("{e:#}"), "c: b: a");
    }
}
