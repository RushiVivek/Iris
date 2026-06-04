//! Per-app hooks: capture state at save, build spawn argv at load.
//!
//! Each hook handles one or more `app_id`s — there's no plugin loader
//! (locked decision). To customize spawn behavior beyond what a hook
//! captures, edit the saved snapshot's `[windows.hook]` table directly
//! (`iris snapshot edit NAME`); the schema's `HookData::Generic` variant
//! accepts any argv.
//!
//! Dispatch order is registration order: first hook whose `matches()`
//! returns true wins. `GenericHook` is registered last and matches
//! everything, so it's the always-fallback.
//!
//! ## Capture failure model
//!
//! If a specific hook's `capture()` errors (e.g. TerminalHook hitting
//! `EACCES` on `/proc/<pid>/cwd` for a sudo'd process), the SAVE caller
//! catches the error, logs a warning, and falls back to GenericHook's
//! capture. The snapshot still saves; that window just respawns
//! without per-app state restoration. This is enforced in `save.rs`,
//! not here — hooks themselves only know how to succeed-or-fail.

#![allow(dead_code)]

use anyhow::Result;
use async_trait::async_trait;

use crate::bridge::proto::Window;
use crate::snapshot::schema::HookData;

mod browser;
mod generic;
mod neovim;
mod terminal;
mod vscode;

pub use browser::BrowserHook;
pub use generic::GenericHook;
pub use neovim::NeovimHook;
pub use terminal::TerminalHook;
pub use vscode::VsCodeHook;

#[async_trait]
pub trait AppHook: Send + Sync {
    /// Short name for logs / diagnostics. Not user-visible.
    fn name(&self) -> &'static str;

    /// Should this hook handle a window with the given `app_id`?
    /// `None` arrives for windows niri couldn't determine an app_id for
    /// (rare but possible — typically GenericHook handles them).
    fn matches(&self, app_id: Option<&str>) -> bool;

    /// Capture state at save time. May read `/proc/<pid>/...`, may run
    /// helper subprocesses (e.g. NeovimHook talking to nvim's RPC
    /// socket). Errors propagate up to `save.rs` which downgrades to
    /// GenericHook for that one window.
    async fn capture(&self, w: &Window) -> Result<HookData>;

    /// Build the argv `iris snapshot load` will pass to `Op::Spawn`.
    /// Pure function: no IO, no async. The HookData was previously
    /// captured (or hand-edited via `iris snapshot edit`) and is the
    /// sole input.
    fn build_argv(&self, data: &HookData) -> Result<Vec<String>>;
}

/// Resolve the right hook for an app_id. First registered hook whose
/// `matches()` returns true wins. The registry is a static slice so
/// callers get back `&'static dyn AppHook` and can hold them across
/// awaits without lifetime gymnastics.
pub fn dispatch(app_id: Option<&str>) -> &'static dyn AppHook {
    for h in REGISTRY.iter() {
        if h.matches(app_id) {
            return *h;
        }
    }
    // GenericHook's `matches` is unconditional, so this is unreachable
    // in practice — but never panic in production code.
    &GenericHook
}

/// Registration order matters: first match wins. GenericHook MUST be
/// last because its `matches()` returns true for everything.
static REGISTRY: &[&'static (dyn AppHook + 'static)] = &[
    &TerminalHook,
    &VsCodeHook,
    &NeovimHook,
    &BrowserHook,
    &GenericHook,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_routes_terminals_to_terminal_hook() {
        for app_id in &[
            "foot",
            "Alacritty",
            "kitty",
            "com.mitchellh.ghostty",
            "org.wezfurlong.wezterm",
        ] {
            let h = dispatch(Some(app_id));
            assert_eq!(h.name(), "terminal", "expected terminal for {app_id}");
        }
    }

    #[test]
    fn dispatch_routes_vscode_to_vscode_hook() {
        for app_id in &["code", "Code", "code-oss", "code-insiders"] {
            let h = dispatch(Some(app_id));
            assert_eq!(h.name(), "vscode", "expected vscode for {app_id}");
        }
    }

    #[test]
    fn dispatch_routes_nvim_to_neovim_hook() {
        let h = dispatch(Some("nvim"));
        assert_eq!(h.name(), "neovim");
    }

    #[test]
    fn dispatch_routes_browsers_to_browser_hook() {
        for app_id in &["firefox", "chromium", "Brave-browser", "google-chrome"] {
            let h = dispatch(Some(app_id));
            assert_eq!(h.name(), "browser", "expected browser for {app_id}");
        }
    }

    #[test]
    fn dispatch_falls_back_to_generic() {
        // None app_id and unknown app_id both land on GenericHook.
        assert_eq!(dispatch(None).name(), "generic");
        assert_eq!(dispatch(Some("some-random-app")).name(), "generic");
    }
}
