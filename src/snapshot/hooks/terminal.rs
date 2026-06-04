//! Terminal emulators: foot, Alacritty, kitty, ghostty, wezterm.
//!
//! Captures `/proc/<pid>/cwd`. On respawn, emits `[<bin>, <cwd_flag>,
//! cwd]` with binary + cwd-flag determined per terminal — the flag isn't
//! standard across terminals (foot/Alacritty/ghostty use
//! `--working-directory`, kitty uses `--directory`, wezterm uses
//! `--cwd`).

#![allow(dead_code)]

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;

use crate::bridge::proto::Window;
use crate::snapshot::schema::HookData;

use super::AppHook;

pub struct TerminalHook;

/// (recognized app_id, binary on PATH, cwd flag).
///
/// Keep in lockstep with `matches()`. Table-driven so adding a terminal
/// is one row + the `matches()` arm.
///
/// Verification status:
/// - `foot`, `kitty`: flags verified against upstream docs / man pages.
/// - `Alacritty`: `--working-directory <PATH>` is documented.
/// - `com.mitchellh.ghostty`: ghostty's CLI uses `--working-directory=<PATH>`
///   (equals-syntax) primarily. Space-separated MAY work but unverified
///   against current ghostty; first ghostty user to hit a respawn issue
///   should report back so we can switch to the equals form.
/// - `org.wezfurlong.wezterm`: documented invocation is
///   `wezterm start --cwd <PATH>`. Bare `wezterm --cwd <PATH>` may not
///   parse — wezterm requires the `start` subcommand for terminal
///   sessions. Unverified; first wezterm user to hit a respawn issue
///   should report back.
const TERMINALS: &[(&str, &str, &str)] = &[
    ("foot", "foot", "--working-directory"),
    ("Alacritty", "alacritty", "--working-directory"),
    ("kitty", "kitty", "--directory"),
    ("com.mitchellh.ghostty", "ghostty", "--working-directory"),
    ("org.wezfurlong.wezterm", "wezterm", "--cwd"),
];

#[async_trait]
impl AppHook for TerminalHook {
    fn name(&self) -> &'static str {
        "terminal"
    }

    fn matches(&self, app_id: Option<&str>) -> bool {
        app_id
            .map(|id| TERMINALS.iter().any(|(known, ..)| *known == id))
            .unwrap_or(false)
    }

    async fn capture(&self, w: &Window) -> Result<HookData> {
        let app_id = w
            .app_id
            .as_deref()
            .ok_or_else(|| anyhow!("TerminalHook::capture called with app_id=None"))?;
        let pid = w
            .pid
            .ok_or_else(|| anyhow!("terminal {app_id} has no pid; can't read /proc/<pid>/cwd"))?;
        let cwd = read_cwd(pid)
            .await
            .with_context(|| format!("reading /proc/{pid}/cwd for {app_id}"))?;
        let bin = bin_for(app_id)
            .ok_or_else(|| anyhow!("no terminal binary mapping for app_id {app_id}"))?
            .to_string();
        Ok(HookData::Terminal {
            app_id: app_id.to_string(),
            cwd: Some(cwd),
            argv_fallback: vec![bin],
        })
    }

    fn build_argv(&self, data: &HookData) -> Result<Vec<String>> {
        let HookData::Terminal { app_id, cwd, argv_fallback } = data else {
            anyhow::bail!("TerminalHook can't build argv from non-Terminal variant");
        };
        let (bin, flag) = bin_and_flag_for(app_id).ok_or_else(|| {
            anyhow!("no terminal binary mapping for app_id {app_id}; HookData was hand-edited?")
        })?;
        Ok(match cwd {
            Some(cwd) => vec![bin.into(), flag.into(), cwd.clone()],
            None => {
                if argv_fallback.is_empty() {
                    anyhow::bail!("Terminal HookData has no cwd AND no argv_fallback");
                }
                argv_fallback.clone()
            }
        })
    }
}

fn bin_for(app_id: &str) -> Option<&'static str> {
    TERMINALS.iter().find(|(id, ..)| *id == app_id).map(|(_, bin, _)| *bin)
}

fn bin_and_flag_for(app_id: &str) -> Option<(&'static str, &'static str)> {
    TERMINALS
        .iter()
        .find(|(id, ..)| *id == app_id)
        .map(|(_, bin, flag)| (*bin, *flag))
}

async fn read_cwd(pid: i32) -> Result<String> {
    let path = format!("/proc/{pid}/cwd");
    let target = tokio::fs::read_link(&path)
        .await
        .with_context(|| format!("reading symlink {path}"))?;
    Ok(target.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_each_supported_terminal() {
        for (app_id, _, _) in TERMINALS {
            assert!(
                TerminalHook.matches(Some(app_id)),
                "should match {app_id}"
            );
        }
    }

    #[test]
    fn does_not_match_unknown_or_none() {
        assert!(!TerminalHook.matches(None));
        assert!(!TerminalHook.matches(Some("firefox")));
        assert!(!TerminalHook.matches(Some("nvim")));
        // Case-sensitive: niri reports `Alacritty` not `alacritty`.
        assert!(!TerminalHook.matches(Some("alacritty")));
    }

    #[test]
    fn build_argv_table_per_terminal() {
        // Emit the right argv shape for every terminal in the table.
        let cases: &[(&str, &str, &str)] = &[
            ("foot", "foot", "--working-directory"),
            ("Alacritty", "alacritty", "--working-directory"),
            ("kitty", "kitty", "--directory"),
            ("com.mitchellh.ghostty", "ghostty", "--working-directory"),
            ("org.wezfurlong.wezterm", "wezterm", "--cwd"),
        ];
        for (app_id, expected_bin, expected_flag) in cases {
            let data = HookData::Terminal {
                app_id: (*app_id).into(),
                cwd: Some("/home/rushi/code".into()),
                argv_fallback: vec![(*expected_bin).into()],
            };
            let argv = TerminalHook.build_argv(&data).unwrap();
            assert_eq!(
                argv,
                vec![
                    expected_bin.to_string(),
                    expected_flag.to_string(),
                    "/home/rushi/code".to_string(),
                ],
                "wrong argv for {app_id}"
            );
        }
    }

    #[test]
    fn build_argv_no_cwd_falls_back_to_argv() {
        let data = HookData::Terminal {
            app_id: "kitty".into(),
            cwd: None,
            argv_fallback: vec!["kitty".into()],
        };
        let argv = TerminalHook.build_argv(&data).unwrap();
        assert_eq!(argv, vec!["kitty".to_string()]);
    }

    #[test]
    fn build_argv_no_cwd_no_fallback_errors() {
        let data = HookData::Terminal {
            app_id: "kitty".into(),
            cwd: None,
            argv_fallback: vec![],
        };
        assert!(TerminalHook.build_argv(&data).is_err());
    }

    #[test]
    fn build_argv_unknown_app_id_errors() {
        // Hand-edited Hookdata referencing a terminal we don't know about.
        let data = HookData::Terminal {
            app_id: "weird-fork-of-foot".into(),
            cwd: Some("/tmp".into()),
            argv_fallback: vec!["weird".into()],
        };
        let err = TerminalHook.build_argv(&data).unwrap_err();
        assert!(format!("{err:#}").contains("no terminal binary mapping"));
    }

    #[test]
    fn build_argv_wrong_variant_errors() {
        let data = HookData::Browser {
            app_id: "firefox".into(),
            argv_fallback: vec!["firefox".into()],
        };
        assert!(TerminalHook.build_argv(&data).is_err());
    }
}
