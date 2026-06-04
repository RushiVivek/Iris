//! Last-resort hook: matches anything. Captures `/proc/<pid>/cmdline`
//! + `/proc/<pid>/cwd`, replays argv on spawn (wrapped in `sh -lc 'cd
//! <cwd> && exec ...'` if cwd is known).

#![allow(dead_code)]

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;

use crate::bridge::proto::Window;
use crate::snapshot::schema::HookData;

use super::AppHook;

pub struct GenericHook;

#[async_trait]
impl AppHook for GenericHook {
    fn name(&self) -> &'static str {
        "generic"
    }

    /// Catch-all: handles any app_id (including `None`).
    fn matches(&self, _app_id: Option<&str>) -> bool {
        true
    }

    async fn capture(&self, w: &Window) -> Result<HookData> {
        let pid = w
            .pid
            .ok_or_else(|| anyhow!("window has no pid; can't capture argv"))?;
        let argv = read_cmdline(pid).await?;
        // cwd is best-effort — losing it is fine, GenericHook just
        // spawns without `cd` in that case.
        let cwd = read_cwd(pid).await.ok();
        Ok(HookData::Generic { argv, cwd })
    }

    fn build_argv(&self, data: &HookData) -> Result<Vec<String>> {
        match data {
            HookData::Generic { argv, cwd } => {
                if argv.is_empty() {
                    anyhow::bail!("HookData::Generic has empty argv; nothing to spawn");
                }
                Ok(match cwd {
                    Some(cwd) => {
                        // `sh -lc 'cd <quoted_cwd> && exec <quoted argv>'`.
                        // Use `-l` so the spawned shell sources the user's
                        // login profile (PATH etc.) — tokio::process inherits
                        // bridge's env which is already a user-session env,
                        // but a hand-edited GenericHook entry might reference
                        // something only the login shell sets up.
                        let script = format!(
                            "cd {} && exec {}",
                            shell_quote(cwd),
                            argv.iter().map(|s| shell_quote(s)).collect::<Vec<_>>().join(" ")
                        );
                        vec!["sh".into(), "-lc".into(), script]
                    }
                    None => argv.clone(),
                })
            }
            other => anyhow::bail!(
                "GenericHook can't build argv from {:?} variant",
                std::mem::discriminant(other)
            ),
        }
    }
}

/// Read `/proc/<pid>/cmdline` (NUL-separated argv). Empty argv → fail.
async fn read_cmdline(pid: i32) -> Result<Vec<String>> {
    let path = format!("/proc/{pid}/cmdline");
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("reading {path}"))?;
    // Split on NUL, drop trailing empty (cmdline is NUL-terminated).
    let argv: Vec<String> = bytes
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    if argv.is_empty() {
        anyhow::bail!("/proc/{pid}/cmdline is empty (kernel thread or zombie?)");
    }
    Ok(argv)
}

/// Read `/proc/<pid>/cwd` (a symlink). Returns the resolved path as a
/// String; we keep it as String through the schema since TOML doesn't
/// have a Path type.
async fn read_cwd(pid: i32) -> Result<String> {
    let path = format!("/proc/{pid}/cwd");
    let target = tokio::fs::read_link(&path)
        .await
        .with_context(|| format!("reading symlink {path}"))?;
    Ok(target.to_string_lossy().into_owned())
}

/// Single-quote a string for safe inclusion in `sh -c` script. Embedded
/// single-quotes get escaped via the standard `'\''` trick.
fn shell_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str(r"'\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_anything() {
        assert!(GenericHook.matches(None));
        assert!(GenericHook.matches(Some("foot")));
        assert!(GenericHook.matches(Some("anything-else")));
    }

    #[test]
    fn build_argv_no_cwd_passes_argv_verbatim() {
        let data = HookData::Generic {
            argv: vec!["echo".into(), "hello".into()],
            cwd: None,
        };
        let argv = GenericHook.build_argv(&data).unwrap();
        assert_eq!(argv, vec!["echo".to_string(), "hello".to_string()]);
    }

    #[test]
    fn build_argv_with_cwd_wraps_in_sh_lc() {
        let data = HookData::Generic {
            argv: vec!["echo".into(), "hi".into()],
            cwd: Some("/home/rushi".into()),
        };
        let argv = GenericHook.build_argv(&data).unwrap();
        assert_eq!(argv[0], "sh");
        assert_eq!(argv[1], "-lc");
        assert!(argv[2].contains("cd '/home/rushi'"));
        assert!(argv[2].contains("exec 'echo' 'hi'"));
    }

    #[test]
    fn build_argv_empty_argv_errors() {
        let data = HookData::Generic { argv: vec![], cwd: None };
        let err = GenericHook.build_argv(&data).unwrap_err();
        assert!(format!("{err:#}").contains("empty"));
    }

    #[test]
    fn build_argv_wrong_variant_errors() {
        // GenericHook can only build from HookData::Generic.
        let data = HookData::Browser {
            app_id: "firefox".into(),
            argv_fallback: vec!["firefox".into()],
        };
        assert!(GenericHook.build_argv(&data).is_err());
    }

    #[test]
    fn shell_quote_escapes_embedded_quotes() {
        assert_eq!(shell_quote("simple"), "'simple'");
        assert_eq!(shell_quote("/tmp/a b"), "'/tmp/a b'");
        // The classic ' escape: 'foo'\''bar'.
        assert_eq!(shell_quote("foo'bar"), r"'foo'\''bar'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn shell_quote_handles_paths_with_special_chars() {
        // Real cwd values can have $, !, &, ;, etc. — single-quoting
        // makes them all literal.
        assert_eq!(shell_quote("/tmp/$HOME"), "'/tmp/$HOME'");
        assert_eq!(shell_quote("a;b&c"), "'a;b&c'");
    }
}
