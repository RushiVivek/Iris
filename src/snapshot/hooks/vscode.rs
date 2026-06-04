//! VS Code (code, Code, code-oss, code-insiders, com.visualstudio.code.oss).
//!
//! Spawned with `--new-window <cwd>` so each saved entry gets its own
//! window. Without `--new-window`, a second `code <dir>` invocation
//! merges into the existing instance — no fresh `WindowOpenedOrChanged`
//! fires, so token correlation never resolves and the load times out.

#![allow(dead_code)]

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;

use crate::bridge::proto::Window;
use crate::snapshot::schema::HookData;

use super::AppHook;

pub struct VsCodeHook;

/// (recognized app_id, binary on PATH).
const VSCODES: &[(&str, &str)] = &[
    ("code", "code"),
    ("Code", "code"),
    ("code-oss", "code-oss"),
    ("code-insiders", "code-insiders"),
    ("com.visualstudio.code.oss", "code-oss"),
];

#[async_trait]
impl AppHook for VsCodeHook {
    fn name(&self) -> &'static str {
        "vscode"
    }

    fn matches(&self, app_id: Option<&str>) -> bool {
        app_id
            .map(|id| VSCODES.iter().any(|(known, _)| *known == id))
            .unwrap_or(false)
    }

    async fn capture(&self, w: &Window) -> Result<HookData> {
        let app_id = w
            .app_id
            .as_deref()
            .ok_or_else(|| anyhow!("VsCodeHook::capture called with app_id=None"))?;
        let pid = w
            .pid
            .ok_or_else(|| anyhow!("VS Code window has no pid; can't read /proc/<pid>/cwd"))?;
        let cwd = read_cwd(pid)
            .await
            .with_context(|| format!("reading /proc/{pid}/cwd for {app_id}"))?;
        let bin = bin_for(app_id)
            .ok_or_else(|| anyhow!("no VS Code binary mapping for app_id {app_id}"))?
            .to_string();
        Ok(HookData::VsCode {
            app_id: app_id.to_string(),
            cwd: Some(cwd),
            argv_fallback: vec![bin],
        })
    }

    fn build_argv(&self, data: &HookData) -> Result<Vec<String>> {
        let HookData::VsCode { app_id, cwd, argv_fallback } = data else {
            anyhow::bail!("VsCodeHook can't build argv from non-VsCode variant");
        };
        let bin = bin_for(app_id).ok_or_else(|| {
            anyhow!("no VS Code binary mapping for app_id {app_id}; HookData was hand-edited?")
        })?;
        Ok(match cwd {
            Some(cwd) => vec![bin.into(), "--new-window".into(), cwd.clone()],
            None => {
                if argv_fallback.is_empty() {
                    anyhow::bail!("VsCode HookData has no cwd AND no argv_fallback");
                }
                // Even without cwd, force --new-window so the saved
                // entry doesn't merge into an existing instance.
                let mut argv = argv_fallback.clone();
                if !argv.iter().any(|a| a == "--new-window") {
                    argv.push("--new-window".into());
                }
                argv
            }
        })
    }
}

fn bin_for(app_id: &str) -> Option<&'static str> {
    VSCODES.iter().find(|(id, _)| *id == app_id).map(|(_, bin)| *bin)
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
    fn matches_each_supported_id() {
        for (app_id, _) in VSCODES {
            assert!(VsCodeHook.matches(Some(app_id)), "should match {app_id}");
        }
    }

    #[test]
    fn does_not_match_unknown_or_none() {
        assert!(!VsCodeHook.matches(None));
        assert!(!VsCodeHook.matches(Some("nvim")));
        assert!(!VsCodeHook.matches(Some("vscode")));
    }

    #[test]
    fn build_argv_includes_new_window_when_cwd_present() {
        let data = HookData::VsCode {
            app_id: "code".into(),
            cwd: Some("/home/rushi/proj".into()),
            argv_fallback: vec!["code".into()],
        };
        let argv = VsCodeHook.build_argv(&data).unwrap();
        assert_eq!(
            argv,
            vec![
                "code".to_string(),
                "--new-window".to_string(),
                "/home/rushi/proj".to_string()
            ]
        );
    }

    #[test]
    fn build_argv_appends_new_window_to_fallback() {
        let data = HookData::VsCode {
            app_id: "code".into(),
            cwd: None,
            argv_fallback: vec!["code".into()],
        };
        let argv = VsCodeHook.build_argv(&data).unwrap();
        assert_eq!(argv, vec!["code".to_string(), "--new-window".to_string()]);
    }

    #[test]
    fn build_argv_does_not_double_add_new_window() {
        let data = HookData::VsCode {
            app_id: "code".into(),
            cwd: None,
            argv_fallback: vec!["code".into(), "--new-window".into()],
        };
        let argv = VsCodeHook.build_argv(&data).unwrap();
        assert_eq!(argv, vec!["code".to_string(), "--new-window".to_string()]);
    }

    #[test]
    fn build_argv_wrong_variant_errors() {
        let data = HookData::Browser {
            app_id: "firefox".into(),
            argv_fallback: vec!["firefox".into()],
        };
        assert!(VsCodeHook.build_argv(&data).is_err());
    }

    #[test]
    fn capitalized_code_app_id_maps_to_lowercase_bin() {
        // niri reports `Code` (capitalized) for upstream VS Code .deb.
        // The bin we spawn is still `code`.
        let data = HookData::VsCode {
            app_id: "Code".into(),
            cwd: Some("/tmp".into()),
            argv_fallback: vec!["code".into()],
        };
        let argv = VsCodeHook.build_argv(&data).unwrap();
        assert_eq!(argv[0], "code");
    }
}
