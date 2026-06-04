//! Browsers: firefox, chromium, Brave-browser, google-chrome.
//!
//! Per the locked decision: trust the browser's own session restore.
//! Don't parse `sessionstore.jsonlz4` / Chrome's `Sessions` directory /
//! anything proprietary. Capture is just `(app_id, argv_fallback)`;
//! `build_argv` returns `argv_fallback` verbatim.

#![allow(dead_code)]

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use crate::bridge::proto::Window;
use crate::snapshot::schema::HookData;

use super::AppHook;

pub struct BrowserHook;

/// (recognized app_id, binary on PATH).
const BROWSERS: &[(&str, &str)] = &[
    ("firefox", "firefox"),
    ("chromium", "chromium"),
    ("Brave-browser", "brave-browser"),
    ("google-chrome", "google-chrome-stable"),
];

#[async_trait]
impl AppHook for BrowserHook {
    fn name(&self) -> &'static str {
        "browser"
    }

    fn matches(&self, app_id: Option<&str>) -> bool {
        app_id
            .map(|id| BROWSERS.iter().any(|(known, _)| *known == id))
            .unwrap_or(false)
    }

    async fn capture(&self, w: &Window) -> Result<HookData> {
        let app_id = w
            .app_id
            .as_deref()
            .ok_or_else(|| anyhow!("BrowserHook::capture called with app_id=None"))?;
        let bin = bin_for(app_id)
            .ok_or_else(|| anyhow!("no browser binary mapping for app_id {app_id}"))?
            .to_string();
        Ok(HookData::Browser {
            app_id: app_id.to_string(),
            argv_fallback: vec![bin],
        })
    }

    fn build_argv(&self, data: &HookData) -> Result<Vec<String>> {
        let HookData::Browser { argv_fallback, .. } = data else {
            anyhow::bail!("BrowserHook can't build argv from non-Browser variant");
        };
        if argv_fallback.is_empty() {
            anyhow::bail!("Browser HookData has empty argv_fallback");
        }
        Ok(argv_fallback.clone())
    }
}

fn bin_for(app_id: &str) -> Option<&'static str> {
    BROWSERS.iter().find(|(id, _)| *id == app_id).map(|(_, bin)| *bin)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_each_supported_browser() {
        for (app_id, _) in BROWSERS {
            assert!(BrowserHook.matches(Some(app_id)), "should match {app_id}");
        }
    }

    #[test]
    fn does_not_match_unknown_or_none() {
        assert!(!BrowserHook.matches(None));
        assert!(!BrowserHook.matches(Some("foot")));
        assert!(!BrowserHook.matches(Some("Firefox"))); // case-sensitive
    }

    #[test]
    fn build_argv_returns_fallback_verbatim() {
        let data = HookData::Browser {
            app_id: "firefox".into(),
            argv_fallback: vec!["firefox".into()],
        };
        assert_eq!(BrowserHook.build_argv(&data).unwrap(), vec!["firefox".to_string()]);
    }

    #[test]
    fn build_argv_empty_fallback_errors() {
        let data = HookData::Browser {
            app_id: "firefox".into(),
            argv_fallback: vec![],
        };
        assert!(BrowserHook.build_argv(&data).is_err());
    }

    #[test]
    fn build_argv_wrong_variant_errors() {
        let data = HookData::Generic { argv: vec!["x".into()], cwd: None };
        assert!(BrowserHook.build_argv(&data).is_err());
    }
}
