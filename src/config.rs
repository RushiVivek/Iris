//! User config loader at `~/.config/iris/config.toml`.
//!
//! Best-effort: missing file → defaults; malformed TOML → defaults +
//! warn. Bridge never hard-fails on a bad config; the user gets a
//! warning in the log + a working daemon at default cadence.
//!
//! `IRIS_CONFIG` env var overrides the path (used by tests).

use std::path::PathBuf;

use serde::Deserialize;
use tracing::warn;

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct Config {
    pub focus: FocusConfig,
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct FocusConfig {
    pub sample_interval_ms: u64,
}

impl Default for FocusConfig {
    fn default() -> Self {
        Self { sample_interval_ms: 300 }
    }
}

pub fn load() -> Config {
    let path = config_path();
    let mut cfg = match std::fs::read_to_string(&path) {
        Ok(s) => match toml::from_str::<Config>(&s) {
            Ok(c) => c,
            Err(e) => {
                warn!(
                    "invalid config at {}: {e}; using defaults",
                    path.display()
                );
                Config::default()
            }
        },
        Err(_) => Config::default(),
    };
    // tokio::time::interval panics on Duration::ZERO; clamp here so
    // a malformed user config can never crash the sampler at startup.
    if cfg.focus.sample_interval_ms == 0 {
        warn!("focus.sample_interval_ms = 0 is not allowed; clamping to 50ms");
        cfg.focus.sample_interval_ms = 50;
    }
    cfg
}

fn config_path() -> PathBuf {
    if let Ok(p) = std::env::var("IRIS_CONFIG") {
        return PathBuf::from(p);
    }
    directories::ProjectDirs::from("", "", "iris")
        .map(|d| d.config_dir().join("config.toml"))
        .unwrap_or_else(|| PathBuf::from("/dev/null"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialize tests that mutate IRIS_CONFIG so they don't race.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_config<F: FnOnce() -> R, R>(contents: Option<&str>, f: F) -> R {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        if let Some(c) = contents {
            std::fs::write(&path, c).unwrap();
        }
        // SAFETY: the test holds ENV_LOCK so no concurrent test reads/writes
        // IRIS_CONFIG. Single-threaded mutation is fine.
        unsafe {
            std::env::set_var("IRIS_CONFIG", &path);
        }
        let r = f();
        unsafe {
            std::env::remove_var("IRIS_CONFIG");
        }
        r
    }

    #[test]
    fn missing_file_returns_defaults() {
        // Point at a path that doesn't exist.
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            std::env::set_var("IRIS_CONFIG", "/nonexistent/iris/config.toml");
        }
        let cfg = load();
        unsafe {
            std::env::remove_var("IRIS_CONFIG");
        }
        assert_eq!(cfg.focus.sample_interval_ms, 300);
    }

    #[test]
    fn valid_config_parses() {
        with_config(
            Some("[focus]\nsample_interval_ms = 1000\n"),
            || {
                let cfg = load();
                assert_eq!(cfg.focus.sample_interval_ms, 1000);
            },
        );
    }

    #[test]
    fn invalid_toml_returns_defaults() {
        with_config(Some("this is not valid toml"), || {
            let cfg = load();
            assert_eq!(cfg.focus.sample_interval_ms, 300);
        });
    }

    #[test]
    fn missing_field_uses_field_default() {
        // Empty file parses but no [focus] section → field defaults.
        with_config(Some(""), || {
            let cfg = load();
            assert_eq!(cfg.focus.sample_interval_ms, 300);
        });
    }

    #[test]
    fn zero_interval_clamps_to_floor() {
        // tokio::time::interval(Duration::ZERO) panics — guard against
        // a malformed config crashing the sampler at startup.
        with_config(
            Some("[focus]\nsample_interval_ms = 0\n"),
            || {
                let cfg = load();
                assert_eq!(cfg.focus.sample_interval_ms, 50);
            },
        );
    }
}
