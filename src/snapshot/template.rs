//! Variable substitution for snapshot TOML.
//!
//! Pre-deserialize textual replacement: `iris snapshot load NAME --var
//! KEY=VALUE ...` resolves `{{name}}` placeholders in the snapshot TOML
//! before `Snapshot::from_toml` parses it. Default values via
//! `{{name:default}}` syntax fall back when no `--var name=...` is given.
//!
//! Strict on missing variables: `{{name}}` with no `--var` and no
//! default is a hard error at load time, not a silent literal
//! pass-through.
//!
//! Substitution is naive textual replacement on the raw TOML string. We
//! don't parse TOML before substituting, so placeholders work anywhere
//! — `cwd`, `argv` array elements, `argv_fallback`, even `title`. The
//! schema is unaware of templating; only `read_snapshot_with_vars`
//! knows.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Result, anyhow};
use regex::Regex;

/// Substitute `{{name}}` and `{{name:default}}` placeholders in
/// `toml_src` against `vars`. Returns the substituted TOML string.
///
/// Errors with a clear message naming the missing variable when a
/// `{{name}}` placeholder has no entry in `vars` AND no default.
pub fn expand(toml_src: &str, vars: &HashMap<String, String>) -> Result<String> {
    let re = placeholder_regex();
    let mut last_err: Option<anyhow::Error> = None;
    let result = re.replace_all(toml_src, |caps: &regex::Captures<'_>| {
        let name = caps.get(1).map(|m| m.as_str().trim()).unwrap_or("");
        let default = caps.get(2).map(|m| m.as_str());
        if let Some(value) = vars.get(name) {
            value.clone()
        } else if let Some(default) = default {
            default.to_string()
        } else {
            // Stash the first missing-var error; we can't return Err
            // from inside replace_all's closure, so we record and
            // surface after the loop.
            if last_err.is_none() {
                last_err = Some(anyhow!(
                    "unknown variable {{{{{name}}}}} (pass --var {name}=...)"
                ));
            }
            // Replace with the literal placeholder so the error path
            // below sees a defined string; the error takes precedence.
            caps.get(0).unwrap().as_str().to_string()
        }
    });
    if let Some(err) = last_err {
        return Err(err);
    }
    Ok(result.into_owned())
}

fn placeholder_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // {{ name }} or {{ name:default with anything except } }}
        // - whitespace tolerated around the name
        // - default starts at the first ':' and runs to the closing '}}'
        // - `[^}]*` keeps the default greedy-but-bounded by the next `}`
        Regex::new(r"\{\{\s*(\w+)\s*(?::([^}]*))?\}\}").unwrap()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn single_variable_substitution() {
        let out = expand(
            r#"cwd = "{{wrkdir}}""#,
            &vars(&[("wrkdir", "/home/rushi/code")]),
        )
        .unwrap();
        assert_eq!(out, r#"cwd = "/home/rushi/code""#);
    }

    #[test]
    fn multiple_variables_in_same_input() {
        let src = r#"argv = ["{{bin}}", "--directory", "{{wrkdir}}"]"#;
        let out = expand(
            src,
            &vars(&[("bin", "kitty"), ("wrkdir", "/proj")]),
        )
        .unwrap();
        assert_eq!(out, r#"argv = ["kitty", "--directory", "/proj"]"#);
    }

    #[test]
    fn same_variable_used_twice_replaces_both() {
        let src = "a = \"{{x}}\"\nb = \"{{x}}\"";
        let out = expand(src, &vars(&[("x", "Y")])).unwrap();
        assert_eq!(out, "a = \"Y\"\nb = \"Y\"");
    }

    #[test]
    fn default_fallback_when_var_absent() {
        let out = expand(
            r#"cwd = "{{wrkdir:/tmp}}""#,
            &vars(&[]),
        )
        .unwrap();
        assert_eq!(out, r#"cwd = "/tmp""#);
    }

    #[test]
    fn default_overridden_when_var_present() {
        let out = expand(
            r#"cwd = "{{wrkdir:/tmp}}""#,
            &vars(&[("wrkdir", "/home/rushi/proj")]),
        )
        .unwrap();
        assert_eq!(out, r#"cwd = "/home/rushi/proj""#);
    }

    #[test]
    fn missing_var_with_no_default_errors() {
        let err = expand(r#"cwd = "{{wrkdir}}""#, &vars(&[])).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("wrkdir"), "error should name the variable: {msg}");
        assert!(msg.contains("--var"), "error should hint at the fix: {msg}");
    }

    #[test]
    fn default_with_spaces_and_slashes_preserved() {
        // The default value runs literal up to the closing `}}` — paths,
        // multi-word strings all pass through untouched.
        let out = expand(
            r#"x = "{{path:/usr/local/bin}}""#,
            &vars(&[]),
        )
        .unwrap();
        assert_eq!(out, r#"x = "/usr/local/bin""#);
    }

    #[test]
    fn default_with_embedded_colons_preserved() {
        // Default starts at FIRST `:` and runs to `}}` — additional
        // colons in the default are part of it, not a syntax marker.
        let out = expand(
            r#"x = "{{addr:127.0.0.1:8080}}""#,
            &vars(&[]),
        )
        .unwrap();
        assert_eq!(out, r#"x = "127.0.0.1:8080""#);
    }

    #[test]
    fn whitespace_around_name_is_trimmed() {
        let out = expand(
            r#"x = "{{  name  }}""#,
            &vars(&[("name", "alice")]),
        )
        .unwrap();
        assert_eq!(out, r#"x = "alice""#);
    }

    #[test]
    fn no_placeholders_passes_through_unchanged() {
        let src = r#"
version = 1
[workspace]
index = 1
"#;
        let out = expand(src, &vars(&[])).unwrap();
        assert_eq!(out, src);
    }

    #[test]
    fn empty_default_is_valid() {
        // `{{name:}}` → empty string when var unset.
        let out = expand(r#"x = "{{name:}}""#, &vars(&[])).unwrap();
        assert_eq!(out, r#"x = """#);
    }

    #[test]
    fn first_missing_variable_wins_in_error() {
        // Multiple undefined vars; error should name the first one
        // encountered (left-to-right).
        let err = expand(
            r#"a = "{{first}}"
b = "{{second}}""#,
            &vars(&[]),
        )
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("first"), "error should name first undefined: {msg}");
    }

    #[test]
    fn placeholder_anywhere_substitutes() {
        // Substitution scope: anywhere in the file. Placeholder in a
        // table key, an array element, a value, or a comment all get
        // replaced — pre-deserialize textual replacement makes no
        // distinction.
        let src = r#"
[table_{{section}}]
elem = ["{{a}}", "{{b}}"]
# comment with {{c}}
"#;
        let out = expand(
            src,
            &vars(&[("section", "X"), ("a", "1"), ("b", "2"), ("c", "see")]),
        )
        .unwrap();
        assert!(out.contains("[table_X]"));
        assert!(out.contains(r#"["1", "2"]"#));
        assert!(out.contains("# comment with see"));
    }
}
