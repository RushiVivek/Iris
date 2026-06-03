//! Re-exports of bridge wire types so subcommands can write
//! `use crate::client::proto::Op` without reaching into `bridge::`. The
//! point is to give `bridge::proto` and `client` the same surface to
//! consumers — if we ever split the wire types into their own crate, only
//! this re-export changes.

#![allow(dead_code, unused_imports)]

pub use crate::bridge::proto::*;
