//! Focus sampler. Emits `focus.sample` events at a configured cadence
//! (default 300ms) and `focus.session_end` when the focused window
//! changes. W7's `iris time` consumes these.
//!
//! Locked behavior:
//! - Skip ticks when no window is focused (the sampler is *about*
//!   a window; a null sample is semantically weird).
//! - On the focus boundary, emit `focus.session_end` for the previous
//!   window with `duration_ms = prev_ticks * interval_ms`.
//! - Reset on `state.reset` (niri reconnect): the previous-window
//!   counter is dropped because the trailing session is unknowable.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::json;
use tokio::time::MissedTickBehavior;

use super::proto::{self, topics};
use super::state::SharedState;

/// Pure boundary helper, separate from the runtime loop so we can unit-
/// test the session-end logic without a tokio interval.
///
/// Returns `Some((window_id, duration_ms))` iff the previous window's
/// session has ended (i.e. focus changed away from it).
pub fn compute_session_end(
    prev: Option<u64>,
    current: Option<u64>,
    prev_ticks: u64,
    interval_ms: u64,
) -> Option<(u64, u64)> {
    match (prev, current) {
        (Some(p), c) if Some(p) != c => Some((p, prev_ticks * interval_ms)),
        _ => None,
    }
}

pub async fn run(state: SharedState, interval: Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let interval_ms = interval.as_millis() as u64;
    let mut prev: Option<u64> = None;
    let mut prev_ticks: u64 = 0;
    let mut state_rx = state.subscribe();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let cur = state.with(|s| s.focused_window).await;

                if let Some((ended_id, dur_ms)) = compute_session_end(prev, cur, prev_ticks, interval_ms) {
                    let _ = state.events.send(proto::Event {
                        event: topics::FOCUS_SESSION_END.into(),
                        ts: now_ms(),
                        data: json!({
                            "window_id": ended_id,
                            "duration_ms": dur_ms,
                        }),
                    });
                }
                if cur != prev {
                    prev = cur;
                    prev_ticks = 0;
                }

                // Sample tick. Skip when nothing is focused.
                if let Some(id) = cur {
                    let win = state.with(|s| s.windows.get(&id).cloned()).await;
                    if let Some(w) = win {
                        let _ = state.events.send(proto::Event {
                            event: topics::FOCUS_SAMPLE.into(),
                            ts: now_ms(),
                            data: json!({
                                "window_id": id,
                                "app_id": w.app_id,
                                "title": w.title,
                                "workspace_id": w.workspace_id,
                                "interval_ms": interval_ms,
                            }),
                        });
                        prev_ticks += 1;
                    }
                }
            }
            ev = state_rx.recv() => {
                match ev {
                    Ok(e) if e.event == topics::STATE => {
                        // niri reconnect: trailing session unknowable.
                        prev = None;
                        prev_ticks = 0;
                    }
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // We may have missed a state.reset in the lagged
                        // batch; defensively reset so we don't credit
                        // ticks across a niri reconnect we can't see.
                        prev = None;
                        prev_ticks = 0;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_end_same_id_returns_none() {
        assert_eq!(compute_session_end(Some(1), Some(1), 5, 300), None);
    }

    #[test]
    fn session_end_different_id_returns_duration() {
        assert_eq!(
            compute_session_end(Some(1), Some(2), 5, 300),
            Some((1, 1500))
        );
    }

    #[test]
    fn session_end_to_none_returns_duration() {
        assert_eq!(
            compute_session_end(Some(1), None, 3, 300),
            Some((1, 900))
        );
    }

    #[test]
    fn session_end_first_sample_is_none() {
        // None → Some: no prior session to end.
        assert_eq!(compute_session_end(None, Some(1), 0, 300), None);
    }

    #[test]
    fn session_end_none_to_none_is_none() {
        assert_eq!(compute_session_end(None, None, 0, 300), None);
    }
}
