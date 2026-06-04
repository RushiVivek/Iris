//! Pending-spawn queue: correlate `Op::Spawn` responses to the
//! `windows` events they produce, via the activation token.
//!
//! Flow during snapshot load with respawn:
//!   1. Send `Op::Spawn { request_activation_token: true }` → bridge
//!      mints a token, plants it in the child's `XDG_ACTIVATION_TOKEN`
//!      env, returns it. Caller `register()`s the token here.
//!   2. niri eventually emits `WindowOpenedOrChanged` for the spawned
//!      window. Bridge's `niri_conn` looks up the token by pid and
//!      stamps it onto the emitted `windows` event payload.
//!   3. Caller pumps each `windows` event into `dispatch()`. If the
//!      event's `activation_token` matches a pending entry, the entry's
//!      `oneshot::Sender<u64>` fires with the niri window id and the
//!      entry is removed.
//!   4. Caller `await`s each `oneshot::Receiver` with a per-spawn
//!      timeout. Timeouts mean the spawn either failed or its window
//!      never carried the token (e.g. niri reported `pid: None` for a
//!      portal app — see plan §593 / W2 open question).
//!
//! All state lives in this struct; no global. Callers get back
//! `oneshot::Receiver<u64>` and own their own timeout policy.

#![allow(dead_code)]

use std::collections::HashMap;

use serde_json::Value;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use crate::bridge::proto::Event;

pub struct PendingSpawns {
    by_token: HashMap<String, oneshot::Sender<u64>>,
    /// Window ids whose `windows` event arrived BEFORE the matching
    /// `register()` call. The race is real because the bridge dispatches
    /// the spawn-response synchronously after `cmd.spawn()` returns,
    /// and the spawned process may produce its `WindowOpenedOrChanged`
    /// before the loader's main task gets back to the lock for
    /// `register()`. Holding events here lets `register()` consume
    /// them retroactively.
    early_arrivals: HashMap<String, u64>,
}

impl Default for PendingSpawns {
    fn default() -> Self {
        Self::new()
    }
}

impl PendingSpawns {
    pub fn new() -> Self {
        Self {
            by_token: HashMap::new(),
            early_arrivals: HashMap::new(),
        }
    }

    /// Register a spawn. Returns the rx half of a `oneshot` that fires
    /// with the niri window id when a `windows` event tagged with this
    /// token arrives — OR immediately if the matching event has already
    /// been dispatched (early-arrival case).
    ///
    /// Caller is responsible for `await`-ing the rx with a timeout —
    /// `PendingSpawns` itself has no timer; events that never arrive
    /// just leave the entry in the map until the struct drops.
    /// (`drop(PendingSpawns)` cancels all in-flight oneshots, which the
    /// awaiters observe as `RecvError`.)
    pub fn register(&mut self, token: String) -> oneshot::Receiver<u64> {
        let (tx, rx) = oneshot::channel();
        // Early-arrival path: event was already dispatched for this
        // token before register() was called. Resolve immediately.
        if let Some(window_id) = self.early_arrivals.remove(&token) {
            debug!(
                token = %token,
                window_id,
                "register matched a pre-arrived event; resolving immediately"
            );
            // Send can only fail if the rx has been dropped, which
            // doesn't happen between the channel() and this line.
            let _ = tx.send(window_id);
            return rx;
        }
        if let Some(prev) = self.by_token.insert(token.clone(), tx) {
            // Same token registered twice → previous awaiter gets a
            // `RecvError` (sender dropped). This shouldn't happen in
            // normal flow (each spawn gets a fresh token from niri),
            // but if it does, document the failure.
            warn!(token = %token, "duplicate token registration; previous awaiter aborted");
            drop(prev);
        }
        rx
    }

    /// Drive an incoming `windows` event into the queue. If the event's
    /// `activation_token` matches a pending entry, the entry's sender
    /// fires with the window id; otherwise the event is ignored.
    /// Defensive against malformed events: missing token, missing
    /// window id, wrong topic — all silently dropped (with a debug log
    /// for diagnostics).
    pub fn dispatch(&mut self, event: &Event) {
        if event.event != crate::bridge::proto::topics::WINDOWS {
            return;
        }
        let Some(token) = event.data.get("activation_token").and_then(Value::as_str) else {
            // Most `windows` events don't carry a token (only the first
            // WindowOpenedOrChanged for a registered spawn does).
            return;
        };
        let Some(window_id) = event
            .data
            .get("opened_or_changed")
            .and_then(|w| w.get("id"))
            .and_then(Value::as_u64)
        else {
            // Token was stamped but the payload shape is off. Bridge
            // shouldn't emit this; defensive only.
            warn!(token = %token, "event has activation_token but no opened_or_changed.id");
            return;
        };
        if let Some(tx) = self.by_token.remove(token) {
            debug!(token = %token, window_id, "matched pending spawn to window");
            // If the receiver was dropped (caller gave up after
            // timeout), `send` errors — that's fine, we're done.
            let _ = tx.send(window_id);
        } else {
            // No pending entry for this token. Two possibilities:
            //   1. Early arrival — register() will be called shortly,
            //      and we should resolve it then.
            //   2. Spurious event from outside this loader's scope —
            //      e.g. a token minted by a different snapshot load
            //      that happened to overlap, or a duplicate event for
            //      an already-resolved entry.
            // We can't distinguish, so we stash unconditionally. The
            // caller drops PendingSpawns at end-of-flow, releasing
            // any unconsumed early arrivals.
            debug!(token = %token, window_id, "stashing event for not-yet-registered token");
            self.early_arrivals.insert(token.to_string(), window_id);
        }
    }

    /// How many spawns are still waiting for a matching event.
    pub fn pending_count(&self) -> usize {
        self.by_token.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn windows_event(token: Option<&str>, window_id: Option<u64>) -> Event {
        let mut data = json!({
            "opened_or_changed": match window_id {
                Some(id) => json!({ "id": id, "title": "irrelevant" }),
                None => json!({ "title": "no id" }),
            }
        });
        if let Some(t) = token {
            data["activation_token"] = json!(t);
        }
        Event {
            event: "windows".into(),
            ts: 0,
            data,
        }
    }

    #[tokio::test]
    async fn register_then_dispatch_fires_oneshot() {
        let mut pending = PendingSpawns::new();
        let rx = pending.register("tok-A".into());
        pending.dispatch(&windows_event(Some("tok-A"), Some(42)));
        assert_eq!(rx.await.unwrap(), 42);
        assert_eq!(pending.pending_count(), 0);
    }

    #[tokio::test]
    async fn dispatch_with_no_match_leaves_rx_pending() {
        let mut pending = PendingSpawns::new();
        let mut rx = pending.register("tok-A".into());
        pending.dispatch(&windows_event(Some("tok-OTHER"), Some(42)));
        // rx still pending; try_recv returns Empty.
        assert!(rx.try_recv().is_err());
        assert_eq!(pending.pending_count(), 1);
    }

    #[tokio::test]
    async fn out_of_order_dispatch_resolves_correct_token() {
        let mut pending = PendingSpawns::new();
        let rx_a = pending.register("tok-A".into());
        let rx_b = pending.register("tok-B".into());
        // Dispatch B before A.
        pending.dispatch(&windows_event(Some("tok-B"), Some(20)));
        pending.dispatch(&windows_event(Some("tok-A"), Some(10)));
        assert_eq!(rx_a.await.unwrap(), 10);
        assert_eq!(rx_b.await.unwrap(), 20);
    }

    #[tokio::test]
    async fn event_without_activation_token_is_ignored() {
        let mut pending = PendingSpawns::new();
        let mut rx = pending.register("tok-A".into());
        pending.dispatch(&windows_event(None, Some(42)));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn event_with_token_but_no_window_id_is_ignored() {
        let mut pending = PendingSpawns::new();
        let mut rx = pending.register("tok-A".into());
        pending.dispatch(&windows_event(Some("tok-A"), None));
        // Defensive: the entry is NOT consumed because the dispatch
        // couldn't extract a window id. The next well-formed event for
        // tok-A would still resolve.
        assert!(rx.try_recv().is_err());
        assert_eq!(pending.pending_count(), 1);
    }

    #[tokio::test]
    async fn non_windows_event_is_ignored() {
        let mut pending = PendingSpawns::new();
        let mut rx = pending.register("tok-A".into());
        let mut ev = windows_event(Some("tok-A"), Some(42));
        ev.event = "focus".into();
        pending.dispatch(&ev);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn duplicate_register_aborts_previous() {
        let mut pending = PendingSpawns::new();
        let rx_first = pending.register("tok-A".into());
        let rx_second = pending.register("tok-A".into());
        // The first rx should error (sender dropped).
        assert!(rx_first.await.is_err());
        // The second rx is the active one; resolves on dispatch.
        pending.dispatch(&windows_event(Some("tok-A"), Some(99)));
        assert_eq!(rx_second.await.unwrap(), 99);
    }

    #[tokio::test]
    async fn early_arrival_is_resolved_on_register() {
        // Race: bridge stamps the token onto a windows event before
        // the loader's main task gets back to `register()`. The event
        // should be stashed and delivered when register() runs.
        let mut pending = PendingSpawns::new();
        // Event arrives BEFORE register.
        pending.dispatch(&windows_event(Some("tok-A"), Some(42)));
        // Now register — should resolve immediately.
        let rx = pending.register("tok-A".into());
        assert_eq!(rx.await.unwrap(), 42);
        assert_eq!(pending.pending_count(), 0);
    }

    #[tokio::test]
    async fn early_arrival_does_not_leak_into_unrelated_register() {
        let mut pending = PendingSpawns::new();
        pending.dispatch(&windows_event(Some("tok-A"), Some(42)));
        // Register a different token — should NOT pick up the stashed
        // event (it belongs to tok-A).
        let mut rx_b = pending.register("tok-B".into());
        assert!(rx_b.try_recv().is_err());
        // tok-A is still in early_arrivals; registering tok-A now
        // resolves it.
        let rx_a = pending.register("tok-A".into());
        assert_eq!(rx_a.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn drop_pendingspawns_cancels_all_oneshots() {
        let mut pending = PendingSpawns::new();
        let rx1 = pending.register("tok-A".into());
        let rx2 = pending.register("tok-B".into());
        drop(pending);
        assert!(rx1.await.is_err());
        assert!(rx2.await.is_err());
    }
}
