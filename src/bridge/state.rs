//! Shared in-memory state. Holds the cached view of niri's windows /
//! workspaces and the registry of subscribed clients. Wrapped in
//! `Arc<Mutex<…>>` because the niri-conn task and per-client server tasks
//! both read and write it.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, broadcast};

use crate::bridge::proto::{Event, Window, Workspace};

/// Channel capacity for the broadcast used to fan out events. If a client
/// is too slow, it drops events (lagged). 256 is generous for a personal tool.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Shared, cheaply cloneable handle to all bridge-wide state.
#[derive(Clone)]
pub struct SharedState {
    inner: Arc<Mutex<Inner>>,
    /// Broadcast tx used by the niri-conn task to push events to every
    /// connected client. Each per-client task holds an `rx` it filters
    /// against the client's subscribed topics.
    pub events: broadcast::Sender<Event>,
}

pub struct Inner {
    pub windows: HashMap<u64, Window>,
    pub workspaces: HashMap<u64, Workspace>,
    pub focused_window: Option<u64>,
    pub focused_workspace: Option<u64>,
}

impl SharedState {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            inner: Arc::new(Mutex::new(Inner {
                windows: HashMap::new(),
                workspaces: HashMap::new(),
                focused_window: None,
                focused_workspace: None,
            })),
            events: tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    pub async fn with<R>(&self, f: impl FnOnce(&Inner) -> R) -> R {
        f(&*self.inner.lock().await)
    }

    pub async fn with_mut<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        f(&mut *self.inner.lock().await)
    }
}

/// Per-client subscription set. Held by each per-client server task.
#[derive(Default)]
pub struct ClientSubs {
    pub topics: HashSet<String>,
}

impl ClientSubs {
    pub fn matches(&self, topic: &str) -> bool {
        self.topics.contains(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::proto;
    use serde_json::json;

    fn mk_window(id: u64, app_id: &str) -> proto::Window {
        proto::Window {
            id,
            app_id: Some(app_id.into()),
            title: Some("t".into()),
            pid: Some(1234),
            workspace_id: Some(1),
            is_focused: false,
            is_floating: false,
        }
    }

    #[tokio::test]
    async fn with_mut_persists_writes_to_with() {
        let s = SharedState::new();
        s.with_mut(|inner| {
            inner.windows.insert(1, mk_window(1, "foot"));
            inner.focused_window = Some(1);
        })
        .await;
        let (count, focused) = s.with(|inner| (inner.windows.len(), inner.focused_window)).await;
        assert_eq!(count, 1);
        assert_eq!(focused, Some(1));
    }

    #[tokio::test]
    async fn broadcast_delivers_to_all_subscribers() {
        let s = SharedState::new();
        let mut a = s.subscribe();
        let mut b = s.subscribe();
        let ev = proto::Event {
            event: proto::topics::FOCUS.into(),
            ts: 0,
            data: json!({"focused_window_id": 7}),
        };
        s.events.send(ev).unwrap();
        let got_a = a.recv().await.unwrap();
        let got_b = b.recv().await.unwrap();
        assert_eq!(got_a.event, "focus");
        assert_eq!(got_b.event, "focus");
        assert_eq!(got_a.data["focused_window_id"], 7);
    }

    #[tokio::test]
    async fn broadcast_no_subscribers_is_not_an_error() {
        let s = SharedState::new();
        // No one is subscribed — this should NOT panic or error; broadcast
        // just drops the message. Real bridge code uses `let _ = send(...)`.
        let res = s.events.send(proto::Event {
            event: "x".into(),
            ts: 0,
            data: json!({}),
        });
        assert!(res.is_err(), "no subscribers means SendError");
        // The point is the next operation works fine.
        let _rx = s.subscribe();
    }

    #[test]
    fn client_subs_topic_filter() {
        let mut subs = ClientSubs::default();
        assert!(!subs.matches("windows"));
        subs.topics.insert("windows".into());
        subs.topics.insert("focus".into());
        assert!(subs.matches("windows"));
        assert!(subs.matches("focus"));
        assert!(!subs.matches("workspaces"));
    }
}
