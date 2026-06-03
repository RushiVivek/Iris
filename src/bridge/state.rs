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
