//! XDG activation v1 broker.
//!
//! Owns one Wayland client connection for the bridge's lifetime. niri (or any
//! XDG-activation-capable compositor) hands out one token per
//! `get_activation_token` round-trip; we reply to the spawn op with that
//! token, set `XDG_ACTIVATION_TOKEN` in the spawned child's env, and remember
//! `pid → token` so the niri-conn task can stamp the token onto the matching
//! `WindowOpenedOrChanged` event.
//!
//! niri-ipc 25.11.0's `Window` does not expose the activation token niri
//! observed, so correlation is by **pid**: niri-ipc *does* surface
//! `Window::pid`. If/when niri-ipc gains a token field, switch the matcher in
//! `niri_conn.rs` and this module's API stays the same.
//!
//! Threading: the Wayland event queue is blocking + `!Send` in practice, so
//! it lives on a dedicated `std::thread`. The tokio side talks to it via an
//! mpsc command channel and awaits a `oneshot` for the resulting token. The
//! pid map is a plain `Arc<Mutex<...>>` because the niri-conn task reads it
//! synchronously.

#![allow(dead_code)]

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

use wayland_client::{
    Connection, Dispatch, EventQueue, QueueHandle,
    globals::{GlobalListContents, registry_queue_init},
    protocol::wl_registry,
};
use wayland_protocols::xdg::activation::v1::client::{
    xdg_activation_token_v1::{self, XdgActivationTokenV1},
    xdg_activation_v1::XdgActivationV1,
};

/// How long a pid → token entry lingers in the map. Spawn-to-window is
/// usually <500ms; this just bounds memory if a spawn never produces a
/// window. Eviction-only.
const PID_TOKEN_TTL: Duration = Duration::from_secs(30);

/// Maximum age at which `take_token_for_pid` will still hand back the
/// token. Tighter than the TTL because the *correlation window* is what
/// matters: the kernel can recycle a pid within seconds of process exit,
/// and we'd rather drop a real (but slow) match than stamp a stale token
/// onto an unrelated app's window.
const PID_TOKEN_MATCH_WINDOW: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
struct TokenEntry {
    token: String,
    inserted_at: Instant,
}

/// Cheaply-cloneable handle to the activation broker. Created once during
/// bridge startup; cloned into the server task and the niri-conn task.
#[derive(Clone)]
pub struct ActivationBroker {
    cmd_tx: mpsc::Sender<Command>,
    pid_map: Arc<Mutex<HashMap<i32, TokenEntry>>>,
}

enum Command {
    Mint(oneshot::Sender<Result<String>>),
}

impl ActivationBroker {
    /// Spawn the Wayland thread, return a handle. Errors only if the initial
    /// Wayland connect or the bind of `xdg_activation_v1` fail synchronously.
    pub fn start() -> Result<Self> {
        let (cmd_tx, cmd_rx) = mpsc::channel::<Command>(32);
        let pid_map = Arc::new(Mutex::new(HashMap::<i32, TokenEntry>::new()));

        // Synchronous handshake on this thread: connect, bind global. If that
        // works we hand the queue off to a dedicated worker thread. We do it
        // here so a missing `XDG_ACTIVATION_TOKEN` global surfaces during
        // bridge startup (loud) rather than on first spawn op (quiet).
        let conn = Connection::connect_to_env()
            .map_err(|e| anyhow!("connecting to Wayland display: {e}"))?;
        let (globals, queue) = registry_queue_init::<BrokerState>(&conn)
            .map_err(|e| anyhow!("Wayland registry init: {e}"))?;
        let qh = queue.handle();
        let activation: XdgActivationV1 = globals
            .bind(&qh, 1..=1, ())
            .map_err(|e| anyhow!("binding xdg_activation_v1: {e}"))?;

        info!("xdg_activation_v1 bound; broker thread starting");

        thread::Builder::new()
            .name("iris-wayland".into())
            .spawn(move || run_worker(conn, queue, activation, cmd_rx))
            .map_err(|e| anyhow!("spawning Wayland worker thread: {e}"))?;

        Ok(Self { cmd_tx, pid_map })
    }

    /// Mint a fresh activation token. Returns the token string the
    /// compositor allocated. Errors if the broker is dead or the round-trip
    /// fails.
    pub async fn mint_token(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Mint(tx))
            .await
            .map_err(|_| anyhow!("activation broker thread is gone"))?;
        rx.await
            .map_err(|_| anyhow!("activation broker dropped reply channel"))?
    }

    /// Remember `pid → token` so a later `WindowOpenedOrChanged` for that
    /// pid can be stamped with the token. Sweeps expired entries on the way
    /// in to bound memory.
    pub fn register_spawn(&self, pid: i32, token: String) {
        let mut map = self.pid_map.lock().expect("pid_map poisoned");
        sweep_expired(&mut map);
        map.insert(
            pid,
            TokenEntry { token, inserted_at: Instant::now() },
        );
    }

    /// Build a broker handle whose Wayland side is dead. Used by tests in
    /// other modules that need a working pid map but can't (and shouldn't)
    /// reach a real compositor. `mint_token()` will error since there's no
    /// worker. Use [`Self::test_handle_with_minter`] when a happy-path mint
    /// is needed.
    #[cfg(test)]
    pub fn test_handle() -> Self {
        let (cmd_tx, _cmd_rx) = mpsc::channel::<Command>(1);
        Self {
            cmd_tx,
            pid_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build a broker handle backed by an in-process tokio task that
    /// synthesizes activation tokens (`fake-token-{N}`). Lets server tests
    /// exercise the full spawn → mint → register path without Wayland.
    #[cfg(test)]
    pub fn test_handle_with_minter() -> Self {
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(32);
        let pid_map = Arc::new(Mutex::new(HashMap::new()));
        tokio::spawn(async move {
            let mut counter: u64 = 0;
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    Command::Mint(reply) => {
                        counter += 1;
                        let _ = reply.send(Ok(format!("fake-token-{counter}")));
                    }
                }
            }
        });
        Self { cmd_tx, pid_map }
    }

    /// Look up and remove the token for a pid. Returns `None` if no token
    /// was registered, the entry has expired by TTL, or the correlation
    /// window has elapsed (in which case the entry is also dropped — a
    /// late-arriving event won't match either, but more importantly a
    /// recycled pid for a subsequent unrelated window won't either).
    pub fn take_token_for_pid(&self, pid: i32) -> Option<String> {
        let mut map = self.pid_map.lock().expect("pid_map poisoned");
        sweep_expired(&mut map);
        let entry = map.remove(&pid)?;
        if entry.inserted_at.elapsed() <= PID_TOKEN_MATCH_WINDOW {
            Some(entry.token)
        } else {
            None
        }
    }
}

fn sweep_expired(map: &mut HashMap<i32, TokenEntry>) {
    let now = Instant::now();
    map.retain(|_, e| now.duration_since(e.inserted_at) < PID_TOKEN_TTL);
}

// ─────────────────────────────── Wayland worker ───────────────────────────────

struct BrokerState {
    /// Filled in by the `done` event on `XdgActivationTokenV1`. Set by the
    /// dispatch impl, drained by the worker loop after a roundtrip.
    pending_token: Option<String>,
}

fn run_worker(
    _conn: Connection,
    mut queue: EventQueue<BrokerState>,
    activation: XdgActivationV1,
    mut cmd_rx: mpsc::Receiver<Command>,
) {
    let mut state = BrokerState { pending_token: None };
    let qh = queue.handle();
    debug!("Wayland worker loop entered");

    // `blocking_recv` returns None only when every Sender is dropped. The
    // broker is `Clone` so in production this happens at process teardown.
    while let Some(cmd) = cmd_rx.blocking_recv() {
        match cmd {
            Command::Mint(reply) => {
                // Catch a panic in the Wayland call stack so a single bad
                // round-trip doesn't permanently wedge every future
                // mint_token() (the request-side oneshot would otherwise
                // never resolve, since the worker thread would be dead and
                // future `cmd_tx.send().await`s would error — but the
                // currently in-flight `reply.send` would never fire).
                let result = match catch_unwind(AssertUnwindSafe(|| {
                    mint_one(&mut queue, &qh, &activation, &mut state)
                })) {
                    Ok(r) => r,
                    Err(_) => {
                        error!("activation broker mint_one panicked; reporting failure");
                        // Best-effort: clear any half-set state so the next
                        // mint starts clean.
                        state.pending_token = None;
                        Err(anyhow!("activation broker panicked during mint"))
                    }
                };
                let _ = reply.send(result);
            }
        }
    }

    // Sender side has gone away. Drain anything that raced in just before
    // shutdown so callers don't hang on their oneshot rx.
    while let Ok(cmd) = cmd_rx.try_recv() {
        match cmd {
            Command::Mint(reply) => {
                let _ = reply.send(Err(anyhow!("activation broker shutting down")));
            }
        }
    }
    info!("activation broker shutting down");
}

fn mint_one(
    queue: &mut EventQueue<BrokerState>,
    qh: &QueueHandle<BrokerState>,
    activation: &XdgActivationV1,
    state: &mut BrokerState,
) -> Result<String> {
    state.pending_token = None;
    // get_activation_token() returns a new XdgActivationTokenV1 proxy. We
    // skip set_serial / set_app_id / set_surface — they're optional, and the
    // bridge doesn't have a surface anyway. commit() asks the compositor to
    // emit `done` with the token string.
    let tok = activation.get_activation_token(qh, ());
    tok.commit();
    queue.flush()?;
    // One round-trip is enough: niri's xdg_activation_v1 replies inline
    // with `done` and `roundtrip()` blocks until the server has processed
    // every request we sent. A wedged compositor would block here forever;
    // that's a known limitation — the broker's mpsc backpressures the
    // server task, and bridge restart unblocks. A wall-clock timeout would
    // require switching to `prepare_read`/`poll`; not worth the complexity
    // for v1.
    queue.roundtrip(state)?;
    tok.destroy();
    state
        .pending_token
        .take()
        .ok_or_else(|| anyhow!("compositor did not emit activation token after roundtrip"))
}

// ─────────────────────────────── Dispatch impls ──────────────────────────────

// Registry events: we don't react to runtime add/remove for the activation
// global; what we bound at startup is what we use.
impl Dispatch<wl_registry::WlRegistry, GlobalListContents> for BrokerState {
    fn event(
        _: &mut Self,
        _: &wl_registry::WlRegistry,
        _: wl_registry::Event,
        _: &GlobalListContents,
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}

// xdg_activation_v1 has no client-side events — empty impl.
impl Dispatch<XdgActivationV1, ()> for BrokerState {
    fn event(
        _: &mut Self,
        _: &XdgActivationV1,
        _: <XdgActivationV1 as wayland_client::Proxy>::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
    }
}

impl Dispatch<XdgActivationTokenV1, ()> for BrokerState {
    fn event(
        state: &mut Self,
        _: &XdgActivationTokenV1,
        event: xdg_activation_token_v1::Event,
        _: &(),
        _: &Connection,
        _: &QueueHandle<Self>,
    ) {
        if let xdg_activation_token_v1::Event::Done { token } = event {
            if state.pending_token.is_some() {
                warn!("dropping previous pending token; new one arrived");
            }
            state.pending_token = Some(token);
        }
    }
}

// ─────────────────────────────────── Tests ───────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_take_round_trip() {
        let b = ActivationBroker::test_handle();
        b.register_spawn(1234, "tok-A".into());
        assert_eq!(b.take_token_for_pid(1234).as_deref(), Some("tok-A"));
        // Second take is None — registration is consumed.
        assert!(b.take_token_for_pid(1234).is_none());
    }

    #[test]
    fn unknown_pid_returns_none() {
        let b = ActivationBroker::test_handle();
        assert!(b.take_token_for_pid(9999).is_none());
    }

    #[test]
    fn expired_entries_are_swept() {
        let b = ActivationBroker::test_handle();
        // Insert an entry with a stale timestamp directly so we don't have
        // to sleep 30s in a unit test.
        b.pid_map.lock().unwrap().insert(
            42,
            TokenEntry {
                token: "stale".into(),
                inserted_at: Instant::now() - PID_TOKEN_TTL - Duration::from_secs(1),
            },
        );
        // Triggering any access path runs sweep_expired.
        assert!(b.take_token_for_pid(42).is_none());
        assert!(b.pid_map.lock().unwrap().is_empty());
    }

    #[test]
    fn multiple_pids_are_independent() {
        let b = ActivationBroker::test_handle();
        b.register_spawn(1, "one".into());
        b.register_spawn(2, "two".into());
        assert_eq!(b.take_token_for_pid(2).as_deref(), Some("two"));
        assert_eq!(b.take_token_for_pid(1).as_deref(), Some("one"));
    }

    #[test]
    fn entry_outside_match_window_is_not_returned() {
        // Entry is still within the eviction TTL (so sweep_expired keeps it)
        // but past the correlation window. Defends against pid recycling:
        // a stale slow-spawn shouldn't stamp its token onto an unrelated
        // window that just happened to receive the same pid.
        let b = ActivationBroker::test_handle();
        b.pid_map.lock().unwrap().insert(
            7,
            TokenEntry {
                token: "tok-late".into(),
                inserted_at: Instant::now() - PID_TOKEN_MATCH_WINDOW - Duration::from_secs(1),
            },
        );
        assert!(b.take_token_for_pid(7).is_none());
        // Entry is consumed even though we returned None — caller asked
        // for the token, we said "no" definitively.
        assert!(b.pid_map.lock().unwrap().is_empty());
    }

    #[test]
    fn register_overwrites_existing_pid() {
        // If a pid is recycled and we spawn into it again before the first
        // entry expires, the new token wins. Asserting current behavior.
        let b = ActivationBroker::test_handle();
        b.register_spawn(100, "first".into());
        b.register_spawn(100, "second".into());
        assert_eq!(b.take_token_for_pid(100).as_deref(), Some("second"));
        assert!(b.take_token_for_pid(100).is_none());
    }
}
