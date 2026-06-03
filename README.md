# iris

niri toolkit. One Rust binary, several subcommands. Linux + niri only.

## What's here

- **`iris bridge`** — long-running daemon. Owns the niri IPC connection,
  caches windows/workspaces, multiplexes events out to clients over a
  Unix socket. Required by every other subcommand.
- **`iris snapshot`** — per-workspace named save/restore (planned).
- **`iris pin`** — sticky-follow a floating window (planned).
- **`iris scratchpad`** — peek-strip on the right edge (planned).
- **`iris time`** — per-app focus tracking (planned).

Only the bridge is functional in this commit; the rest stub out to "not
implemented yet."

## Build

```sh
cargo build --release
# or:
cargo install --path .
```

## Run

```sh
# Make sure niri is running and $NIRI_SOCKET is exported in this shell.
iris bridge
```

The daemon binds `$XDG_RUNTIME_DIR/iris.sock` (mode 0600) and writes its
PID to `$XDG_RUNTIME_DIR/iris.pid`. It refuses to start if another bridge
is already running.

To autostart with niri, add to `~/.config/niri/config.kdl`:

```kdl
spawn-at-startup "iris" "bridge"
```

Logging is controlled by `IRIS_LOG` (env-filter syntax). Default is `info`.

```sh
IRIS_LOG=debug iris bridge
```

## Smoke test the protocol

In one terminal:

```sh
iris bridge
```

In another:

```sh
# Subscribe + query in one session.
socat - UNIX-CONNECT:$XDG_RUNTIME_DIR/iris.sock
```

Then paste these one at a time:

```json
{"id":"1","op":"workspaces.list"}
{"id":"2","op":"windows.list"}
{"id":"3","op":"state.snapshot"}
{"id":"4","op":"subscribe","params":{"topics":["windows","workspaces","focus"]}}
```

Open or close a window in niri; you should see live events stream in.

## Wire protocol

Line-delimited JSON over the Unix socket. Every line is one JSON object.

**Request** (client → bridge):

```json
{"id": "<client-chosen>", "op": "<op>", "params": {...}}
```

**Response** (bridge → client):

```json
{"id": "<echo>", "ok": true,  "data": {...}}
{"id": "<echo>", "ok": false, "error": "..."}
```

**Event** (bridge → client, after `subscribe`; no `id`):

```json
{"event": "<topic>", "ts": <epoch_ms>, "data": {...}}
```

### Operations

| Op | Params | Description |
|---|---|---|
| `windows.list` | – | Cached list of all windows |
| `windows.get` | `{id}` | Single window or null |
| `workspaces.list` | – | Cached list of workspaces |
| `workspaces.focused` | – | The focused workspace, or null |
| `state.snapshot` | – | windows + workspaces + focus, single round-trip |
| `window.focus` | `{id}` | Focus a window |
| `window.close` | `{id}` | Close a window |
| `window.move_to_workspace` | `{id, workspace: {id} | {idx}}` | Move a window |
| `window.toggle_floating` | `{id}` | Toggle floating |
| `subscribe` | `{topics: [...]}` | Add topics to this client's subscription |
| `unsubscribe` | `{topics: [...]}` | Remove topics |
| `noop` | – | Heartbeat — clients use it to detect a dead bridge |

### Topics

`windows`, `workspaces`, `focus`, `focus.sample`, `focus.session_end`,
`idle`, `state`. (Not all are emitted yet — see project plan for the
phase that lights each one up.)

## Non-goals

- Multi-WM (Sway / Hyprland / GNOME / KDE) — niri only.
- D-Bus interface — UDS only.
- Cloud / remote — local only.
- Generic plugin system — hooks are hardcoded.
