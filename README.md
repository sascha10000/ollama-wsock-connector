# ollama-wsock-connector

A small Rust client that bridges a remote WebSocket service to a user's local
[Ollama](https://ollama.com) instance — so a service operator can offer
"bring-your-own local inference" without ever proxying or holding the user's
prompts and completions in plain text on the operator's side.

```
   Service Backend  ◀──── WebSocket ────▶  ollama-wsock-connector  ◀── HTTP ──▶  Ollama
       (yours)                                  (on user's machine)              (on user's machine)
```

The connector dials *outward* from the user's machine, so it works behind home
routers and corporate NATs that block inbound traffic. It maintains the
connection, listens for chat requests, runs them against the local Ollama HTTP
API, and streams the generated tokens back to the server over the same
WebSocket. It also responds to a "which models are installed?" probe so the
service can adapt to what the user actually has.

## Who this is for

This tool is for **services that want to let their users run inference on the
user's own hardware**, instead of (or in addition to) a cloud LLM. Reasons you
might want that:

- **Privacy** — the prompt and the completion never leave the user's machine.
- **Cost** — you pay nothing per token; the user's GPU does the work.
- **Model choice** — your users can use any model they have pulled locally,
  including ones you couldn't legally host yourself.
- **Compliance** — for regulated industries where data residency matters.

The hard part is usually plumbing: browser code can't talk to `localhost`
reliably, your backend can't punch through the user's firewall, and you don't
want users running random Docker containers. This connector is the smallest
amount of moving parts that solves that.

## For end users (the people running this on their machine)

You shouldn't need to know anything about WebSockets, Rust, or Ollama
internals. The service that asked you to install this should give you **two
files**:

1. The `ollama-wsock-connector` executable.
2. A `config.toml` file pre-filled with their server URL (and any token they
   issued you).

Put both files in the same folder, make sure Ollama is running locally
(`ollama serve`), then run the executable:

```
./ollama-wsock-connector
```

That's it. The connector dials out to the service, identifies your machine,
and waits. When the service wants to use a model on your machine, it sends a
request, your local Ollama answers it, and the answer streams back — your
prompts never leave your machine in plain text to anyone but the service that
issued the config.

Stop it with `Ctrl-C`.

## For service implementers (the people running the WebSocket server)

The user-side story you can sell is: **download two files, drop them in a
folder, run the binary** — nothing else. To make that true, you build a
config once per user (or once per deployment) and ship it alongside the
binary. The connector loads `./config.toml` automatically when present; no
CLI flags, no env vars, no installer.

A minimal config to hand a user looks like this:

```toml
[websocket]
url = "wss://api.yourservice.com/ollama-bridge"
# If your auth needs it, embed the user's token here. The connector sends it
# as the Authorization header on the WebSocket upgrade request.
# auth_header = "Bearer <user-specific-token>"

[ollama]
url = "http://127.0.0.1:11434"

[client]
id = "user-12345"      # whatever identifier your backend expects
```

On your side, accept the WebSocket connection and implement the protocol
described below. A complete, copyable reference server lives in
[`examples/server.rs`](examples/server.rs). Run it locally with:

```
cargo run --example server
```

…then start the connector against it (`cargo run -- --ws-url ws://127.0.0.1:9001`)
to watch a real round-trip, including a two-turn conversation that exercises
multi-turn history handling.

### Distributing the binary

Build for the platforms your users have:

```
# native release build for your current platform
cargo build --release
# binary at: target/release/ollama-wsock-connector
```

For cross-compiling to other platforms, [`cross`](https://github.com/cross-rs/cross)
or a CI matrix is the usual approach. The binary statically links rustls so
there is no OpenSSL dependency on the user's machine.

## Protocol

All messages are JSON, tagged by a `type` field, and correlated by a
`request_id` chosen by whichever side initiated the exchange.

### Server → connector

- `list_models` — ask the user's machine which Ollama models are installed.
  ```json
  { "type": "list_models", "request_id": "any-string" }
  ```

- `generate` — ask the user's Ollama for a chat completion. The shape of
  `messages` and `options` mirrors Ollama's
  [`/api/chat`](https://github.com/ollama/ollama/blob/main/docs/api.md#generate-a-chat-completion)
  so the connector can pass them through unchanged.
  ```json
  {
    "type": "generate",
    "request_id": "any-string",
    "model": "llama3.2:latest",
    "messages": [
      { "role": "system", "content": "You are a concise assistant." },
      { "role": "user",   "content": "Hi!" }
    ],
    "options": { "temperature": 0.7 }
  }
  ```
  `messages` is the **full conversation history**. Ollama's chat endpoint is
  stateless, so for multi-turn conversations the server is responsible for
  accumulating the prior `assistant` replies and re-sending them. See
  `examples/server.rs` for a worked example.

### Connector → server

- `hello` — sent once, immediately after the WebSocket upgrade.
  ```json
  { "type": "hello", "client_id": "user-12345", "version": "0.1.0" }
  ```

- `token` — one streamed chunk of an in-flight `generate`.
  ```json
  { "type": "token", "request_id": "...", "content": "Hel" }
  ```

- `done` — generation finished. Includes optional Ollama timing stats.
  ```json
  { "type": "done", "request_id": "...",
    "stats": { "total_duration_ns": 123, "eval_count": 42 } }
  ```

- `models` — reply to `list_models`.
  ```json
  { "type": "models", "request_id": "...",
    "models": [
      { "name": "llama3.2:latest", "size": 2000000000, "modified_at": "2025-01-01T00:00:00Z" }
    ]
  }
  ```

- `error` — anything went wrong for that `request_id` (model not found,
  Ollama unreachable, etc.). The corresponding `generate`/`list_models` is
  cancelled; the WebSocket stays open for further requests.
  ```json
  { "type": "error", "request_id": "...", "message": "ollama /api/chat returned 400 Bad Request: ..." }
  ```

### Behavioural guarantees

- **Concurrent requests** — the connector handles multiple in-flight
  `generate`s on a single WebSocket, multiplexed by `request_id`.
- **Reconnect** — if the WebSocket drops, the connector retries with
  exponential backoff (1s → 2s → … → 30s cap) until it reconnects or the
  user kills it. In-flight requests at the moment of disconnect are dropped;
  the server is expected to retry if needed.
- **No state replay** — there is no persistent queue. A fresh `hello` is
  sent on every (re)connect.

## Configuration

`./config.toml` is loaded automatically when present. Every field can also be
set via a CLI flag or an environment variable; CLI > env > file in precedence.

| What                       | TOML key                  | CLI flag             | Env var               | Default                       |
| -------------------------- | ------------------------- | -------------------- | --------------------- | ----------------------------- |
| WebSocket URL              | `[websocket].url`         | `--ws-url`           | `OWSC_WS_URL`         | *(required)*                  |
| WebSocket auth header      | `[websocket].auth_header` | `--ws-auth-header`   | `OWSC_WS_AUTH_HEADER` | none                          |
| Ollama base URL            | `[ollama].url`            | `--ollama-url`       | `OWSC_OLLAMA_URL`     | `http://127.0.0.1:11434`      |
| Client identifier (hello)  | `[client].id`             | `--client-id`        | `OWSC_CLIENT_ID`      | `client-<random-uuid>`        |
| Tracing log level / filter | `[client].log_level`      | `--log-level`        | `OWSC_LOG_LEVEL`      | `info`                        |
| Path to config file        | —                         | `--config <path>`    | `OWSC_CONFIG`         | `./config.toml` if it exists  |

`./ollama-wsock-connector --help` prints the full CLI surface.

## Development

```
cargo build                                       # debug build
cargo build --release                             # release build
cargo test                                        # library + integration tests
cargo test --example server                       # heuristic tests in the demo server
cargo clippy --all-targets -- -D warnings         # lints
cargo run -- --ws-url ws://127.0.0.1:9001         # run the client against a local server
cargo run --example server                        # run the demo server
```

Source layout:

```
src/
├── main.rs       # CLI, tracing, reconnect loop
├── config.rs     # TOML + CLI merge
├── protocol.rs   # tagged-JSON message enums (serde)
├── ollama.rs     # Ollama HTTP client (streaming chat + list_models)
└── session.rs    # one WebSocket session: dispatch, per-request handlers
examples/
└── server.rs     # reference server implementation
```
