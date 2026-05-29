//! Example WebSocket server demonstrating the ollama-wsock-connector protocol.
//!
//! Run the server:
//!     cargo run --example server -- --addr 127.0.0.1:9001
//!
//! Then start the client (in another terminal, with Ollama running locally):
//!     cargo run -- --ws-url ws://127.0.0.1:9001
//!
//! On each connection the server:
//!   1. Waits for the client's `hello` message.
//!   2. Sends a `list_models` request and logs the response.
//!   3. Runs a short two-turn conversation against the chosen model
//!      (`--model` or the first chat-capable model the client reports).
//!      The second turn references the first ("…in which continent is *that*
//!      country?") so it only makes sense if the server correctly carries the
//!      assistant's reply forward in the message history.
//!   4. Prints streamed `token` content to stdout as it arrives, per turn.
//!   5. Logs the final `done` stats and closes the connection.
//!
//! Multi-turn note: Ollama's `/api/chat` is stateless — each call must include
//! the full prior history. This server appends each streamed assistant reply
//! to its local `Vec<ChatMessage>` and re-sends it on the next `generate`.
//!
//! The protocol types below mirror `src/protocol.rs`. They are duplicated here
//! deliberately so this single file documents the full wire format — a server
//! author in any language can read this file and implement the same protocol.

use anyhow::{Context, Result};
use clap::Parser;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Write;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::{self, Message};
use tracing_subscriber::{fmt, EnvFilter};

// ───────────────────────── Protocol (mirror of src/protocol.rs) ──────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChatMessage {
    role: String,
    content: String,
}

/// Messages this server sends to the client.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ServerToClient {
    Generate {
        request_id: String,
        model: String,
        messages: Vec<ChatMessage>,
        #[serde(skip_serializing_if = "Option::is_none")]
        options: Option<Value>,
    },
    ListModels {
        request_id: String,
    },
}

/// Messages this server receives from the client.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientToServer {
    Hello {
        client_id: String,
        version: String,
    },
    Token {
        request_id: String,
        content: String,
    },
    Done {
        request_id: String,
        #[serde(default)]
        stats: Option<Stats>,
    },
    Models {
        request_id: String,
        models: Vec<ModelInfo>,
    },
    Error {
        request_id: String,
        message: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // read via Debug in `tracing::info!(?stats, ...)`
struct Stats {
    #[serde(default)]
    total_duration_ns: Option<u64>,
    #[serde(default)]
    eval_count: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ModelInfo {
    name: String,
    #[serde(default)]
    #[allow(dead_code)]
    size: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)]
    modified_at: Option<String>,
}

// ───────────────────────── CLI ───────────────────────────────────────────────

#[derive(Parser, Debug, Clone)]
#[command(about = "Example WebSocket server for ollama-wsock-connector")]
struct Cli {
    /// Address to bind the WebSocket server to.
    #[arg(long, default_value = "127.0.0.1:9001")]
    addr: SocketAddr,

    /// Model to request. If omitted, the first chat-capable model the client
    /// reports is used (see `pick_chat_model`).
    #[arg(long)]
    model: Option<String>,
}

/// Two-turn demo conversation. The second turn deliberately references the
/// first ("that country") so a broken history would produce a nonsense reply.
const CONVERSATION: &[&str] = &[
    "In which country is the city of Baku located?",
    "And on which continent is that country?",
];

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let listener = TcpListener::bind(cli.addr).await.context("bind")?;
    tracing::info!(addr = %cli.addr, "demo server listening, waiting for clients");

    loop {
        let (stream, peer) = listener.accept().await.context("accept")?;
        let cli = cli.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, peer, cli).await {
                tracing::error!(error = %e, %peer, "connection failed");
            }
        });
    }
}

// ───────────────────────── Connection handler ────────────────────────────────

async fn handle_connection(stream: TcpStream, peer: SocketAddr, cli: Cli) -> Result<()> {
    let ws = tokio_tungstenite::accept_async(stream)
        .await
        .context("websocket handshake")?;
    tracing::info!(%peer, "client connected");
    let (mut write, mut read) = ws.split();

    let outcome = run_demo(&mut write, &mut read, &cli).await;
    if let Err(ref e) = outcome {
        tracing::warn!(error = %e, %peer, "demo run failed; sending close frame");
    }
    // Always send a close frame so the client sees a clean WS shutdown rather
    // than "connection reset without closing handshake".
    let _ = write.send(Message::Close(None)).await;
    outcome
}

async fn run_demo<W, R>(write: &mut W, read: &mut R, cli: &Cli) -> Result<()>
where
    W: Sink<Message, Error = tungstenite::Error> + Unpin,
    R: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    // 1. Hello
    let Some(first) = recv(read).await? else {
        anyhow::bail!("client closed before sending hello");
    };
    match first {
        ClientToServer::Hello { client_id, version } => {
            tracing::info!(%client_id, %version, "received hello");
        }
        other => anyhow::bail!("expected hello, got {other:?}"),
    }

    // 2. Ask the client which models it has
    let list_id = "list-1".to_string();
    send(write, &ServerToClient::ListModels { request_id: list_id.clone() }).await?;
    tracing::info!(request_id = %list_id, "→ list_models");

    let models = loop {
        let Some(msg) = recv(read).await? else {
            anyhow::bail!("client closed before responding to list_models");
        };
        match msg {
            ClientToServer::Models { request_id, models } if request_id == list_id => break models,
            ClientToServer::Error { request_id, message } if request_id == list_id => {
                anyhow::bail!("client returned error for list_models: {message}");
            }
            other => tracing::warn!(?other, "ignoring unexpected message while waiting for models"),
        }
    };

    println!("\n=== Models reported by client ===");
    for m in &models {
        println!("  - {}", m.name);
    }
    if models.is_empty() {
        tracing::warn!("client has no models installed; demo cannot continue");
        return Ok(());
    }

    // 3. Pick a model.
    let model = match cli.model.clone() {
        Some(explicit) => explicit,
        None => {
            let pick = pick_chat_model(&models).unwrap_or(&models[0].name).to_string();
            tracing::info!(model = %pick, "auto-selected first model that looks chat-capable");
            pick
        }
    };

    // 4. Run the multi-turn conversation. We hold the running history in
    //    `history` and append both user and assistant messages so each turn is
    //    sent with the full prior context.
    let mut history: Vec<ChatMessage> = Vec::new();
    for (turn_idx, user_prompt) in CONVERSATION.iter().enumerate() {
        let turn_no = turn_idx + 1;
        history.push(ChatMessage {
            role: "user".to_string(),
            content: (*user_prompt).to_string(),
        });

        let gen_id = format!("gen-{turn_no}");
        send(
            write,
            &ServerToClient::Generate {
                request_id: gen_id.clone(),
                model: model.clone(),
                messages: history.clone(),
                options: None,
            },
        )
        .await?;
        tracing::info!(%model, request_id = %gen_id, turn = turn_no, "→ generate");

        println!("\n=== Turn {turn_no} · user ===");
        println!("{user_prompt}");
        println!("=== Turn {turn_no} · assistant ({model}) ===");

        // Collect the streamed reply both to stdout AND into a string we feed
        // back into history for the next turn.
        let assistant_reply = stream_assistant_reply(read, &gen_id).await?;
        history.push(ChatMessage {
            role: "assistant".to_string(),
            content: assistant_reply,
        });
    }

    Ok(())
}

/// Read frames until we see the `done` (or `error`) for `gen_id`, streaming
/// `token` payloads to stdout as they arrive and accumulating them into a
/// single string for the conversation history.
async fn stream_assistant_reply<R>(read: &mut R, gen_id: &str) -> Result<String>
where
    R: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    let mut buf = String::new();
    loop {
        let Some(msg) = recv(read).await? else {
            anyhow::bail!("client closed mid-stream");
        };
        match msg {
            ClientToServer::Token { request_id, content } if request_id == gen_id => {
                print!("{content}");
                let _ = std::io::stdout().flush();
                buf.push_str(&content);
            }
            ClientToServer::Done { request_id, stats } if request_id == gen_id => {
                println!();
                tracing::info!(?stats, request_id, "← done");
                return Ok(buf);
            }
            ClientToServer::Error { request_id, message } if request_id == gen_id => {
                println!();
                anyhow::bail!("client returned error during generation: {message}");
            }
            other => tracing::warn!(?other, "ignoring unexpected message during generation"),
        }
    }
}

/// Pick the first model that does NOT look like an embedding-only model and is
/// not a cloud-only tag. Real servers know which model they want; this is a
/// best-effort heuristic so the unattended demo works against the typical
/// developer's mixed model collection.
fn pick_chat_model(models: &[ModelInfo]) -> Option<&str> {
    models
        .iter()
        .map(|m| m.name.as_str())
        .find(|name| !looks_like_embedding_model(name) && !name.ends_with(":cloud"))
}

fn looks_like_embedding_model(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.contains("embed") || lower.starts_with("bge-") || lower.starts_with("bge:")
}

// ───────────────────────── I/O helpers ───────────────────────────────────────

async fn recv<S>(read: &mut S) -> Result<Option<ClientToServer>>
where
    S: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    loop {
        match read.next().await {
            Some(Ok(Message::Text(t))) => {
                let parsed = serde_json::from_str::<ClientToServer>(&t)
                    .with_context(|| format!("parsing client message: {t}"))?;
                return Ok(Some(parsed));
            }
            // tungstenite handles ping/pong internally; ignore everything else.
            Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_) | Message::Binary(_))) => {
                continue;
            }
            Some(Ok(Message::Close(_))) | None => return Ok(None),
            Some(Err(e)) => return Err(e.into()),
        }
    }
}

async fn send<S>(write: &mut S, msg: &ServerToClient) -> Result<()>
where
    S: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let text = serde_json::to_string(msg).context("serialize outbound")?;
    write.send(Message::Text(text)).await.context("send")?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(name: &str) -> ModelInfo {
        ModelInfo {
            name: name.to_string(),
            size: None,
            modified_at: None,
        }
    }

    #[test]
    fn pick_skips_embeddings_and_cloud_tags() {
        let models = vec![
            m("bge-m3:latest"),
            m("mxbai-embed-large:latest"),
            m("minimax-m2.5:cloud"),
            m("llama3.2:latest"),
            m("gemma3:4b"),
        ];
        assert_eq!(pick_chat_model(&models), Some("llama3.2:latest"));
    }

    #[test]
    fn pick_returns_none_if_only_embeddings() {
        let models = vec![m("bge-m3:latest"), m("nomic-embed-text:latest")];
        assert_eq!(pick_chat_model(&models), None);
    }

    #[test]
    fn pick_first_when_all_look_fine() {
        let models = vec![m("llama3.2:latest"), m("gemma3:4b")];
        assert_eq!(pick_chat_model(&models), Some("llama3.2:latest"));
    }
}
