use anyhow::{Context, Result};
use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use futures_util::SinkExt;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::Message;

use crate::config::Config;
use crate::ollama::OllamaClient;
use crate::protocol::{ChatMessage, Inbound, Outbound, Stats};

const OUTBOUND_CAP: usize = 256;
const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

pub async fn run_session(config: &Config) -> Result<()> {
    let mut req = config
        .ws_url
        .as_str()
        .into_client_request()
        .context("building ws upgrade request")?;
    if let Some(header_value) = &config.ws_auth_header {
        req.headers_mut().insert(
            AUTHORIZATION,
            header_value
                .parse()
                .context("invalid Authorization header value")?,
        );
    }

    let (ws_stream, _resp) = tokio_tungstenite::connect_async(req)
        .await
        .context("websocket connect")?;
    tracing::info!(url = %config.ws_url, "websocket connected");

    let (mut write, mut read) = ws_stream.split();
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Outbound>(OUTBOUND_CAP);

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = outbound_rx.recv().await {
            let text = match serde_json::to_string(&msg) {
                Ok(t) => t,
                Err(e) => {
                    tracing::error!(error = %e, "outbound serialize failed");
                    continue;
                }
            };
            if let Err(e) = write.send(Message::Text(text)).await {
                tracing::warn!(error = %e, "websocket write failed");
                return Err::<(), anyhow::Error>(e.into());
            }
        }
        let _ = write.send(Message::Close(None)).await;
        Ok(())
    });

    outbound_tx
        .send(Outbound::Hello {
            client_id: config.client_id.clone(),
            version: PKG_VERSION.to_string(),
        })
        .await
        .context("sending hello message")?;

    let ollama = OllamaClient::new(config.ollama_url.clone());
    let mut handlers: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();

    let read_result: Result<()> = loop {
        tokio::select! {
            biased;
            _ = handlers.next(), if !handlers.is_empty() => {}
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(t))) => {
                        match serde_json::from_str::<Inbound>(&t) {
                            Ok(inbound) => {
                                let tx = outbound_tx.clone();
                                let o = ollama.clone();
                                handlers.push(Box::pin(dispatch(inbound, tx, o)));
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, raw = %t, "could not parse inbound frame");
                            }
                        }
                    }
                    Some(Ok(Message::Binary(_))) => {
                        tracing::warn!("ignoring binary frame");
                    }
                    Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => {}
                    Some(Ok(Message::Close(frame))) => {
                        tracing::info!(?frame, "server sent close");
                        break Ok(());
                    }
                    Some(Err(e)) => {
                        break Err(anyhow::Error::from(e).context("websocket read"));
                    }
                    None => {
                        tracing::info!("websocket stream ended");
                        break Ok(());
                    }
                }
            }
        }
    };

    drop(outbound_tx);
    // Drain remaining handlers so their final outbound messages get flushed.
    while handlers.next().await.is_some() {}
    let _ = writer_task.await;
    read_result
}

async fn dispatch(inbound: Inbound, tx: mpsc::Sender<Outbound>, ollama: OllamaClient) {
    match inbound {
        Inbound::Generate {
            request_id,
            model,
            messages,
            options,
        } => {
            if let Err(e) = handle_generate(&request_id, &model, &messages, options.as_ref(), &ollama, &tx).await {
                tracing::warn!(error = %e, request_id, "generate failed");
                let _ = tx
                    .send(Outbound::Error {
                        request_id,
                        message: format!("{e:#}"),
                    })
                    .await;
            }
        }
        Inbound::ListModels { request_id } => {
            match ollama.list_models().await {
                Ok(models) => {
                    let _ = tx.send(Outbound::Models { request_id, models }).await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, request_id, "list_models failed");
                    let _ = tx
                        .send(Outbound::Error {
                            request_id,
                            message: format!("{e:#}"),
                        })
                        .await;
                }
            }
        }
    }
}

async fn handle_generate(
    request_id: &str,
    model: &str,
    messages: &[ChatMessage],
    options: Option<&Value>,
    ollama: &OllamaClient,
    tx: &mpsc::Sender<Outbound>,
) -> Result<()> {
    let mut stream = ollama.chat_stream(model, messages, options).await?;
    let mut final_stats: Option<Stats> = None;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if !chunk.content.is_empty()
            && tx
                .send(Outbound::Token {
                    request_id: request_id.to_string(),
                    content: chunk.content,
                })
                .await
                .is_err()
        {
            return Ok(());
        }
        if chunk.done {
            final_stats = Some(Stats {
                total_duration_ns: chunk.total_duration_ns,
                eval_count: chunk.eval_count,
            });
        }
    }

    let _ = tx
        .send(Outbound::Done {
            request_id: request_id.to_string(),
            stats: final_stats,
        })
        .await;
    Ok(())
}
