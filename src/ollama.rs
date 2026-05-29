use anyhow::{Context, Result};
use futures_util::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::protocol::{ChatMessage, ModelInfo};

#[derive(Debug, Clone)]
pub struct OllamaClient {
    http: reqwest::Client,
    base: Url,
}

#[derive(Debug, Clone, Default)]
pub struct ChatChunk {
    pub content: String,
    pub done: bool,
    pub total_duration_ns: Option<u64>,
    pub eval_count: Option<u64>,
}

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: &'a [ChatMessage],
    stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<&'a Value>,
}

#[derive(Deserialize)]
struct ChatRawChunk {
    #[serde(default)]
    message: Option<ChatRawMessage>,
    #[serde(default)]
    done: bool,
    #[serde(default)]
    total_duration: Option<u64>,
    #[serde(default)]
    eval_count: Option<u64>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct ChatRawMessage {
    #[serde(default)]
    content: String,
}

#[derive(Deserialize)]
struct TagsResponse {
    #[serde(default)]
    models: Vec<TagsModel>,
}

#[derive(Deserialize)]
struct TagsModel {
    name: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default)]
    modified_at: Option<String>,
}

impl OllamaClient {
    pub fn new(base: Url) -> Self {
        Self {
            http: reqwest::Client::new(),
            base,
        }
    }

    pub async fn list_models(&self) -> Result<Vec<ModelInfo>> {
        let url = self.base.join("api/tags").context("building /api/tags url")?;
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .context("sending /api/tags request")?;
        let resp = check_status(resp, "/api/tags").await?;
        let tags: TagsResponse = resp.json().await.context("parsing /api/tags response")?;
        Ok(tags
            .models
            .into_iter()
            .map(|m| ModelInfo {
                name: m.name,
                size: m.size,
                modified_at: m.modified_at,
            })
            .collect())
    }

    pub async fn chat_stream(
        &self,
        model: &str,
        messages: &[ChatMessage],
        options: Option<&Value>,
    ) -> Result<impl Stream<Item = Result<ChatChunk>> + Send + Unpin> {
        let url = self.base.join("api/chat").context("building /api/chat url")?;
        let body = ChatRequest {
            model,
            messages,
            stream: true,
            options,
        };
        let resp = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await
            .context("sending /api/chat request")?;
        let resp = check_status(resp, "/api/chat").await?;

        let bytes_stream = resp.bytes_stream();
        Ok(Box::pin(ndjson_chat_stream(bytes_stream)))
    }
}

/// Convert a non-2xx response into an `anyhow::Error` that includes Ollama's
/// response body (typically `{"error":"..."}`), which `reqwest::Response::error_for_status`
/// throws away. Trimmed and length-capped so we don't dump megabytes into logs.
async fn check_status(resp: reqwest::Response, endpoint: &str) -> Result<reqwest::Response> {
    if resp.status().is_success() {
        return Ok(resp);
    }
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    let trimmed = body.trim();
    if trimmed.is_empty() {
        anyhow::bail!("ollama {endpoint} returned {status}");
    }
    let snippet: String = trimmed.chars().take(512).collect();
    anyhow::bail!("ollama {endpoint} returned {status}: {snippet}");
}

fn ndjson_chat_stream<S>(stream: S) -> impl Stream<Item = Result<ChatChunk>> + Send
where
    S: Stream<Item = reqwest::Result<bytes::Bytes>> + Send + 'static,
{
    futures_util::stream::try_unfold(
        (Box::pin(stream), Vec::<u8>::new(), false),
        |(mut s, mut buf, mut eof)| async move {
            loop {
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=pos).collect();
                    let trimmed = trim_trailing_newline(&line);
                    if trimmed.is_empty() {
                        continue;
                    }
                    let chunk = parse_chat_chunk(trimmed)?;
                    return Ok(Some((chunk, (s, buf, eof))));
                }
                if eof {
                    if buf.is_empty() {
                        return Ok(None);
                    }
                    let chunk = parse_chat_chunk(&buf)?;
                    buf.clear();
                    return Ok(Some((chunk, (s, buf, eof))));
                }
                match s.next().await {
                    Some(Ok(b)) => buf.extend_from_slice(&b),
                    Some(Err(e)) => return Err(anyhow::Error::from(e)),
                    None => eof = true,
                }
            }
        },
    )
}

fn trim_trailing_newline(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    while end > 0 && (line[end - 1] == b'\n' || line[end - 1] == b'\r') {
        end -= 1;
    }
    &line[..end]
}

fn parse_chat_chunk(line: &[u8]) -> Result<ChatChunk> {
    let raw: ChatRawChunk = serde_json::from_slice(line)
        .with_context(|| format!("parsing ollama chunk: {}", String::from_utf8_lossy(line)))?;
    if let Some(err) = raw.error {
        anyhow::bail!("ollama error: {err}");
    }
    Ok(ChatChunk {
        content: raw.message.map(|m| m.content).unwrap_or_default(),
        done: raw.done,
        total_duration_ns: raw.total_duration,
        eval_count: raw.eval_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;

    fn bytes_iter(parts: Vec<&'static str>) -> impl Stream<Item = reqwest::Result<bytes::Bytes>> {
        stream::iter(parts.into_iter().map(|s| Ok(bytes::Bytes::from(s.as_bytes()))))
    }

    #[tokio::test]
    async fn parses_ndjson_split_across_packets() {
        let s = bytes_iter(vec![
            "{\"message\":{\"content\":\"Hel\"},\"done\":false}\n",
            "{\"message\":{\"content\":\"lo\"},\"done\":fa",
            "lse}\n",
            "{\"message\":{\"content\":\"\"},\"done\":true,\"total_duration\":123,\"eval_count\":42}\n",
        ]);
        let mut stream = Box::pin(ndjson_chat_stream(s));
        let mut chunks = vec![];
        while let Some(c) = stream.next().await {
            chunks.push(c.unwrap());
        }
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].content, "Hel");
        assert!(!chunks[0].done);
        assert_eq!(chunks[1].content, "lo");
        assert!(chunks[2].done);
        assert_eq!(chunks[2].total_duration_ns, Some(123));
        assert_eq!(chunks[2].eval_count, Some(42));
    }

    #[tokio::test]
    async fn handles_last_line_without_trailing_newline() {
        let s = bytes_iter(vec!["{\"message\":{\"content\":\"x\"},\"done\":true}"]);
        let mut stream = Box::pin(ndjson_chat_stream(s));
        let c = stream.next().await.unwrap().unwrap();
        assert_eq!(c.content, "x");
        assert!(c.done);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn surfaces_ollama_error_field() {
        let s = bytes_iter(vec!["{\"error\":\"model 'foo' not found\"}\n"]);
        let mut stream = Box::pin(ndjson_chat_stream(s));
        let err = stream.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("model 'foo' not found"));
    }
}
