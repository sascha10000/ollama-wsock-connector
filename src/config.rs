use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;
use url::Url;

#[derive(Debug, Clone, Deserialize)]
struct FileConfig {
    websocket: WebsocketSection,
    #[serde(default)]
    ollama: OllamaSection,
    #[serde(default)]
    client: ClientSection,
}

#[derive(Debug, Clone, Deserialize)]
struct WebsocketSection {
    url: String,
    #[serde(default)]
    auth_header: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct OllamaSection {
    #[serde(default)]
    url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ClientSection {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    log_level: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub ws_url: Url,
    pub ws_auth_header: Option<String>,
    pub ollama_url: Url,
    pub client_id: String,
    pub log_level: String,
}

#[derive(Debug, Default)]
pub struct ConfigOverrides {
    pub ws_url: Option<String>,
    pub ws_auth_header: Option<String>,
    pub ollama_url: Option<String>,
    pub client_id: Option<String>,
    pub log_level: Option<String>,
}

const DEFAULT_OLLAMA_URL: &str = "http://127.0.0.1:11434";
const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_CONFIG_PATH: &str = "config.toml";

impl Config {
    pub fn load(path: Option<&Path>, overrides: ConfigOverrides) -> Result<Self> {
        let file_cfg = match path {
            Some(p) => {
                let s = std::fs::read_to_string(p)
                    .with_context(|| format!("reading config file {}", p.display()))?;
                Some(
                    toml::from_str::<FileConfig>(&s)
                        .with_context(|| format!("parsing config file {}", p.display()))?,
                )
            }
            None => None,
        };

        let ws_url_str = overrides
            .ws_url
            .or_else(|| file_cfg.as_ref().map(|c| c.websocket.url.clone()))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "websocket url not configured (set [websocket].url in config.toml or --ws-url)"
                )
            })?;

        let ollama_url_str = overrides
            .ollama_url
            .or_else(|| file_cfg.as_ref().and_then(|c| c.ollama.url.clone()))
            .unwrap_or_else(|| DEFAULT_OLLAMA_URL.to_string());

        let client_id = overrides
            .client_id
            .or_else(|| file_cfg.as_ref().and_then(|c| c.client.id.clone()))
            .unwrap_or_else(|| format!("client-{}", uuid::Uuid::new_v4()));

        let log_level = overrides
            .log_level
            .or_else(|| file_cfg.as_ref().and_then(|c| c.client.log_level.clone()))
            .unwrap_or_else(|| DEFAULT_LOG_LEVEL.to_string());

        let ws_auth_header = overrides
            .ws_auth_header
            .or_else(|| file_cfg.as_ref().and_then(|c| c.websocket.auth_header.clone()));

        Ok(Config {
            ws_url: Url::parse(&ws_url_str).context("parsing websocket url")?,
            ws_auth_header,
            ollama_url: Url::parse(&ollama_url_str).context("parsing ollama url")?,
            client_id,
            log_level,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_overrides_win() {
        let overrides = ConfigOverrides {
            ws_url: Some("ws://override".into()),
            ollama_url: Some("http://1.2.3.4:11434".into()),
            client_id: Some("cli-client".into()),
            log_level: Some("debug".into()),
            ws_auth_header: Some("Bearer cli".into()),
        };
        let cfg = Config::load(None, overrides).unwrap();
        assert_eq!(cfg.ws_url.as_str(), "ws://override/");
        assert_eq!(cfg.ollama_url.as_str(), "http://1.2.3.4:11434/");
        assert_eq!(cfg.client_id, "cli-client");
        assert_eq!(cfg.log_level, "debug");
        assert_eq!(cfg.ws_auth_header.as_deref(), Some("Bearer cli"));
    }

    #[test]
    fn missing_ws_url_errors() {
        let err = Config::load(None, ConfigOverrides::default()).unwrap_err();
        assert!(err.to_string().contains("websocket url not configured"));
    }
}
