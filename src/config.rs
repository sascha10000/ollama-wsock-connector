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

        let client_id_from_override = overrides.client_id.is_some();
        let client_id_from_file = file_cfg
            .as_ref()
            .and_then(|c| c.client.id.as_ref())
            .is_some();
        let client_id = overrides
            .client_id
            .or_else(|| file_cfg.as_ref().and_then(|c| c.client.id.clone()))
            .unwrap_or_else(|| format!("client-{}", uuid::Uuid::new_v4()));

        if !client_id_from_override && !client_id_from_file {
            if let Some(p) = path {
                if let Err(e) = persist_client_id(p, &client_id) {
                    tracing::warn!(
                        path = %p.display(),
                        error = ?e,
                        "could not persist generated client_id to config file; \
                         a new id will be generated on next start"
                    );
                }
            }
        }

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

fn persist_client_id(path: &Path, client_id: &str) -> Result<()> {
    let original = std::fs::read_to_string(path)
        .with_context(|| format!("reading config file {} for update", path.display()))?;
    let mut doc: toml_edit::DocumentMut = original
        .parse()
        .with_context(|| format!("re-parsing config file {} as editable TOML", path.display()))?;

    let client = doc
        .entry("client")
        .or_insert_with(|| toml_edit::Item::Table(toml_edit::Table::new()));
    let table = client
        .as_table_mut()
        .ok_or_else(|| anyhow::anyhow!("[client] in config file is not a table"))?;
    table.set_implicit(false);
    table["id"] = toml_edit::value(client_id);

    std::fs::write(path, doc.to_string())
        .with_context(|| format!("writing updated config file {}", path.display()))?;
    tracing::info!(
        path = %path.display(),
        client_id,
        "wrote generated client_id back to config file"
    );
    Ok(())
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

    #[test]
    fn missing_client_id_is_persisted_to_config_file() {
        let path = std::env::temp_dir()
            .join(format!("owsc-test-{}.toml", uuid::Uuid::new_v4()));
        let original = "\
# user-written comment that must survive
[websocket]
url = \"ws://example.test\"

[client]
log_level = \"debug\"
";
        std::fs::write(&path, original).unwrap();

        let cfg = Config::load(Some(&path), ConfigOverrides::default()).unwrap();
        assert!(cfg.client_id.starts_with("client-"));

        let updated = std::fs::read_to_string(&path).unwrap();
        assert!(
            updated.contains("user-written comment that must survive"),
            "comment was lost:\n{updated}"
        );
        assert!(
            updated.contains(&format!("id = \"{}\"", cfg.client_id)),
            "client_id was not written back:\n{updated}"
        );

        // Re-loading without overrides must yield the same id (no second rewrite).
        let cfg2 = Config::load(Some(&path), ConfigOverrides::default()).unwrap();
        assert_eq!(cfg.client_id, cfg2.client_id);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn cli_override_does_not_persist_to_file() {
        let path = std::env::temp_dir()
            .join(format!("owsc-test-{}.toml", uuid::Uuid::new_v4()));
        let original = "[websocket]\nurl = \"ws://example.test\"\n";
        std::fs::write(&path, original).unwrap();

        let overrides = ConfigOverrides {
            client_id: Some("ephemeral-cli-id".into()),
            ..ConfigOverrides::default()
        };
        let cfg = Config::load(Some(&path), overrides).unwrap();
        assert_eq!(cfg.client_id, "ephemeral-cli-id");

        let after = std::fs::read_to_string(&path).unwrap();
        assert_eq!(after, original, "file should be untouched when CLI overrides client_id");

        let _ = std::fs::remove_file(&path);
    }
}
