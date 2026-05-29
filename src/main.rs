use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing_subscriber::{fmt, EnvFilter};

mod config;
mod ollama;
mod protocol;
mod session;

use config::{Config, ConfigOverrides, DEFAULT_CONFIG_PATH};

#[derive(Parser, Debug)]
#[command(version, about = "Ollama WebSocket bridge client")]
struct Cli {
    /// Path to a TOML config file. If omitted, ./config.toml is loaded when present.
    #[arg(long, env = "OWSC_CONFIG")]
    config: Option<PathBuf>,

    /// WebSocket server URL (e.g. wss://server.example.com/path). Overrides the config file.
    #[arg(long, env = "OWSC_WS_URL")]
    ws_url: Option<String>,

    /// Optional Authorization header sent on the WS upgrade request.
    #[arg(long, env = "OWSC_WS_AUTH_HEADER")]
    ws_auth_header: Option<String>,

    /// Local Ollama base URL. Defaults to http://127.0.0.1:11434.
    #[arg(long, env = "OWSC_OLLAMA_URL")]
    ollama_url: Option<String>,

    /// Identifier this client reports to the server in the hello message.
    #[arg(long, env = "OWSC_CLIENT_ID")]
    client_id: Option<String>,

    /// Tracing filter (e.g. info, debug, ollama_wsock_connector=debug).
    #[arg(long, env = "OWSC_LOG_LEVEL")]
    log_level: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let config_path = cli.config.or_else(|| {
        let p = PathBuf::from(DEFAULT_CONFIG_PATH);
        if p.exists() {
            Some(p)
        } else {
            None
        }
    });

    let overrides = ConfigOverrides {
        ws_url: cli.ws_url,
        ws_auth_header: cli.ws_auth_header,
        ollama_url: cli.ollama_url,
        client_id: cli.client_id,
        log_level: cli.log_level,
    };
    let config = Config::load(config_path.as_deref(), overrides)?;

    init_tracing(&config.log_level);
    tracing::info!(
        client_id = %config.client_id,
        ws_url = %config.ws_url,
        ollama_url = %config.ollama_url,
        version = env!("CARGO_PKG_VERSION"),
        "starting ollama-wsock-connector"
    );

    run_reconnect_loop(config).await
}

fn init_tracing(level: &str) {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level))
        .unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).init();
}

async fn run_reconnect_loop(config: Config) -> Result<()> {
    const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    const STABLE_AFTER: Duration = Duration::from_secs(30);

    let mut backoff = INITIAL_BACKOFF;
    loop {
        let started = Instant::now();
        let outcome = tokio::select! {
            r = session::run_session(&config) => Outcome::Ended(r),
            _ = tokio::signal::ctrl_c() => Outcome::Interrupted,
        };

        match outcome {
            Outcome::Interrupted => {
                tracing::info!("ctrl-c received, exiting");
                return Ok(());
            }
            Outcome::Ended(Ok(())) => {
                tracing::info!("session ended");
            }
            Outcome::Ended(Err(e)) => {
                tracing::warn!(error = ?e, "session ended with error");
            }
        }

        if started.elapsed() >= STABLE_AFTER {
            backoff = INITIAL_BACKOFF;
        }
        tracing::info!("reconnecting in {:?}", backoff);

        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("ctrl-c received while waiting to reconnect, exiting");
                return Ok(());
            }
        }
        backoff = (backoff.saturating_mul(2)).min(MAX_BACKOFF);
    }
}

enum Outcome {
    Ended(Result<()>),
    Interrupted,
}
