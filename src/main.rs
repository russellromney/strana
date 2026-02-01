use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

mod auth;
mod backend;
mod config;
mod http;
mod journal;
mod protocol;
mod rewriter;
mod server;
mod session;
mod snapshot;
mod values;
mod wire;

use auth::TokenStore;
use backend::KuzuBackend;
use config::Config;
use server::AppState;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "strana=info".into()),
        )
        .init();

    let config = Config::parse();

    // Handle --generate-token: print a new token + hash and exit.
    if config.generate_token {
        let token = auth::generate_token();
        let hash = auth::hash_token(&token);
        println!("Token:  {token}");
        println!("Hash:   {hash}");
        return;
    }

    // Build TokenStore from config.
    let tokens = if let Some(ref t) = config.token {
        Arc::new(TokenStore::from_token(t))
    } else if let Some(ref path) = config.token_file {
        Arc::new(TokenStore::from_file(path).unwrap_or_else(|e| {
            eprintln!("Error loading token file: {e}");
            std::process::exit(1);
        }))
    } else {
        Arc::new(TokenStore::open())
    };

    if tokens.is_empty() {
        info!("Auth: open access (no tokens configured)");
    } else {
        info!("Auth: token authentication enabled");
    }

    // Ensure the data directory exists.
    std::fs::create_dir_all(&config.data_dir).expect("Failed to create data directory");

    // Handle --restore: restore from snapshot, replay journal, then exit.
    if config.restore {
        match snapshot::restore(&config.data_dir, config.snapshot.as_deref()) {
            Ok(()) => {
                info!("Restore completed successfully");
                return;
            }
            Err(e) => {
                eprintln!("Restore failed: {e}");
                std::process::exit(1);
            }
        }
    }

    let db_dir = config.data_dir.join("db");
    let db = KuzuBackend::open(&db_dir).expect("Failed to open Kuzu database");
    info!("Opened Kuzu database at {:?}", db_dir);

    // Conditionally start journal writer.
    let (journal_tx, journal_state) = if config.journal {
        let journal_dir = config.data_dir.join("journal");

        // Recover journal state from existing segments so the chain continues correctly.
        let (seq, hash) = journal::recover_journal_state(&journal_dir).unwrap_or_else(|e| {
            eprintln!("Failed to recover journal state: {e}");
            std::process::exit(1);
        });
        if seq > 0 {
            info!("Journal: recovered state seq={seq}, hash={:x?}", &hash[..4]);
        }

        let state = Arc::new(journal::JournalState::with_sequence_and_hash(seq, hash));
        let tx = journal::spawn_journal_writer(
            journal_dir,
            config.journal_segment_mb * 1024 * 1024,
            config.journal_fsync_ms,
            state.clone(),
        );
        info!("Journal: enabled (segment={}MB, fsync={}ms)", config.journal_segment_mb, config.journal_fsync_ms);
        (Some(tx), Some(state))
    } else {
        info!("Journal: disabled");
        (None, None)
    };

    let state = AppState {
        db: Arc::new(db),
        tokens,
        cursor_timeout: Duration::from_secs(30),
        journal: journal_tx,
        journal_state,
        data_dir: config.data_dir.clone(),
        snapshot_lock: Arc::new(std::sync::RwLock::new(())),
        retention_config: snapshot::RetentionConfig {
            daily: config.retain_daily,
            weekly: config.retain_weekly,
            monthly: config.retain_monthly,
        },
    };

    let app = Router::new()
        .route("/ws", get(server::ws_upgrade))
        .route("/v1/execute", post(http::execute_handler))
        .route("/v1/batch", post(http::batch_handler))
        .route("/v1/pipeline", post(http::pipeline_handler))
        .route("/v1/snapshot", post(http::snapshot_handler))
        .route("/health", get(|| async { "ok" }))
        .with_state(state);

    let addr = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("Failed to bind");
    info!("graphd listening on {addr}");

    axum::serve(listener, app).await.expect("Server error");
}
