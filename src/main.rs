use axum::routing::{get, post};
use axum::Router;
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

mod auth;
mod backend;
mod bolt;
mod config;
mod engine;
mod engine_manager;
mod graphj;
mod journal;
mod neo4j_http;
mod query;
mod rewriter;
mod snapshot;
#[allow(clippy::all)]
mod graphd;
mod values;

use auth::TokenStore;
use backend::Backend;
use config::Config;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "graphd=info".into()),
        )
        .init();

    let config = Config::parse();

    // Parse encryption key from hex if provided.
    let encryption_key = config
        .journal_encryption_key
        .as_ref()
        .map(|hex| {
            if hex.len() != 64 {
                eprintln!("Error: journal_encryption_key must be 64 hex characters (32 bytes)");
                std::process::exit(1);
            }
            let mut key = [0u8; 32];
            for i in 0..32 {
                key[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or_else(|_| {
                    eprintln!("Error: invalid hex in journal_encryption_key");
                    std::process::exit(1);
                });
            }
            key
        });

    if encryption_key.is_some() {
        info!("Journal encryption: enabled");
    }
    if config.journal_compress {
        info!(
            "Journal compression: enabled (level={})",
            config.journal_compress_level
        );
    }

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
        if let (Some(ref bucket), None) = (&config.s3_bucket, &config.snapshot) {
            let snapshots_dir = config.data_dir.join("snapshots");
            info!("Downloading latest snapshot from S3...");
            let key = snapshot::find_latest_snapshot_s3(bucket, &config.s3_prefix)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to find S3 snapshot: {e}");
                    std::process::exit(1);
                });
            let stem = key.trim_end_matches(".tar.zst");
            let seq_str = stem.rsplit('/').next().unwrap_or(stem);
            let snap_dir = snapshots_dir.join(seq_str);
            snapshot::download_snapshot_s3(bucket, &key, &snap_dir)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to download S3 snapshot: {e}");
                    std::process::exit(1);
                });
        }

        match snapshot::restore(&config.data_dir, config.snapshot.as_deref(), encryption_key) {
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
    let db = Backend::open(&db_dir).expect("Failed to open database");
    info!("Opened database at {:?}", db_dir);

    // Conditionally start journal writer.
    let (journal_tx, journal_state) = if config.journal {
        let journal_dir = config.data_dir.join("journal");
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
        info!(
            "Journal: enabled (segment={}MB, fsync={}ms)",
            config.journal_segment_mb, config.journal_fsync_ms
        );
        (Some(tx), Some(state))
    } else {
        info!("Journal: disabled");
        (None, None)
    };

    let db = Arc::new(db);

    // ── Engine ──
    let engine = Arc::new(engine::Engine::new(
        db.clone(),
        config.read_connections,
        journal_tx.clone(),
    ));
    info!("Engine: {} reader(s) + 1 writer", config.read_connections);

    // ── Bolt listener ──
    let bolt_state = bolt::BoltState {
        engine: engine.clone(),
        tokens: tokens.clone(),
        conn_semaphore: Arc::new(tokio::sync::Semaphore::new(
            config.bolt_max_connections as usize,
        )),
    };

    // ── Shutdown signal ──
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received");
        shutdown_tx.send(true).ok();
    });

    let bolt_addr = format!("{}:{}", config.bolt_host, config.bolt_port);
    let bolt_state_clone = bolt_state.clone();
    let bolt_shutdown = shutdown_rx.clone();
    let bolt_handle = tokio::spawn(async move {
        bolt::listen(bolt_addr, bolt_state_clone, bolt_shutdown).await;
    });

    // ── HTTP listener (Neo4j Query API + management) ──
    let http_state = neo4j_http::AppState {
        engine: engine.clone(),
        tokens,
        journal_state,
        data_dir: config.data_dir.clone(),
        retention_config: snapshot::RetentionConfig {
            daily: config.retain_daily,
            weekly: config.retain_weekly,
            monthly: config.retain_monthly,
        },
        s3_bucket: config.s3_bucket.clone(),
        s3_prefix: config.s3_prefix.clone(),
        transactions: Arc::new(std::sync::RwLock::new(HashMap::new())),
    };

    // ── Transaction reaper ──
    neo4j_http::spawn_tx_reaper(
        http_state.transactions.clone(),
        std::time::Duration::from_secs(config.tx_timeout_secs),
    );
    info!(
        "Transaction reaper: enabled (timeout={}s)",
        config.tx_timeout_secs
    );

    if http_state.s3_bucket.is_some() {
        info!(
            "S3 snapshots: enabled (bucket={})",
            http_state.s3_bucket.as_ref().unwrap()
        );
    }

    let app = Router::new()
        // Neo4j Query API
        .route("/db/{name}/query/v2", post(neo4j_http::query_handler))
        .route("/db/{name}/query/v2/tx", post(neo4j_http::begin_handler))
        .route(
            "/db/{name}/query/v2/tx/{id}",
            post(neo4j_http::run_in_tx_handler).delete(neo4j_http::rollback_handler),
        )
        .route(
            "/db/{name}/query/v2/tx/{id}/commit",
            post(neo4j_http::commit_handler),
        )
        // Management
        .route("/v1/snapshot", post(neo4j_http::snapshot_handler))
        .route("/health", get(|| async { "ok" }))
        .with_state(http_state);

    let http_addr = format!("{}:{}", config.http_host, config.http_port);
    let listener = tokio::net::TcpListener::bind(&http_addr)
        .await
        .expect("Failed to bind HTTP listener");
    info!("HTTP listening on {http_addr}");

    let http_shutdown = shutdown_rx.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            http_shutdown.clone().changed().await.ok();
            info!("HTTP listener shutting down");
        })
        .await
        .expect("Server error");

    // Wait for Bolt listener to finish.
    bolt_handle.await.ok();

    // Flush journal before exit.
    if let Some(ref jtx) = engine.journal() {
        info!("Flushing journal before exit...");
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel(1);
        if jtx.send(journal::JournalCommand::Flush(ack_tx)).is_ok() {
            ack_rx.recv().ok();
        }
        info!("Journal flushed");
    }

    info!("Shutdown complete");
}
