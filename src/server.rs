use axum::extract::ws::{Message, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::auth::TokenStore;
use crate::backend::Backend;
use crate::journal::{JournalSender, JournalState};
use crate::snapshot::RetentionConfig;
use std::path::PathBuf;
use std::sync::RwLock;
use crate::protocol::{ClientMessage, ServerMessage};
use crate::session::{self, SessionHandle, SessionOp};

/// Shared state for the WebSocket handler.
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Backend>,
    /// Token store for authenticating clients.
    pub tokens: Arc<TokenStore>,
    /// How long idle cursors survive before being swept.
    pub cursor_timeout: Duration,
    /// Journal writer channel (None if journaling disabled).
    pub journal: Option<JournalSender>,
    /// Journal shared state for snapshot coordination (None if journaling disabled).
    pub journal_state: Option<Arc<JournalState>>,
    /// Path to the database data directory.
    pub data_dir: PathBuf,
    /// Read lock for mutations, write lock for snapshots.
    /// Prevents mutations from executing + journaling while a snapshot is in progress.
    pub snapshot_lock: Arc<RwLock<()>>,
    /// GFS retention config for snapshot cleanup.
    pub retention_config: RetentionConfig,
    /// S3 bucket for snapshot uploads (None if disabled).
    pub s3_bucket: Option<String>,
    /// S3 key prefix for snapshots.
    pub s3_prefix: String,
}

/// Axum handler: upgrade HTTP to WebSocket.
pub async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

/// Dispatch an operation to the session worker and send the response over the WebSocket.
/// Returns `false` if the WebSocket send failed (caller should break the loop).
async fn dispatch(session: &SessionHandle, socket: &mut WebSocket, op: SessionOp) -> bool {
    let response = match session.request(op).await {
        Ok(msg) => msg,
        Err(e) => ServerMessage::Error {
            message: e,
            request_id: None,
        },
    };
    send_message(socket, &response).await.is_ok()
}

/// Handle a single WebSocket connection through its full lifecycle.
async fn handle_connection(mut socket: WebSocket, state: AppState) {
    // Step 1: Expect `hello` as the first message.
    match recv_message(&mut socket).await {
        Some(ClientMessage::Hello { token }) => {
            if state.tokens.is_empty() {
                info!("Client connected (open access)");
            } else if let Some(ref t) = token {
                match state.tokens.validate(t) {
                    Ok(label) => info!("Client authenticated as {label}"),
                    Err(()) => {
                        let _ = send_message(
                            &mut socket,
                            &ServerMessage::HelloError {
                                message: "Invalid token".into(),
                            },
                        )
                        .await;
                        return;
                    }
                }
            } else {
                let _ = send_message(
                    &mut socket,
                    &ServerMessage::HelloError {
                        message: "Invalid token".into(),
                    },
                )
                .await;
                return;
            }
        }
        Some(_) => {
            let _ = send_message(
                &mut socket,
                &ServerMessage::HelloError {
                    message: "First message must be hello".into(),
                },
            )
            .await;
            return;
        }
        None => return, // Client disconnected.
    };

    let _ = send_message(
        &mut socket,
        &ServerMessage::HelloOk {
            version: env!("CARGO_PKG_VERSION").into(),
        },
    )
    .await;

    // Spawn a per-session worker with its own database connection.
    let session = session::spawn_session(
        state.db.clone(),
        state.cursor_timeout,
        state.journal.clone(),
        state.snapshot_lock.clone(),
    );

    // Step 2: Message loop.
    loop {
        match recv_message(&mut socket).await {
            Some(ClientMessage::Execute { query, params, request_id, fetch_size }) => {
                if !dispatch(&session, &mut socket, SessionOp::Execute { query, params, request_id, fetch_size }).await {
                    break;
                }
            }
            Some(ClientMessage::Begin { mode, request_id }) => {
                if !dispatch(&session, &mut socket, SessionOp::Begin { mode, request_id }).await {
                    break;
                }
            }
            Some(ClientMessage::Commit { request_id }) => {
                if !dispatch(&session, &mut socket, SessionOp::Commit { request_id }).await {
                    break;
                }
            }
            Some(ClientMessage::Rollback { request_id }) => {
                if !dispatch(&session, &mut socket, SessionOp::Rollback { request_id }).await {
                    break;
                }
            }
            Some(ClientMessage::Batch { statements, request_id }) => {
                if !dispatch(&session, &mut socket, SessionOp::Batch { statements, request_id }).await {
                    break;
                }
            }
            Some(ClientMessage::Fetch { stream_id, request_id }) => {
                if !dispatch(&session, &mut socket, SessionOp::Fetch { stream_id, request_id }).await {
                    break;
                }
            }
            Some(ClientMessage::CloseStream { stream_id, request_id }) => {
                if !dispatch(&session, &mut socket, SessionOp::CloseStream { stream_id, request_id }).await {
                    break;
                }
            }
            Some(ClientMessage::Close) => {
                let _ = send_message(&mut socket, &ServerMessage::CloseOk).await;
                break;
            }
            Some(ClientMessage::Hello { .. }) => {
                let _ = send_message(
                    &mut socket,
                    &ServerMessage::Error {
                        message: "Already authenticated".into(),
                        request_id: None,
                    },
                )
                .await;
            }
            None => break, // Client disconnected.
        }
    }

    // session handle drops here → worker exits → connection drops → auto-rollback
    info!("Client disconnected");
}

/// Read and parse the next client message from the WebSocket (binary protobuf).
async fn recv_message(socket: &mut WebSocket) -> Option<ClientMessage> {
    loop {
        match socket.recv().await {
            Some(Ok(Message::Binary(bytes))) => {
                match crate::wire::decode_client_message(&bytes) {
                    Ok(msg) => return Some(msg),
                    Err(e) => {
                        warn!("Invalid protobuf from client: {e}");
                        let _ = send_message(
                            socket,
                            &ServerMessage::Error {
                                message: format!("Invalid message: {e}"),
                                request_id: None,
                            },
                        )
                        .await;
                        return None;
                    }
                }
            }
            Some(Ok(Message::Text(_))) => {
                warn!("Text frames not supported — use binary protobuf");
                let _ = send_message(
                    socket,
                    &ServerMessage::Error {
                        message: "Text encoding not supported — use binary protobuf".into(),
                        request_id: None,
                    },
                )
                .await;
                return None;
            }
            Some(Ok(Message::Close(_))) => return None,
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            Some(Err(e)) => {
                warn!("WebSocket error: {e}");
                return None;
            }
            None => return None,
        }
    }
}

/// Send a server message as binary protobuf over the WebSocket.
async fn send_message(
    socket: &mut WebSocket,
    msg: &ServerMessage,
) -> Result<(), axum::Error> {
    let bytes = crate::wire::encode_server_message(msg);
    socket.send(Message::Binary(bytes.into())).await
}
