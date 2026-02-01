use crate::values::GraphValue;
use serde::{Deserialize, Serialize};

/// A single statement within a batch request.
#[derive(Debug, Clone, Deserialize)]
pub struct BatchStatement {
    pub query: String,
    pub params: Option<serde_json::Value>,
}

/// Messages sent from client to server.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Hello { token: Option<String> },
    Execute {
        query: String,
        params: Option<serde_json::Value>,
        request_id: Option<String>,
        fetch_size: Option<u64>,
    },
    Begin {
        mode: Option<String>,
        request_id: Option<String>,
    },
    Commit {
        request_id: Option<String>,
    },
    Rollback {
        request_id: Option<String>,
    },
    Batch {
        statements: Vec<BatchStatement>,
        request_id: Option<String>,
    },
    Fetch {
        stream_id: u64,
        request_id: Option<String>,
    },
    CloseStream {
        stream_id: u64,
        request_id: Option<String>,
    },
    Close,
}

/// A single entry in a batch result.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchResultEntry {
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<GraphValue>>,
        timing_ms: f64,
    },
    Error {
        message: String,
    },
}

/// Messages sent from server to client.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    HelloOk { version: String },
    HelloError { message: String },
    Result {
        columns: Vec<String>,
        rows: Vec<Vec<GraphValue>>,
        timing_ms: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stream_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        has_more: Option<bool>,
    },
    Error {
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    BeginOk {
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    CommitOk {
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    RollbackOk {
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    BatchResult {
        results: Vec<BatchResultEntry>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    CloseStreamOk {
        stream_id: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
    },
    CloseOk,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hello() {
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type": "hello", "token": "secret"}"#).unwrap();
        match msg {
            ClientMessage::Hello { token } => assert_eq!(token, Some("secret".to_string())),
            _ => panic!("expected Hello"),
        }
    }

    #[test]
    fn test_parse_hello_no_token() {
        let msg: ClientMessage = serde_json::from_str(r#"{"type": "hello"}"#).unwrap();
        match msg {
            ClientMessage::Hello { token } => assert!(token.is_none()),
            _ => panic!("expected Hello"),
        }
    }

    #[test]
    fn test_parse_execute() {
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type": "execute", "query": "MATCH (n) RETURN n", "params": {"x": 1}}"#,
        )
        .unwrap();
        match msg {
            ClientMessage::Execute { query, params, request_id, fetch_size } => {
                assert_eq!(query, "MATCH (n) RETURN n");
                assert!(params.is_some());
                assert!(request_id.is_none());
                assert!(fetch_size.is_none());
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_parse_execute_with_request_id() {
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type": "execute", "query": "RETURN 1", "request_id": "req-42"}"#,
        )
        .unwrap();
        match msg {
            ClientMessage::Execute { query, request_id, .. } => {
                assert_eq!(query, "RETURN 1");
                assert_eq!(request_id, Some("req-42".to_string()));
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_parse_close() {
        let msg: ClientMessage = serde_json::from_str(r#"{"type": "close"}"#).unwrap();
        assert!(matches!(msg, ClientMessage::Close));
    }

    #[test]
    fn test_serialize_hello_ok() {
        let msg = ServerMessage::HelloOk {
            version: "0.1.0".into(),
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "hello_ok");
        assert_eq!(json["version"], "0.1.0");
    }

    #[test]
    fn test_serialize_result() {
        let msg = ServerMessage::Result {
            columns: vec!["name".into()],
            rows: vec![vec![GraphValue::String("Alice".into())]],
            timing_ms: 1.5,
            request_id: None,
            stream_id: None,
            has_more: None,
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "result");
        assert_eq!(json["columns"], serde_json::json!(["name"]));
        assert_eq!(json["rows"], serde_json::json!([["Alice"]]));
        assert_eq!(json["timing_ms"], 1.5);
        assert!(json.get("request_id").is_none());
        assert!(json.get("stream_id").is_none());
        assert!(json.get("has_more").is_none());
    }

    #[test]
    fn test_serialize_result_with_request_id() {
        let msg = ServerMessage::Result {
            columns: vec![],
            rows: vec![],
            timing_ms: 0.1,
            request_id: Some("req-1".into()),
            stream_id: None,
            has_more: None,
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["request_id"], "req-1");
    }

    #[test]
    fn test_serialize_error() {
        let msg = ServerMessage::Error {
            message: "bad query".into(),
            request_id: None,
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "error");
        assert_eq!(json["message"], "bad query");
        assert!(json.get("request_id").is_none());
    }

    #[test]
    fn test_serialize_error_with_request_id() {
        let msg = ServerMessage::Error {
            message: "fail".into(),
            request_id: Some("req-err".into()),
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["request_id"], "req-err");
    }

    #[test]
    fn test_unknown_fields_ignored() {
        // Per spec: "Clients and servers MUST ignore unrecognized fields."
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type": "hello", "token": "x", "stream_id": 0, "encoding": "json"}"#,
        )
        .unwrap();
        assert!(matches!(msg, ClientMessage::Hello { .. }));
    }

    #[test]
    fn test_parse_begin() {
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type": "begin"}"#).unwrap();
        match msg {
            ClientMessage::Begin { mode, request_id } => {
                assert!(mode.is_none());
                assert!(request_id.is_none());
            }
            _ => panic!("expected Begin"),
        }
    }

    #[test]
    fn test_parse_begin_read() {
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type": "begin", "mode": "read", "request_id": "r1"}"#)
                .unwrap();
        match msg {
            ClientMessage::Begin { mode, request_id } => {
                assert_eq!(mode, Some("read".to_string()));
                assert_eq!(request_id, Some("r1".to_string()));
            }
            _ => panic!("expected Begin"),
        }
    }

    #[test]
    fn test_parse_commit() {
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type": "commit", "request_id": "c1"}"#).unwrap();
        match msg {
            ClientMessage::Commit { request_id } => {
                assert_eq!(request_id, Some("c1".to_string()));
            }
            _ => panic!("expected Commit"),
        }
    }

    #[test]
    fn test_parse_rollback() {
        let msg: ClientMessage =
            serde_json::from_str(r#"{"type": "rollback"}"#).unwrap();
        assert!(matches!(msg, ClientMessage::Rollback { .. }));
    }

    #[test]
    fn test_parse_batch() {
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type": "batch", "statements": [{"query": "RETURN 1"}, {"query": "RETURN 2", "params": {"x": 1}}]}"#,
        )
        .unwrap();
        match msg {
            ClientMessage::Batch {
                statements,
                request_id,
            } => {
                assert_eq!(statements.len(), 2);
                assert_eq!(statements[0].query, "RETURN 1");
                assert!(statements[0].params.is_none());
                assert_eq!(statements[1].query, "RETURN 2");
                assert!(statements[1].params.is_some());
                assert!(request_id.is_none());
            }
            _ => panic!("expected Batch"),
        }
    }

    #[test]
    fn test_serialize_begin_ok() {
        let msg = ServerMessage::BeginOk {
            request_id: Some("b1".into()),
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "begin_ok");
        assert_eq!(json["request_id"], "b1");
    }

    #[test]
    fn test_serialize_commit_ok() {
        let msg = ServerMessage::CommitOk { request_id: None };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "commit_ok");
        assert!(json.get("request_id").is_none());
    }

    #[test]
    fn test_serialize_rollback_ok() {
        let msg = ServerMessage::RollbackOk { request_id: None };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "rollback_ok");
    }

    #[test]
    fn test_serialize_batch_result() {
        let msg = ServerMessage::BatchResult {
            results: vec![
                BatchResultEntry::Result {
                    columns: vec!["x".into()],
                    rows: vec![vec![GraphValue::Int(1)]],
                    timing_ms: 0.5,
                },
                BatchResultEntry::Error {
                    message: "fail".into(),
                },
            ],
            request_id: Some("batch-1".into()),
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "batch_result");
        assert_eq!(json["request_id"], "batch-1");
        assert_eq!(json["results"].as_array().unwrap().len(), 2);
        assert_eq!(json["results"][0]["type"], "result");
        assert_eq!(json["results"][1]["type"], "error");
    }

    #[test]
    fn test_parse_execute_with_fetch_size() {
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type": "execute", "query": "MATCH (n) RETURN n", "fetch_size": 100}"#,
        )
        .unwrap();
        match msg {
            ClientMessage::Execute { query, fetch_size, .. } => {
                assert_eq!(query, "MATCH (n) RETURN n");
                assert_eq!(fetch_size, Some(100));
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_parse_fetch() {
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type": "fetch", "stream_id": 42, "request_id": "f1"}"#,
        )
        .unwrap();
        match msg {
            ClientMessage::Fetch { stream_id, request_id } => {
                assert_eq!(stream_id, 42);
                assert_eq!(request_id, Some("f1".to_string()));
            }
            _ => panic!("expected Fetch"),
        }
    }

    #[test]
    fn test_parse_close_stream() {
        let msg: ClientMessage = serde_json::from_str(
            r#"{"type": "close_stream", "stream_id": 7}"#,
        )
        .unwrap();
        match msg {
            ClientMessage::CloseStream { stream_id, request_id } => {
                assert_eq!(stream_id, 7);
                assert!(request_id.is_none());
            }
            _ => panic!("expected CloseStream"),
        }
    }

    #[test]
    fn test_serialize_result_with_streaming() {
        let msg = ServerMessage::Result {
            columns: vec!["x".into()],
            rows: vec![vec![GraphValue::Int(1)]],
            timing_ms: 0.5,
            request_id: Some("r1".into()),
            stream_id: Some(3),
            has_more: Some(true),
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["stream_id"], 3);
        assert_eq!(json["has_more"], true);
    }

    #[test]
    fn test_serialize_close_stream_ok() {
        let msg = ServerMessage::CloseStreamOk {
            stream_id: 5,
            request_id: Some("cs1".into()),
        };
        let json: serde_json::Value = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "close_stream_ok");
        assert_eq!(json["stream_id"], 5);
        assert_eq!(json["request_id"], "cs1");
    }
}
