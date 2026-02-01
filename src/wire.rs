use prost::Message;

use crate::http::HttpResult;
use crate::protocol;
use crate::values;

pub mod proto {
    include!("strana.rs");
}

// ─── Encode / Decode ───

pub fn encode_server_message(msg: &protocol::ServerMessage) -> Vec<u8> {
    server_message_to_proto(msg).encode_to_vec()
}

pub fn decode_client_message(bytes: &[u8]) -> Result<protocol::ClientMessage, String> {
    let proto_msg =
        proto::ClientMessage::decode(bytes).map_err(|e| format!("Failed to decode protobuf: {e}"))?;
    proto_to_client_message(proto_msg)
}

// ─── Proto → Internal ───

fn proto_to_client_message(msg: proto::ClientMessage) -> Result<protocol::ClientMessage, String> {
    use proto::client_message::Msg;
    match msg.msg.ok_or("Empty client message")? {
        Msg::Hello(h) => Ok(protocol::ClientMessage::Hello { token: h.token }),
        Msg::Execute(e) => Ok(protocol::ClientMessage::Execute {
            query: e.query,
            params: proto_params_to_json(&e.params),
            request_id: e.request_id,
            fetch_size: e.fetch_size,
        }),
        Msg::Begin(b) => Ok(protocol::ClientMessage::Begin {
            mode: b.mode,
            request_id: b.request_id,
        }),
        Msg::Commit(c) => Ok(protocol::ClientMessage::Commit {
            request_id: c.request_id,
        }),
        Msg::Rollback(r) => Ok(protocol::ClientMessage::Rollback {
            request_id: r.request_id,
        }),
        Msg::Batch(b) => {
            let statements = b
                .statements
                .into_iter()
                .map(|s| protocol::BatchStatement {
                    query: s.query,
                    params: proto_params_to_json(&s.params),
                })
                .collect();
            Ok(protocol::ClientMessage::Batch {
                statements,
                request_id: b.request_id,
            })
        }
        Msg::Fetch(f) => Ok(protocol::ClientMessage::Fetch {
            stream_id: f.stream_id,
            request_id: f.request_id,
        }),
        Msg::CloseStream(cs) => Ok(protocol::ClientMessage::CloseStream {
            stream_id: cs.stream_id,
            request_id: cs.request_id,
        }),
        Msg::Close(_) => Ok(protocol::ClientMessage::Close),
    }
}

fn proto_params_to_json(params: &[proto::MapEntry]) -> Option<serde_json::Value> {
    if params.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for entry in params {
        let value = match entry.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(proto::graph_value::Value::NullValue(_)) | None => serde_json::Value::Null,
            Some(proto::graph_value::Value::BoolValue(b)) => serde_json::Value::Bool(*b),
            Some(proto::graph_value::Value::IntValue(i)) => serde_json::json!(*i),
            Some(proto::graph_value::Value::FloatValue(f)) => serde_json::json!(*f),
            Some(proto::graph_value::Value::StringValue(s)) => {
                serde_json::Value::String(s.clone())
            }
            _ => serde_json::Value::Null,
        };
        map.insert(entry.key.clone(), value);
    }
    Some(serde_json::Value::Object(map))
}

// ─── Internal → Proto ───

fn server_message_to_proto(msg: &protocol::ServerMessage) -> proto::ServerMessage {
    use proto::server_message::Msg;
    let variant = match msg {
        protocol::ServerMessage::HelloOk { version } => Msg::HelloOk(proto::HelloOk {
            version: version.clone(),
        }),
        protocol::ServerMessage::HelloError { message } => {
            Msg::HelloError(proto::HelloError {
                message: message.clone(),
            })
        }
        protocol::ServerMessage::Result {
            columns,
            rows,
            timing_ms,
            request_id,
            stream_id,
            has_more,
        } => Msg::Result(proto::Result {
            columns: columns.clone(),
            rows: rows_to_proto(rows),
            timing_ms: *timing_ms,
            request_id: request_id.clone(),
            stream_id: *stream_id,
            has_more: *has_more,
        }),
        protocol::ServerMessage::Error {
            message,
            request_id,
        } => Msg::Error(proto::Error {
            message: message.clone(),
            request_id: request_id.clone(),
        }),
        protocol::ServerMessage::BeginOk { request_id } => Msg::BeginOk(proto::BeginOk {
            request_id: request_id.clone(),
        }),
        protocol::ServerMessage::CommitOk { request_id } => Msg::CommitOk(proto::CommitOk {
            request_id: request_id.clone(),
        }),
        protocol::ServerMessage::RollbackOk { request_id } => {
            Msg::RollbackOk(proto::RollbackOk {
                request_id: request_id.clone(),
            })
        }
        protocol::ServerMessage::BatchResult {
            results,
            request_id,
        } => Msg::BatchResult(proto::BatchResult {
            results: results.iter().map(batch_entry_to_proto).collect(),
            request_id: request_id.clone(),
        }),
        protocol::ServerMessage::CloseStreamOk {
            stream_id,
            request_id,
        } => Msg::CloseStreamOk(proto::CloseStreamOk {
            stream_id: *stream_id,
            request_id: request_id.clone(),
        }),
        protocol::ServerMessage::CloseOk => Msg::CloseOk(proto::CloseOk {}),
    };
    proto::ServerMessage {
        msg: Some(variant),
    }
}

fn rows_to_proto(rows: &[Vec<values::GraphValue>]) -> Vec<proto::Row> {
    rows.iter()
        .map(|row| proto::Row {
            values: row.iter().map(graph_value_to_proto).collect(),
        })
        .collect()
}

fn batch_entry_to_proto(entry: &protocol::BatchResultEntry) -> proto::BatchResultEntry {
    let variant = match entry {
        protocol::BatchResultEntry::Result {
            columns,
            rows,
            timing_ms,
        } => proto::batch_result_entry::Entry::Result(proto::Result {
            columns: columns.clone(),
            rows: rows_to_proto(rows),
            timing_ms: *timing_ms,
            request_id: None,
            stream_id: None,
            has_more: None,
        }),
        protocol::BatchResultEntry::Error { message } => {
            proto::batch_result_entry::Entry::Error(proto::Error {
                message: message.clone(),
                request_id: None,
            })
        }
    };
    proto::BatchResultEntry {
        entry: Some(variant),
    }
}

fn graph_value_to_proto(v: &values::GraphValue) -> proto::GraphValue {
    use proto::graph_value::Value;
    let value = match v {
        values::GraphValue::Null => Value::NullValue(proto::NullValue {}),
        values::GraphValue::Bool(b) => Value::BoolValue(*b),
        values::GraphValue::Int(i) => Value::IntValue(*i),
        values::GraphValue::Float(f) => Value::FloatValue(*f),
        values::GraphValue::String(s) => Value::StringValue(s.clone()),
        values::GraphValue::List(items) => Value::ListValue(proto::ListValue {
            values: items.iter().map(graph_value_to_proto).collect(),
        }),
        values::GraphValue::Map(map) => Value::MapValue(proto::MapValue {
            entries: map
                .iter()
                .map(|(k, v)| proto::MapEntry {
                    key: k.clone(),
                    value: Some(graph_value_to_proto(v)),
                })
                .collect(),
        }),
        values::GraphValue::Tagged(tagged) => match tagged {
            values::TaggedValue::Node {
                id,
                label,
                properties,
            } => Value::NodeValue(proto::NodeValue {
                id: Some(proto::InternalId {
                    table: id.table,
                    offset: id.offset,
                }),
                label: label.clone(),
                properties: properties
                    .iter()
                    .map(|(k, v)| proto::MapEntry {
                        key: k.clone(),
                        value: Some(graph_value_to_proto(v)),
                    })
                    .collect(),
            }),
            values::TaggedValue::Rel {
                id,
                label,
                src,
                dst,
                properties,
            } => Value::RelValue(proto::RelValue {
                id: Some(proto::InternalId {
                    table: id.table,
                    offset: id.offset,
                }),
                label: label.clone(),
                src: Some(proto::InternalId {
                    table: src.table,
                    offset: src.offset,
                }),
                dst: Some(proto::InternalId {
                    table: dst.table,
                    offset: dst.offset,
                }),
                properties: properties
                    .iter()
                    .map(|(k, v)| proto::MapEntry {
                        key: k.clone(),
                        value: Some(graph_value_to_proto(v)),
                    })
                    .collect(),
            }),
            values::TaggedValue::Path { nodes, rels } => Value::PathValue(proto::PathValue {
                nodes: nodes.iter().map(graph_value_to_proto).collect(),
                rels: rels.iter().map(graph_value_to_proto).collect(),
            }),
            values::TaggedValue::Union { tag, value } => {
                Value::UnionValue(Box::new(proto::UnionValue {
                    tag: tag.clone(),
                    value: Some(Box::new(graph_value_to_proto(value))),
                }))
            }
        },
    };
    proto::GraphValue {
        value: Some(value),
    }
}

// ─── HTTP protobuf support ───

pub fn http_result_to_proto(result: &HttpResult) -> Vec<u8> {
    let msg = match result {
        HttpResult::Result {
            columns,
            rows,
            timing_ms,
        } => proto::ServerMessage {
            msg: Some(proto::server_message::Msg::Result(proto::Result {
                columns: columns.clone(),
                rows: rows_to_proto(rows),
                timing_ms: *timing_ms,
                request_id: None,
                stream_id: None,
                has_more: None,
            })),
        },
        HttpResult::Error { message } => proto::ServerMessage {
            msg: Some(proto::server_message::Msg::Error(proto::Error {
                message: message.clone(),
                request_id: None,
            })),
        },
        HttpResult::BatchResult { results } => proto::ServerMessage {
            msg: Some(proto::server_message::Msg::BatchResult(
                proto::BatchResult {
                    results: results.iter().map(batch_entry_to_proto).collect(),
                    request_id: None,
                },
            )),
        },
        HttpResult::PipelineResult { results } => proto::ServerMessage {
            msg: Some(proto::server_message::Msg::BatchResult(
                proto::BatchResult {
                    results: results.iter().map(batch_entry_to_proto).collect(),
                    request_id: None,
                },
            )),
        },
    };
    msg.encode_to_vec()
}

pub fn decode_execute_request(
    bytes: &[u8],
) -> Result<(String, Option<serde_json::Value>), String> {
    let exec =
        proto::Execute::decode(bytes).map_err(|e| format!("Failed to decode protobuf: {e}"))?;
    Ok((exec.query, proto_params_to_json(&exec.params)))
}

pub fn decode_batch_request(
    bytes: &[u8],
) -> Result<Vec<crate::http::StatementEntry>, String> {
    let batch =
        proto::Batch::decode(bytes).map_err(|e| format!("Failed to decode protobuf: {e}"))?;
    Ok(batch
        .statements
        .into_iter()
        .map(|s| crate::http::StatementEntry {
            query: s.query,
            params: proto_params_to_json(&s.params),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::{GraphValue, InternalId, TaggedValue};
    use std::collections::HashMap;

    #[test]
    fn test_roundtrip_hello() {
        let proto_msg = proto::ClientMessage {
            msg: Some(proto::client_message::Msg::Hello(proto::Hello {
                token: Some("secret".into()),
            })),
        };
        let bytes = proto_msg.encode_to_vec();
        let internal = decode_client_message(&bytes).unwrap();
        match internal {
            protocol::ClientMessage::Hello { token } => {
                assert_eq!(token, Some("secret".to_string()));
            }
            _ => panic!("expected Hello"),
        }
    }

    #[test]
    fn test_roundtrip_execute() {
        let proto_msg = proto::ClientMessage {
            msg: Some(proto::client_message::Msg::Execute(proto::Execute {
                query: "RETURN $x".into(),
                params: vec![proto::MapEntry {
                    key: "x".into(),
                    value: Some(proto::GraphValue {
                        value: Some(proto::graph_value::Value::IntValue(42)),
                    }),
                }],
                request_id: Some("r1".into()),
                fetch_size: Some(10),
            })),
        };
        let bytes = proto_msg.encode_to_vec();
        let internal = decode_client_message(&bytes).unwrap();
        match internal {
            protocol::ClientMessage::Execute {
                query,
                params,
                request_id,
                fetch_size,
            } => {
                assert_eq!(query, "RETURN $x");
                assert_eq!(params.unwrap()["x"], 42);
                assert_eq!(request_id, Some("r1".into()));
                assert_eq!(fetch_size, Some(10));
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_roundtrip_batch() {
        let proto_msg = proto::ClientMessage {
            msg: Some(proto::client_message::Msg::Batch(proto::Batch {
                statements: vec![
                    proto::BatchStatement {
                        query: "RETURN 1".into(),
                        params: vec![],
                    },
                    proto::BatchStatement {
                        query: "RETURN $x".into(),
                        params: vec![proto::MapEntry {
                            key: "x".into(),
                            value: Some(proto::GraphValue {
                                value: Some(proto::graph_value::Value::StringValue("hi".into())),
                            }),
                        }],
                    },
                ],
                request_id: Some("b1".into()),
            })),
        };
        let bytes = proto_msg.encode_to_vec();
        let internal = decode_client_message(&bytes).unwrap();
        match internal {
            protocol::ClientMessage::Batch {
                statements,
                request_id,
            } => {
                assert_eq!(statements.len(), 2);
                assert_eq!(statements[0].query, "RETURN 1");
                assert!(statements[0].params.is_none());
                assert_eq!(statements[1].query, "RETURN $x");
                assert_eq!(statements[1].params.as_ref().unwrap()["x"], "hi");
                assert_eq!(request_id, Some("b1".into()));
            }
            _ => panic!("expected Batch"),
        }
    }

    #[test]
    fn test_roundtrip_server_result() {
        let msg = protocol::ServerMessage::Result {
            columns: vec!["name".into(), "age".into()],
            rows: vec![vec![
                GraphValue::String("Alice".into()),
                GraphValue::Int(30),
            ]],
            timing_ms: 1.5,
            request_id: Some("r1".into()),
            stream_id: Some(3),
            has_more: Some(true),
        };
        let bytes = encode_server_message(&msg);
        let decoded = proto::ServerMessage::decode(bytes.as_slice()).unwrap();
        match decoded.msg.unwrap() {
            proto::server_message::Msg::Result(r) => {
                assert_eq!(r.columns, vec!["name", "age"]);
                assert_eq!(r.rows.len(), 1);
                assert_eq!(r.rows[0].values.len(), 2);
                assert_eq!(
                    r.rows[0].values[0].value,
                    Some(proto::graph_value::Value::StringValue("Alice".into()))
                );
                assert_eq!(
                    r.rows[0].values[1].value,
                    Some(proto::graph_value::Value::IntValue(30))
                );
                assert_eq!(r.timing_ms, 1.5);
                assert_eq!(r.request_id, Some("r1".into()));
                assert_eq!(r.stream_id, Some(3));
                assert_eq!(r.has_more, Some(true));
            }
            other => panic!("expected Result, got {other:?}"),
        }
    }

    #[test]
    fn test_roundtrip_server_error() {
        let msg = protocol::ServerMessage::Error {
            message: "bad query".into(),
            request_id: Some("e1".into()),
        };
        let bytes = encode_server_message(&msg);
        let decoded = proto::ServerMessage::decode(bytes.as_slice()).unwrap();
        match decoded.msg.unwrap() {
            proto::server_message::Msg::Error(e) => {
                assert_eq!(e.message, "bad query");
                assert_eq!(e.request_id, Some("e1".into()));
            }
            other => panic!("expected Error, got {other:?}"),
        }
    }

    #[test]
    fn test_graph_value_scalars() {
        for (internal, expected) in [
            (
                GraphValue::Null,
                proto::graph_value::Value::NullValue(proto::NullValue {}),
            ),
            (GraphValue::Bool(true), proto::graph_value::Value::BoolValue(true)),
            (GraphValue::Int(42), proto::graph_value::Value::IntValue(42)),
            (
                GraphValue::Float(3.14),
                proto::graph_value::Value::FloatValue(3.14),
            ),
            (
                GraphValue::String("hi".into()),
                proto::graph_value::Value::StringValue("hi".into()),
            ),
        ] {
            let proto = graph_value_to_proto(&internal);
            assert_eq!(proto.value.unwrap(), expected);
        }
    }

    #[test]
    fn test_graph_value_list() {
        let list = GraphValue::List(vec![GraphValue::Int(1), GraphValue::Int(2)]);
        let proto = graph_value_to_proto(&list);
        match proto.value.unwrap() {
            proto::graph_value::Value::ListValue(l) => {
                assert_eq!(l.values.len(), 2);
                assert_eq!(
                    l.values[0].value,
                    Some(proto::graph_value::Value::IntValue(1))
                );
            }
            other => panic!("expected ListValue, got {other:?}"),
        }
    }

    #[test]
    fn test_graph_value_node() {
        let node = GraphValue::Tagged(TaggedValue::Node {
            id: InternalId { table: 0, offset: 5 },
            label: "Person".into(),
            properties: HashMap::from([("name".into(), GraphValue::String("Alice".into()))]),
        });
        let proto = graph_value_to_proto(&node);
        match proto.value.unwrap() {
            proto::graph_value::Value::NodeValue(n) => {
                assert_eq!(n.label, "Person");
                assert_eq!(n.id.unwrap().offset, 5);
                assert_eq!(n.properties.len(), 1);
                assert_eq!(n.properties[0].key, "name");
            }
            other => panic!("expected NodeValue, got {other:?}"),
        }
    }

    #[test]
    fn test_graph_value_rel() {
        let rel = GraphValue::Tagged(TaggedValue::Rel {
            id: InternalId { table: 2, offset: 10 },
            label: "KNOWS".into(),
            src: InternalId { table: 0, offset: 5 },
            dst: InternalId { table: 0, offset: 8 },
            properties: HashMap::new(),
        });
        let proto = graph_value_to_proto(&rel);
        match proto.value.unwrap() {
            proto::graph_value::Value::RelValue(r) => {
                assert_eq!(r.label, "KNOWS");
                assert_eq!(r.src.unwrap().offset, 5);
                assert_eq!(r.dst.unwrap().offset, 8);
            }
            other => panic!("expected RelValue, got {other:?}"),
        }
    }

    #[test]
    fn test_roundtrip_batch_result() {
        let msg = protocol::ServerMessage::BatchResult {
            results: vec![
                protocol::BatchResultEntry::Result {
                    columns: vec!["x".into()],
                    rows: vec![vec![GraphValue::Int(1)]],
                    timing_ms: 0.5,
                },
                protocol::BatchResultEntry::Error {
                    message: "fail".into(),
                },
            ],
            request_id: Some("b1".into()),
        };
        let bytes = encode_server_message(&msg);
        let decoded = proto::ServerMessage::decode(bytes.as_slice()).unwrap();
        match decoded.msg.unwrap() {
            proto::server_message::Msg::BatchResult(br) => {
                assert_eq!(br.results.len(), 2);
                assert_eq!(br.request_id, Some("b1".into()));
                match br.results[0].entry.as_ref().unwrap() {
                    proto::batch_result_entry::Entry::Result(r) => {
                        assert_eq!(r.columns, vec!["x"]);
                    }
                    _ => panic!("expected Result entry"),
                }
                match br.results[1].entry.as_ref().unwrap() {
                    proto::batch_result_entry::Entry::Error(e) => {
                        assert_eq!(e.message, "fail");
                    }
                    _ => panic!("expected Error entry"),
                }
            }
            other => panic!("expected BatchResult, got {other:?}"),
        }
    }

    #[test]
    fn test_roundtrip_all_client_messages() {
        let messages = vec![
            proto::ClientMessage {
                msg: Some(proto::client_message::Msg::Hello(proto::Hello {
                    token: None,
                })),
            },
            proto::ClientMessage {
                msg: Some(proto::client_message::Msg::Begin(proto::Begin {
                    mode: Some("read".into()),
                    request_id: None,
                })),
            },
            proto::ClientMessage {
                msg: Some(proto::client_message::Msg::Commit(proto::Commit {
                    request_id: Some("c1".into()),
                })),
            },
            proto::ClientMessage {
                msg: Some(proto::client_message::Msg::Rollback(proto::Rollback {
                    request_id: None,
                })),
            },
            proto::ClientMessage {
                msg: Some(proto::client_message::Msg::Fetch(proto::Fetch {
                    stream_id: 42,
                    request_id: Some("f1".into()),
                })),
            },
            proto::ClientMessage {
                msg: Some(proto::client_message::Msg::CloseStream(
                    proto::CloseStream {
                        stream_id: 7,
                        request_id: None,
                    },
                )),
            },
            proto::ClientMessage {
                msg: Some(proto::client_message::Msg::Close(proto::Close {})),
            },
        ];
        for proto_msg in messages {
            let bytes = proto_msg.encode_to_vec();
            let _ = decode_client_message(&bytes).unwrap();
        }
    }

    #[test]
    fn test_roundtrip_all_server_messages() {
        let messages = vec![
            protocol::ServerMessage::HelloOk {
                version: "0.1.0".into(),
            },
            protocol::ServerMessage::HelloError {
                message: "bad".into(),
            },
            protocol::ServerMessage::BeginOk {
                request_id: Some("b1".into()),
            },
            protocol::ServerMessage::CommitOk { request_id: None },
            protocol::ServerMessage::RollbackOk { request_id: None },
            protocol::ServerMessage::CloseStreamOk {
                stream_id: 5,
                request_id: Some("cs1".into()),
            },
            protocol::ServerMessage::CloseOk,
        ];
        for msg in messages {
            let bytes = encode_server_message(&msg);
            let decoded = proto::ServerMessage::decode(bytes.as_slice()).unwrap();
            assert!(decoded.msg.is_some());
        }
    }

    #[test]
    fn test_params_conversion() {
        // Empty params
        assert!(proto_params_to_json(&[]).is_none());

        // Mixed param types
        let params = vec![
            proto::MapEntry {
                key: "s".into(),
                value: Some(proto::GraphValue {
                    value: Some(proto::graph_value::Value::StringValue("hello".into())),
                }),
            },
            proto::MapEntry {
                key: "i".into(),
                value: Some(proto::GraphValue {
                    value: Some(proto::graph_value::Value::IntValue(42)),
                }),
            },
            proto::MapEntry {
                key: "f".into(),
                value: Some(proto::GraphValue {
                    value: Some(proto::graph_value::Value::FloatValue(3.14)),
                }),
            },
            proto::MapEntry {
                key: "b".into(),
                value: Some(proto::GraphValue {
                    value: Some(proto::graph_value::Value::BoolValue(true)),
                }),
            },
            proto::MapEntry {
                key: "n".into(),
                value: Some(proto::GraphValue {
                    value: Some(proto::graph_value::Value::NullValue(proto::NullValue {})),
                }),
            },
        ];
        let json = proto_params_to_json(&params).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["s"], "hello");
        assert_eq!(obj["i"], 42);
        assert!((obj["f"].as_f64().unwrap() - 3.14).abs() < 0.001);
        assert_eq!(obj["b"], true);
        assert!(obj["n"].is_null());
    }

    #[test]
    fn test_empty_client_message_error() {
        let proto_msg = proto::ClientMessage { msg: None };
        let bytes = proto_msg.encode_to_vec();
        assert!(decode_client_message(&bytes).is_err());
    }

    #[test]
    fn test_invalid_bytes_error() {
        assert!(decode_client_message(&[0xff, 0xff, 0xff]).is_err());
    }
}
