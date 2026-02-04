//! Shared Bolt protocol types and helpers.
//!
//! Contains reusable Bolt protocol logic shared between graphd (strana) and
//! cinch-cloud: version negotiation, value conversion (GraphValue <-> bolt-proto
//! Value <-> JSON), and message serialization helpers.
//!
//! Consumers provide their own TCP listener, auth, and session worker.

use bytes::Bytes;
use std::collections::HashMap;
use tracing::error;

use bolt_proto::message::{Failure, Record, Success};
use bolt_proto::{Message, Value};

use crate::values::GraphValue;

// ─── Bolt version ───

/// Bolt protocol version negotiated with the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoltVersion {
    V4_4,
    /// Bolt 5.x where the u8 is the minor version (0-7 for Bolt 5.0-5.7)
    V5(u8),
}

impl std::fmt::Display for BoltVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BoltVersion::V4_4 => write!(f, "4.4"),
            BoltVersion::V5(minor) => write!(f, "5.{}", minor),
        }
    }
}

/// Bolt handshake magic bytes.
pub const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Encode a BoltVersion as the 4-byte version response.
pub fn version_bytes(v: BoltVersion) -> [u8; 4] {
    match v {
        BoltVersion::V4_4 => [0x00, 0x00, 0x04, 0x04],
        BoltVersion::V5(minor) => [0x00, 0x00, minor, 0x05],
    }
}

// ─── Version negotiation ───

/// Negotiate Bolt protocol version with the client.
///
/// Each proposal is 4 bytes: [patch, range, minor, major].
/// `range` means the client accepts major.minor down to major.(minor - range).
/// Prefers the highest supported version.
///
/// Supports Bolt 4.4 and Bolt 5.0-5.7.
pub fn negotiate_bolt_version(versions: &[u8; 16]) -> Option<BoltVersion> {
    negotiate_bolt_version_with_limit(versions, None)
}

/// Negotiate with optional version limit (for testing).
pub fn negotiate_bolt_version_with_limit(
    versions: &[u8; 16],
    max_version: Option<BoltVersion>,
) -> Option<BoltVersion> {
    const MAX_BOLT_5_MINOR: u8 = 7;

    let (max_5_minor, allow_5_x, allow_4_4) = match max_version {
        Some(BoltVersion::V4_4) => (0, false, true),
        Some(BoltVersion::V5(m)) => (m, true, true),
        None => (MAX_BOLT_5_MINOR, true, true),
    };

    for i in 0..4 {
        let offset = i * 4;
        let major = versions[offset + 3];
        let minor = versions[offset + 2];
        let range = versions[offset + 1];

        if allow_5_x && major == 5 {
            let min_minor = minor.saturating_sub(range);
            let max_minor = minor;
            for supported_minor in (0..=max_5_minor.min(MAX_BOLT_5_MINOR)).rev() {
                if supported_minor >= min_minor && supported_minor <= max_minor {
                    return Some(BoltVersion::V5(supported_minor));
                }
            }
        }

        if allow_4_4 && major == 4 && minor >= 4 && minor.saturating_sub(range) <= 4 {
            return Some(BoltVersion::V4_4);
        }
    }

    None
}

// ─── Value conversion: Bolt <-> JSON ───

/// Convert Bolt parameters (HashMap<String, Value>) to JSON.
pub fn bolt_params_to_json(params: &HashMap<String, Value>) -> Option<serde_json::Value> {
    if params.is_empty() {
        return None;
    }
    let mut map = serde_json::Map::new();
    for (k, v) in params {
        map.insert(k.clone(), bolt_value_to_json(v));
    }
    Some(serde_json::Value::Object(map))
}

/// Convert a bolt-rs Value to JSON.
pub fn bolt_value_to_json(bolt: &Value) -> serde_json::Value {
    match bolt {
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Integer(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Null => serde_json::Value::Null,
        Value::List(list) => {
            serde_json::Value::Array(list.iter().map(bolt_value_to_json).collect())
        }
        Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), bolt_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
        _ => serde_json::Value::Null, // Bytes, DateTime, Node, Relationship, etc.
    }
}

/// Convert serde_json::Value to bolt-rs Value.
pub fn json_to_bolt(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::from(i)
            } else if let Some(f) = n.as_f64() {
                Value::from(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::from(s.clone()),
        serde_json::Value::Array(arr) => {
            Value::from(arr.iter().map(json_to_bolt).collect::<Vec<_>>())
        }
        serde_json::Value::Object(obj) => {
            let map: HashMap<String, Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_bolt(v)))
                .collect();
            Value::from(map)
        }
    }
}

// ─── Value conversion: GraphValue <-> Bolt ───

/// Convert a GraphValue to bolt-rs Value for RECORD responses.
pub fn graph_value_to_bolt(gv: &GraphValue) -> Value {
    match gv {
        GraphValue::Null => Value::Null,
        GraphValue::Bool(b) => Value::from(*b),
        GraphValue::Int(i) => Value::from(*i),
        GraphValue::Float(f) => Value::from(*f),
        GraphValue::String(s) => Value::from(s.clone()),
        GraphValue::List(items) => {
            Value::from(items.iter().map(graph_value_to_bolt).collect::<Vec<_>>())
        }
        GraphValue::Map(entries) => {
            let map: HashMap<String, Value> = entries
                .iter()
                .map(|(k, v)| (k.clone(), graph_value_to_bolt(v)))
                .collect();
            Value::from(map)
        }
        GraphValue::Tagged(_) => {
            let json = serde_json::to_value(gv).unwrap_or(serde_json::Value::Null);
            json_to_bolt(&json)
        }
    }
}

// ─── Message serialization ───

/// Create a SUCCESS response message as chunked bytes.
pub fn success_message(metadata: HashMap<String, Value>) -> Vec<Bytes> {
    let success = Success::new(metadata);
    let msg = Message::Success(success);
    msg.into_chunks().unwrap_or_else(|e| {
        error!("Failed to serialize SUCCESS: {e}");
        Vec::new()
    })
}

/// Create a RECORD message as chunked bytes.
pub fn record_message(values: Vec<Value>) -> Vec<Bytes> {
    let record = Record::new(values);
    let msg = Message::Record(record);
    msg.into_chunks().unwrap_or_else(|e| {
        error!("Failed to serialize RECORD: {e}");
        Vec::new()
    })
}

/// Create a version-aware FAILURE response message as chunked bytes.
///
/// Bolt 4.4-5.6: Returns `code` and `message` fields.
/// Bolt 5.7+: Returns `neo4j_code`, `gql_status`, `description`, `diagnostic_record`.
pub fn failure_message_versioned(bolt_version: BoltVersion, code: &str, message: &str) -> Vec<Bytes> {
    let mut metadata = HashMap::new();

    match bolt_version {
        BoltVersion::V5(minor) if minor >= 7 => {
            metadata.insert("neo4j_code".to_string(), Value::from(code));
            metadata.insert("description".to_string(), Value::from(message));
            let gql_status = map_neo4j_to_gql_status(code);
            metadata.insert("gql_status".to_string(), Value::from(gql_status));
            let mut diagnostic = HashMap::new();
            diagnostic.insert("_severity".to_string(), Value::from("ERROR"));
            metadata.insert("diagnostic_record".to_string(), Value::from(diagnostic));
        }
        _ => {
            metadata.insert("code".to_string(), Value::from(code));
            metadata.insert("message".to_string(), Value::from(message));
        }
    }

    let failure = Failure::new(metadata);
    let msg = Message::Failure(failure);
    msg.into_chunks().unwrap_or_else(|e| {
        error!("Failed to serialize FAILURE: {e}");
        Vec::new()
    })
}

/// Map Neo4j error codes to GQL status codes (simplified).
fn map_neo4j_to_gql_status(neo4j_code: &str) -> &'static str {
    match neo4j_code {
        code if code.starts_with("Neo.ClientError.Security") => "42000",
        code if code.starts_with("Neo.ClientError.Statement") => "42000",
        code if code.starts_with("Neo.ClientError.Schema") => "42000",
        code if code.starts_with("Neo.ClientError.Request") => "42000",
        code if code.starts_with("Neo.ClientError") => "22000",
        code if code.starts_with("Neo.TransientError") => "40000",
        code if code.starts_with("Neo.DatabaseError") => "HZ000",
        _ => "HZ000",
    }
}

// ─── Auth helpers ───

/// Extract the "credentials" string from a HELLO/LOGON metadata map.
pub fn extract_credentials(metadata: &HashMap<String, Value>) -> Option<&str> {
    metadata.get("credentials").and_then(|v| match v {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    })
}

// ─── Connection hints ───

/// Add version-specific connection hints to HELLO/LOGON SUCCESS metadata.
pub fn add_connection_hints(bolt_version: BoltVersion, metadata: &mut HashMap<String, Value>) {
    let mut hints = HashMap::new();

    match bolt_version {
        BoltVersion::V4_4 => {}
        BoltVersion::V5(minor) => {
            if minor >= 4 {
                hints.insert("telemetry.enabled".to_string(), Value::from(true));
            }
            if !hints.is_empty() {
                metadata.insert("hints".to_string(), Value::from(hints));
            }
        }
    }
}

// ─── Tests ───

#[cfg(test)]
mod tests {
    use super::*;

    // ── bolt_value_to_json ──

    #[test]
    fn bolt_string_to_json() {
        let bolt = Value::from("hello");
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!("hello"));
    }

    #[test]
    fn bolt_integer_to_json() {
        let bolt = Value::from(42i64);
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!(42));
    }

    #[test]
    fn bolt_float_to_json() {
        let bolt = Value::from(3.14f64);
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!(3.14));
    }

    #[test]
    fn bolt_bool_to_json() {
        let bolt = Value::from(true);
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!(true));
    }

    #[test]
    fn bolt_null_to_json() {
        let bolt = Value::Null;
        assert_eq!(bolt_value_to_json(&bolt), serde_json::Value::Null);
    }

    #[test]
    fn bolt_list_to_json() {
        let bolt = Value::from(vec![Value::from(1i64), Value::from("two")]);
        assert_eq!(bolt_value_to_json(&bolt), serde_json::json!([1, "two"]));
    }

    #[test]
    fn bolt_map_to_json() {
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::from("Alice"));
        map.insert("age".to_string(), Value::from(30i64));
        let bolt = Value::from(map);
        let json = bolt_value_to_json(&bolt);
        assert_eq!(json["name"], "Alice");
        assert_eq!(json["age"], 30);
    }

    // ── bolt_params_to_json ──

    #[test]
    fn empty_params_returns_none() {
        let params = HashMap::new();
        assert!(bolt_params_to_json(&params).is_none());
    }

    #[test]
    fn params_with_values() {
        let mut params = HashMap::new();
        params.insert("name".to_string(), Value::from("Bob"));
        let json = bolt_params_to_json(&params).unwrap();
        assert_eq!(json["name"], "Bob");
    }

    // ── graph_value_to_bolt ──

    #[test]
    fn graph_null_to_bolt() {
        let gv = GraphValue::Null;
        assert_eq!(graph_value_to_bolt(&gv), Value::Null);
    }

    #[test]
    fn graph_bool_to_bolt() {
        let gv = GraphValue::Bool(true);
        match graph_value_to_bolt(&gv) {
            Value::Boolean(b) => assert!(b),
            other => panic!("Expected Boolean, got {other:?}"),
        }
    }

    #[test]
    fn graph_int_to_bolt() {
        let gv = GraphValue::Int(99);
        match graph_value_to_bolt(&gv) {
            Value::Integer(i) => assert_eq!(i, 99),
            other => panic!("Expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn graph_float_to_bolt() {
        let gv = GraphValue::Float(2.718);
        match graph_value_to_bolt(&gv) {
            Value::Float(f) => assert!((f - 2.718).abs() < 1e-10),
            other => panic!("Expected Float, got {other:?}"),
        }
    }

    #[test]
    fn graph_string_to_bolt() {
        let gv = GraphValue::String("hello".into());
        match graph_value_to_bolt(&gv) {
            Value::String(s) => assert_eq!(s, "hello"),
            other => panic!("Expected String, got {other:?}"),
        }
    }

    #[test]
    fn graph_list_to_bolt() {
        let gv = GraphValue::List(vec![GraphValue::Int(1), GraphValue::Int(2)]);
        match graph_value_to_bolt(&gv) {
            Value::List(list) => {
                assert_eq!(list.len(), 2);
                match &list[0] {
                    Value::Integer(i) => assert_eq!(*i, 1),
                    other => panic!("Expected Integer, got {other:?}"),
                }
            }
            other => panic!("Expected List, got {other:?}"),
        }
    }

    #[test]
    fn graph_map_to_bolt() {
        let mut hm = HashMap::new();
        hm.insert("key".to_string(), GraphValue::String("value".into()));
        let gv = GraphValue::Map(hm);
        match graph_value_to_bolt(&gv) {
            Value::Map(bmap) => {
                let val = bmap.get("key").unwrap();
                match val {
                    Value::String(s) => assert_eq!(s, "value"),
                    other => panic!("Expected String, got {other:?}"),
                }
            }
            other => panic!("Expected Map, got {other:?}"),
        }
    }

    // ── json_to_bolt ──

    #[test]
    fn json_null_to_bolt() {
        assert_eq!(json_to_bolt(&serde_json::Value::Null), Value::Null);
    }

    #[test]
    fn json_string_to_bolt() {
        match json_to_bolt(&serde_json::json!("test")) {
            Value::String(s) => assert_eq!(s, "test"),
            other => panic!("Expected String, got {other:?}"),
        }
    }

    #[test]
    fn json_integer_to_bolt() {
        match json_to_bolt(&serde_json::json!(42)) {
            Value::Integer(i) => assert_eq!(i, 42),
            other => panic!("Expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn json_float_to_bolt() {
        match json_to_bolt(&serde_json::json!(3.14)) {
            Value::Float(f) => assert!((f - 3.14).abs() < 1e-10),
            other => panic!("Expected Float, got {other:?}"),
        }
    }

    #[test]
    fn json_array_to_bolt() {
        match json_to_bolt(&serde_json::json!([1, "two"])) {
            Value::List(list) => assert_eq!(list.len(), 2),
            other => panic!("Expected List, got {other:?}"),
        }
    }

    #[test]
    fn json_object_to_bolt() {
        match json_to_bolt(&serde_json::json!({"a": 1})) {
            Value::Map(map) => {
                assert_eq!(map.len(), 1);
                let val = map.get("a").unwrap();
                match val {
                    Value::Integer(i) => assert_eq!(*i, 1),
                    other => panic!("Expected Integer, got {other:?}"),
                }
            }
            other => panic!("Expected Map, got {other:?}"),
        }
    }

    // ── Roundtrips ──

    #[test]
    fn json_bolt_roundtrip_nested() {
        let original = serde_json::json!({
            "name": "Alice",
            "age": 30,
            "active": true,
            "scores": [90, 85, 92],
            "address": {
                "city": "Wonderland",
                "zip": null
            }
        });
        let bolt = json_to_bolt(&original);
        let back = bolt_value_to_json(&bolt);
        assert_eq!(back["name"], "Alice");
        assert_eq!(back["age"], 30);
        assert_eq!(back["active"], true);
        assert_eq!(back["scores"], serde_json::json!([90, 85, 92]));
        assert_eq!(back["address"]["city"], "Wonderland");
        assert!(back["address"]["zip"].is_null());
    }

    #[test]
    fn graph_value_bolt_roundtrip() {
        let gv = GraphValue::List(vec![
            GraphValue::String("hello".into()),
            GraphValue::Int(42),
            GraphValue::Float(1.5),
            GraphValue::Bool(false),
            GraphValue::Null,
        ]);
        let bolt = graph_value_to_bolt(&gv);
        let json = bolt_value_to_json(&bolt);
        assert_eq!(json, serde_json::json!(["hello", 42, 1.5, false, null]));
    }

    // ── extract_credentials ──

    #[test]
    fn extract_credentials_basic_scheme() {
        let mut metadata = HashMap::new();
        metadata.insert("scheme".to_string(), Value::from("basic"));
        metadata.insert("principal".to_string(), Value::from("neo4j"));
        metadata.insert("credentials".to_string(), Value::from("my-secret-token"));
        assert_eq!(extract_credentials(&metadata), Some("my-secret-token"));
    }

    #[test]
    fn extract_credentials_missing() {
        let metadata = HashMap::new();
        assert_eq!(extract_credentials(&metadata), None);
    }

    #[test]
    fn extract_credentials_non_string() {
        let mut metadata = HashMap::new();
        metadata.insert("credentials".to_string(), Value::from(42i64));
        assert_eq!(extract_credentials(&metadata), None);
    }

    #[test]
    fn extract_credentials_empty_string() {
        let mut metadata = HashMap::new();
        metadata.insert("credentials".to_string(), Value::from(""));
        assert_eq!(extract_credentials(&metadata), Some(""));
    }

    // ── negotiate_bolt_version ──

    #[test]
    fn version_exact_4_4() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V4_4));
    }

    #[test]
    fn version_range_includes_4_4() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x02, 0x04, 0x04]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V4_4));
    }

    #[test]
    fn version_4_3_no_range() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x03, 0x04]);
        assert_eq!(negotiate_bolt_version(&v), None);
    }

    #[test]
    fn version_5_0() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(0)));
    }

    #[test]
    fn version_5_1() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x01, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(1)));
    }

    #[test]
    fn version_prefers_highest() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x01, 0x05]);
        v[4..8].copy_from_slice(&[0x00, 0x00, 0x00, 0x05]);
        v[8..12].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(1)));
    }

    #[test]
    fn version_in_second_slot() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x00, 0x06]);
        v[4..8].copy_from_slice(&[0x00, 0x00, 0x04, 0x04]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V4_4));
    }

    #[test]
    fn version_all_zeros() {
        let v = [0u8; 16];
        assert_eq!(negotiate_bolt_version(&v), None);
    }

    #[test]
    fn version_high_minor_range_includes_4_4() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x03, 0x06, 0x04]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V4_4));
    }

    #[test]
    fn version_5_1_with_range() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x01, 0x01, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(1)));
    }

    #[test]
    fn version_5_2_down_to_5_0_matches_5_2() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x02, 0x02, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(2)));
    }

    #[test]
    fn version_5_2_no_range_matches_5_2() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x02, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(2)));
    }

    #[test]
    fn version_5_7() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x07, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(7)));
    }

    #[test]
    fn version_5_8_not_supported() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x00, 0x08, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), None);
    }

    #[test]
    fn version_5_10_with_range_matches_5_7() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x05, 0x0A, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(7)));
    }

    #[test]
    fn version_malformed_range_greater_than_minor() {
        let mut v = [0u8; 16];
        v[0..4].copy_from_slice(&[0x00, 0x02, 0x01, 0x05]);
        assert_eq!(negotiate_bolt_version(&v), Some(BoltVersion::V5(1)));
    }
}
