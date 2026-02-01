use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Internal ID for nodes and relationships in the graph database.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InternalId {
    pub table: u64,
    pub offset: u64,
}

/// A value returned from a Cypher query, encoded per the Strana protocol spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum GraphValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    List(Vec<GraphValue>),
    Map(HashMap<String, GraphValue>),
    Tagged(TaggedValue),
}

/// Graph-specific types that use a `$type` discriminator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "$type", rename_all = "lowercase")]
pub enum TaggedValue {
    Node {
        id: InternalId,
        label: String,
        properties: HashMap<String, GraphValue>,
    },
    Rel {
        id: InternalId,
        label: String,
        src: InternalId,
        dst: InternalId,
        properties: HashMap<String, GraphValue>,
    },
    Path {
        nodes: Vec<GraphValue>,
        rels: Vec<GraphValue>,
    },
    Union {
        tag: String,
        value: Box<GraphValue>,
    },
}

/// Convert a LadybugDB Value into a Strana GraphValue.
pub fn from_lbug_value(value: &lbug::Value) -> GraphValue {
    match value {
        lbug::Value::Null(_) => GraphValue::Null,
        lbug::Value::Bool(b) => GraphValue::Bool(*b),
        lbug::Value::Int8(n) => GraphValue::Int(*n as i64),
        lbug::Value::Int16(n) => GraphValue::Int(*n as i64),
        lbug::Value::Int32(n) => GraphValue::Int(*n as i64),
        lbug::Value::Int64(n) => GraphValue::Int(*n),
        lbug::Value::Int128(n) => GraphValue::String(n.to_string()),
        lbug::Value::UInt8(n) => GraphValue::Int(*n as i64),
        lbug::Value::UInt16(n) => GraphValue::Int(*n as i64),
        lbug::Value::UInt32(n) => GraphValue::Int(*n as i64),
        lbug::Value::UInt64(n) => {
            if *n <= i64::MAX as u64 {
                GraphValue::Int(*n as i64)
            } else {
                GraphValue::String(n.to_string())
            }
        }
        lbug::Value::Float(f) => GraphValue::Float(*f as f64),
        lbug::Value::Double(f) => GraphValue::Float(*f),
        lbug::Value::Decimal(d) => GraphValue::String(d.to_string()),
        lbug::Value::String(s) => GraphValue::String(s.clone()),
        lbug::Value::Blob(b) => {
            GraphValue::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        lbug::Value::UUID(u) => GraphValue::String(u.to_string()),
        lbug::Value::Date(d) => GraphValue::String(d.to_string()),
        lbug::Value::Timestamp(t)
        | lbug::Value::TimestampTz(t)
        | lbug::Value::TimestampNs(t)
        | lbug::Value::TimestampMs(t)
        | lbug::Value::TimestampSec(t) => GraphValue::String(t.to_string()),
        lbug::Value::Interval(d) => GraphValue::String(format!("{d:?}")),
        lbug::Value::List(_, items) | lbug::Value::Array(_, items) => {
            GraphValue::List(items.iter().map(from_lbug_value).collect())
        }
        lbug::Value::Map(_, entries) => {
            let mut map = HashMap::new();
            for (k, v) in entries {
                let key = match k {
                    lbug::Value::String(s) => s.clone(),
                    other => format!("{other:?}"),
                };
                map.insert(key, from_lbug_value(v));
            }
            GraphValue::Map(map)
        }
        lbug::Value::Struct(fields) => {
            let mut map = HashMap::new();
            for (key, val) in fields {
                map.insert(key.clone(), from_lbug_value(val));
            }
            GraphValue::Map(map)
        }
        lbug::Value::Node(node) => {
            let id = InternalId {
                table: node.get_node_id().table_id,
                offset: node.get_node_id().offset,
            };
            let mut properties = HashMap::new();
            for (key, val) in node.get_properties() {
                properties.insert(key.clone(), from_lbug_value(val));
            }
            GraphValue::Tagged(TaggedValue::Node {
                id,
                label: node.get_label_name().clone(),
                properties,
            })
        }
        lbug::Value::Rel(rel) => {
            let src = InternalId {
                table: rel.get_src_node().table_id,
                offset: rel.get_src_node().offset,
            };
            let dst = InternalId {
                table: rel.get_dst_node().table_id,
                offset: rel.get_dst_node().offset,
            };
            let mut properties = HashMap::new();
            for (key, val) in rel.get_properties() {
                properties.insert(key.clone(), from_lbug_value(val));
            }
            GraphValue::Tagged(TaggedValue::Rel {
                id: InternalId { table: 0, offset: 0 }, // RelVal doesn't expose its own ID
                label: rel.get_label_name().clone(),
                src,
                dst,
                properties,
            })
        }
        lbug::Value::RecursiveRel { nodes, rels } => {
            GraphValue::Tagged(TaggedValue::Path {
                nodes: nodes.iter().map(|n| from_lbug_value(&lbug::Value::Node(n.clone()))).collect(),
                rels: rels.iter().map(|r| from_lbug_value(&lbug::Value::Rel(r.clone()))).collect(),
            })
        }
        lbug::Value::InternalID(id) => GraphValue::Map(HashMap::from([
            ("table".to_string(), GraphValue::Int(id.table_id as i64)),
            ("offset".to_string(), GraphValue::Int(id.offset as i64)),
        ])),
        lbug::Value::Union { value, .. } => from_lbug_value(value),
    }
}

/// Convert a JSON value to a LadybugDB value for parameter binding.
pub fn to_lbug_value(json: &serde_json::Value) -> Result<lbug::Value, String> {
    match json {
        serde_json::Value::Null => Ok(lbug::Value::Null(lbug::LogicalType::Any)),
        serde_json::Value::Bool(b) => Ok(lbug::Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(lbug::Value::Int64(i))
            } else if let Some(u) = n.as_u64() {
                Ok(lbug::Value::UInt64(u))
            } else if let Some(f) = n.as_f64() {
                Ok(lbug::Value::Double(f))
            } else {
                Err("Unsupported number type".into())
            }
        }
        serde_json::Value::String(s) => Ok(lbug::Value::String(s.clone())),
        serde_json::Value::Array(_) => {
            Err("Arrays not supported as query parameters".into())
        }
        serde_json::Value::Object(_) => {
            Err("Objects not supported as query parameters".into())
        }
    }
}

/// Convert a JSON params object to a Vec of (name, lbug::Value) pairs.
pub fn json_params_to_lbug(
    params: &serde_json::Value,
) -> Result<Vec<(String, lbug::Value)>, String> {
    let obj = params
        .as_object()
        .ok_or_else(|| "params must be a JSON object".to_string())?;
    obj.iter()
        .map(|(k, v)| Ok((k.clone(), to_lbug_value(v)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_serialization() {
        assert_eq!(serde_json::to_string(&GraphValue::Null).unwrap(), "null");
        assert_eq!(serde_json::to_string(&GraphValue::Bool(true)).unwrap(), "true");
        assert_eq!(serde_json::to_string(&GraphValue::Int(42)).unwrap(), "42");
        assert_eq!(serde_json::to_string(&GraphValue::Float(3.14)).unwrap(), "3.14");
        assert_eq!(
            serde_json::to_string(&GraphValue::String("hello".into())).unwrap(),
            "\"hello\""
        );
    }

    #[test]
    fn test_node_serialization() {
        let node = GraphValue::Tagged(TaggedValue::Node {
            id: InternalId { table: 0, offset: 5 },
            label: "Person".into(),
            properties: HashMap::from([
                ("name".into(), GraphValue::String("Alice".into())),
                ("age".into(), GraphValue::Int(30)),
            ]),
        });
        let json: serde_json::Value = serde_json::to_value(&node).unwrap();
        assert_eq!(json["$type"], "node");
        assert_eq!(json["label"], "Person");
        assert_eq!(json["id"]["table"], 0);
        assert_eq!(json["id"]["offset"], 5);
    }

    #[test]
    fn test_rel_serialization() {
        let rel = GraphValue::Tagged(TaggedValue::Rel {
            id: InternalId { table: 2, offset: 10 },
            label: "KNOWS".into(),
            src: InternalId { table: 0, offset: 5 },
            dst: InternalId { table: 0, offset: 8 },
            properties: HashMap::new(),
        });
        let json: serde_json::Value = serde_json::to_value(&rel).unwrap();
        assert_eq!(json["$type"], "rel");
        assert_eq!(json["label"], "KNOWS");
        assert_eq!(json["src"]["offset"], 5);
        assert_eq!(json["dst"]["offset"], 8);
    }

    #[test]
    fn test_list_serialization() {
        let list = GraphValue::List(vec![GraphValue::Int(1), GraphValue::Int(2)]);
        assert_eq!(serde_json::to_string(&list).unwrap(), "[1,2]");
    }

    #[test]
    fn test_to_lbug_null() {
        let v = to_lbug_value(&serde_json::Value::Null).unwrap();
        assert!(matches!(v, lbug::Value::Null(_)));
    }

    #[test]
    fn test_to_lbug_bool() {
        assert!(matches!(
            to_lbug_value(&serde_json::json!(true)).unwrap(),
            lbug::Value::Bool(true)
        ));
        assert!(matches!(
            to_lbug_value(&serde_json::json!(false)).unwrap(),
            lbug::Value::Bool(false)
        ));
    }

    #[test]
    fn test_to_lbug_int() {
        match to_lbug_value(&serde_json::json!(42)).unwrap() {
            lbug::Value::Int64(n) => assert_eq!(n, 42),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn test_to_lbug_float() {
        match to_lbug_value(&serde_json::json!(3.14)).unwrap() {
            lbug::Value::Double(f) => assert!((f - 3.14).abs() < f64::EPSILON),
            other => panic!("expected Double, got {other:?}"),
        }
    }

    #[test]
    fn test_to_lbug_string() {
        match to_lbug_value(&serde_json::json!("hello")).unwrap() {
            lbug::Value::String(s) => assert_eq!(s, "hello"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn test_to_lbug_array_error() {
        assert!(to_lbug_value(&serde_json::json!([1, 2])).is_err());
    }

    #[test]
    fn test_to_lbug_object_error() {
        assert!(to_lbug_value(&serde_json::json!({"a": 1})).is_err());
    }

    #[test]
    fn test_json_params_to_lbug() {
        let params = serde_json::json!({"name": "Alice", "age": 30});
        let result = json_params_to_lbug(&params).unwrap();
        assert_eq!(result.len(), 2);
        // Check both params exist (order not guaranteed from JSON object)
        let names: Vec<&str> = result.iter().map(|(k, _)| k.as_str()).collect();
        assert!(names.contains(&"name"));
        assert!(names.contains(&"age"));
    }

    #[test]
    fn test_json_params_not_object() {
        assert!(json_params_to_lbug(&serde_json::json!("not an object")).is_err());
    }
}
