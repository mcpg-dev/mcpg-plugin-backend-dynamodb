//! Lossless JSON ⇄ DynamoDB `AttributeValue` marshalling using the
//! **DynamoDB-JSON** convention at the tool boundary:
//! `{"S":"x"}`, `{"N":"5"}`, `{"BOOL":true}`, `{"NULL":true}`,
//! `{"M":{...}}`, `{"L":[...]}`, `{"SS":[...]}`, `{"NS":[...]}`,
//! `{"BS":["<b64>"]}`, `{"B":"<b64>"}`.
//!
//! This is the central testable unit of the plugin — it never touches
//! the network, so every type round-trips in offline unit tests.

use std::collections::HashMap;

use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::AttributeValue;
use base64::Engine as _;
use serde_json::{Map, Value};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MarshalError {
    #[error("AttributeValue must be a single-key object (e.g. {{\"S\":\"x\"}}), got: {0}")]
    NotSingleKey(String),
    #[error("unknown AttributeValue type tag `{0}`")]
    UnknownTag(String),
    #[error("AttributeValue `{tag}` has the wrong JSON shape")]
    WrongShape { tag: String },
    #[error("AttributeValue N value `{0}` is not a valid number")]
    BadNumber(String),
    #[error("AttributeValue B/BS value is not valid base64")]
    BadBase64,
    #[error("an item must be a JSON object of attribute-name -> AttributeValue")]
    NotObject,
}

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

fn is_dynamo_number(s: &str) -> bool {
    !s.is_empty() && s.parse::<f64>().is_ok()
}

/// Convert a single DynamoDB-JSON `AttributeValue` object into the SDK type.
pub fn json_to_av(v: &Value) -> Result<AttributeValue, MarshalError> {
    let obj = v
        .as_object()
        .ok_or_else(|| MarshalError::NotSingleKey(v.to_string()))?;
    if obj.len() != 1 {
        return Err(MarshalError::NotSingleKey(v.to_string()));
    }
    let (tag, val) = obj.iter().next().expect("len checked == 1");
    match tag.as_str() {
        "S" => val
            .as_str()
            .map(|s| AttributeValue::S(s.to_owned()))
            .ok_or(MarshalError::WrongShape { tag: "S".into() }),
        "N" => {
            let s = val
                .as_str()
                .ok_or(MarshalError::WrongShape { tag: "N".into() })?;
            if !is_dynamo_number(s) {
                return Err(MarshalError::BadNumber(s.to_owned()));
            }
            Ok(AttributeValue::N(s.to_owned()))
        }
        "BOOL" => val
            .as_bool()
            .map(AttributeValue::Bool)
            .ok_or(MarshalError::WrongShape { tag: "BOOL".into() }),
        "NULL" => val
            .as_bool()
            .map(AttributeValue::Null)
            .ok_or(MarshalError::WrongShape { tag: "NULL".into() }),
        "B" => {
            let s = val
                .as_str()
                .ok_or(MarshalError::WrongShape { tag: "B".into() })?;
            let bytes = B64.decode(s).map_err(|_| MarshalError::BadBase64)?;
            Ok(AttributeValue::B(Blob::new(bytes)))
        }
        "SS" => {
            let arr = val
                .as_array()
                .ok_or(MarshalError::WrongShape { tag: "SS".into() })?;
            let mut out = Vec::with_capacity(arr.len());
            for e in arr {
                out.push(
                    e.as_str()
                        .ok_or(MarshalError::WrongShape { tag: "SS".into() })?
                        .to_owned(),
                );
            }
            Ok(AttributeValue::Ss(out))
        }
        "NS" => {
            let arr = val
                .as_array()
                .ok_or(MarshalError::WrongShape { tag: "NS".into() })?;
            let mut out = Vec::with_capacity(arr.len());
            for e in arr {
                let s = e
                    .as_str()
                    .ok_or(MarshalError::WrongShape { tag: "NS".into() })?;
                if !is_dynamo_number(s) {
                    return Err(MarshalError::BadNumber(s.to_owned()));
                }
                out.push(s.to_owned());
            }
            Ok(AttributeValue::Ns(out))
        }
        "BS" => {
            let arr = val
                .as_array()
                .ok_or(MarshalError::WrongShape { tag: "BS".into() })?;
            let mut out = Vec::with_capacity(arr.len());
            for e in arr {
                let s = e
                    .as_str()
                    .ok_or(MarshalError::WrongShape { tag: "BS".into() })?;
                out.push(Blob::new(
                    B64.decode(s).map_err(|_| MarshalError::BadBase64)?,
                ));
            }
            Ok(AttributeValue::Bs(out))
        }
        "M" => {
            let m = val
                .as_object()
                .ok_or(MarshalError::WrongShape { tag: "M".into() })?;
            Ok(AttributeValue::M(json_map_to_item(m)?))
        }
        "L" => {
            let arr = val
                .as_array()
                .ok_or(MarshalError::WrongShape { tag: "L".into() })?;
            let mut out = Vec::with_capacity(arr.len());
            for e in arr {
                out.push(json_to_av(e)?);
            }
            Ok(AttributeValue::L(out))
        }
        other => Err(MarshalError::UnknownTag(other.to_owned())),
    }
}

/// Convert an SDK `AttributeValue` back into DynamoDB-JSON.
pub fn av_to_json(av: &AttributeValue) -> Value {
    match av {
        AttributeValue::S(s) => single("S", Value::String(s.clone())),
        AttributeValue::N(n) => single("N", Value::String(n.clone())),
        AttributeValue::Bool(b) => single("BOOL", Value::Bool(*b)),
        AttributeValue::Null(b) => single("NULL", Value::Bool(*b)),
        AttributeValue::B(b) => single("B", Value::String(B64.encode(b.as_ref()))),
        AttributeValue::Ss(v) => single(
            "SS",
            Value::Array(v.iter().map(|s| Value::String(s.clone())).collect()),
        ),
        AttributeValue::Ns(v) => single(
            "NS",
            Value::Array(v.iter().map(|s| Value::String(s.clone())).collect()),
        ),
        AttributeValue::Bs(v) => single(
            "BS",
            Value::Array(
                v.iter()
                    .map(|b| Value::String(B64.encode(b.as_ref())))
                    .collect(),
            ),
        ),
        AttributeValue::M(m) => single("M", item_to_json(m)),
        AttributeValue::L(l) => single("L", Value::Array(l.iter().map(av_to_json).collect())),
        // `AttributeValue` is #[non_exhaustive]; a future/unknown variant
        // round-trips to NULL rather than panicking.
        _ => single("NULL", Value::Bool(true)),
    }
}

fn single(tag: &str, val: Value) -> Value {
    let mut m = Map::with_capacity(1);
    m.insert(tag.to_owned(), val);
    Value::Object(m)
}

fn json_map_to_item(
    m: &Map<String, Value>,
) -> Result<HashMap<String, AttributeValue>, MarshalError> {
    let mut out = HashMap::with_capacity(m.len());
    for (k, v) in m {
        out.insert(k.clone(), json_to_av(v)?);
    }
    Ok(out)
}

/// Convert a JSON object (attribute name -> DynamoDB-JSON AV) into a
/// DynamoDB item map.
pub fn json_to_item(v: &Value) -> Result<HashMap<String, AttributeValue>, MarshalError> {
    let obj = v.as_object().ok_or(MarshalError::NotObject)?;
    json_map_to_item(obj)
}

/// Convert a DynamoDB item map back into a JSON object of DynamoDB-JSON AVs.
pub fn item_to_json(item: &HashMap<String, AttributeValue>) -> Value {
    let mut m = Map::with_capacity(item.len());
    for (k, v) in item {
        m.insert(k.clone(), av_to_json(v));
    }
    Value::Object(m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn round_trip(v: Value) {
        let av = json_to_av(&v).expect("json_to_av");
        let back = av_to_json(&av);
        assert_eq!(back, v, "round-trip mismatch");
    }

    #[test]
    fn round_trips_scalars() {
        round_trip(json!({ "S": "hello" }));
        round_trip(json!({ "N": "42" }));
        round_trip(json!({ "N": "-3.14" }));
        round_trip(json!({ "BOOL": true }));
        round_trip(json!({ "NULL": true }));
    }

    #[test]
    fn round_trips_binary() {
        let b64 = B64.encode(b"\x00\x01\xfe\xff");
        round_trip(json!({ "B": b64 }));
    }

    #[test]
    fn round_trips_sets() {
        round_trip(json!({ "SS": ["a", "b", "c"] }));
        round_trip(json!({ "NS": ["1", "2", "3"] }));
    }

    #[test]
    fn round_trips_nested_map_and_list() {
        round_trip(json!({
            "M": {
                "name": { "S": "alice" },
                "age": { "N": "30" },
                "tags": { "L": [ { "S": "x" }, { "N": "1" } ] },
                "active": { "BOOL": true }
            }
        }));
    }

    #[test]
    fn item_round_trips() {
        let v = json!({
            "order_id": { "S": "o-1" },
            "created_at": { "N": "1718000000" },
            "amount": { "N": "19.99" }
        });
        let item = json_to_item(&v).unwrap();
        assert_eq!(item_to_json(&item), v);
    }

    #[test]
    fn rejects_non_single_key() {
        assert!(matches!(
            json_to_av(&json!({ "S": "x", "N": "1" })),
            Err(MarshalError::NotSingleKey(_))
        ));
        assert!(matches!(
            json_to_av(&json!("bare")),
            Err(MarshalError::NotSingleKey(_))
        ));
    }

    #[test]
    fn rejects_unknown_tag() {
        assert!(matches!(
            json_to_av(&json!({ "ZZ": "x" })),
            Err(MarshalError::UnknownTag(_))
        ));
    }

    #[test]
    fn rejects_bad_number() {
        assert!(matches!(
            json_to_av(&json!({ "N": "not-a-number" })),
            Err(MarshalError::BadNumber(_))
        ));
        assert!(matches!(
            json_to_av(&json!({ "NS": ["1", "abc"] })),
            Err(MarshalError::BadNumber(_))
        ));
    }

    #[test]
    fn rejects_bad_base64() {
        assert!(matches!(
            json_to_av(&json!({ "B": "!!!not base64!!!" })),
            Err(MarshalError::BadBase64)
        ));
    }

    #[test]
    fn rejects_wrong_shape() {
        assert!(matches!(
            json_to_av(&json!({ "S": 5 })),
            Err(MarshalError::WrongShape { .. })
        ));
        assert!(matches!(
            json_to_av(&json!({ "BOOL": "true" })),
            Err(MarshalError::WrongShape { .. })
        ));
    }

    #[test]
    fn item_requires_object() {
        assert!(matches!(
            json_to_item(&json!([1, 2])),
            Err(MarshalError::NotObject)
        ));
    }
}
