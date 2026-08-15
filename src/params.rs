//! CEL-computed expression-attribute values, bound as DynamoDB
//! `ExpressionAttributeValues`.
//!
//! DynamoDB is not SQL — an operator-fixed `key_condition_expression` /
//! `filter_expression` references placeholders by name (`:name`). Operators
//! declare a `params: { ":name": "<CEL>", … }` map on a binding. At
//! `register_profile` each expression is compiled once; per call each is
//! evaluated against the call's `arguments` object and the resulting scalar is
//! lowered to an [`AttributeValue`] (`S` / `N` / `BOOL` / `NULL`) and merged
//! into the request's `ExpressionAttributeValues` SERVER-SIDE via the aws-sdk —
//! the value never touches the operator-fixed expression string (injection-safe).
//!
//! Scalars only: a single `:placeholder` carries one AttributeValue, so
//! arrays/objects are rejected (`L`/`M` are not produced from CEL params here).

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use cel::{Context as CelContext, Program, Value as CelValue};
use serde_json::Value;

/// A compiled `params` entry — the placeholder name and its CEL program.
/// `cel::Program` isn't `Clone`; the profile shares these via `Arc<[_]>`.
#[derive(Debug)]
pub struct CompiledParam {
    /// Placeholder name as it appears in the expression, e.g. `:status`.
    pub placeholder: String,
    /// Compiled CEL program — evaluated per call.
    pub program: Program,
    /// Original source, retained for diagnostics.
    pub source: String,
}

/// Compile every parameter expression. Errors name the offending placeholder.
/// Placeholders must begin with `:` (the DynamoDB expression-attribute-value
/// sigil) and must not collide.
pub fn compile_params(params: &HashMap<String, String>) -> Result<Vec<CompiledParam>, String> {
    let mut out = Vec::with_capacity(params.len());
    // Deterministic order (BTree-style) so diagnostics / schema derivation are
    // stable across runs.
    let mut entries: Vec<(&String, &String)> = params.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (placeholder, source) in entries {
        if !placeholder.starts_with(':') {
            return Err(format!(
                "params key `{placeholder}` must begin with `:` (DynamoDB expression-attribute-value placeholder)"
            ));
        }
        let program = Program::compile(source)
            .map_err(|e| format!("params[{placeholder}] does not compile as CEL: {e}"))?;
        out.push(CompiledParam {
            placeholder: placeholder.clone(),
            program,
            source: source.clone(),
        });
    }
    Ok(out)
}

/// Evaluate every compiled expression against `arguments`, lowering each result
/// to a scalar [`AttributeValue`]. Returns the placeholder → AttributeValue map
/// ready to merge into a request's `ExpressionAttributeValues`.
pub fn evaluate_params(
    params: &[CompiledParam],
    arguments: &Value,
) -> Result<HashMap<String, AttributeValue>, String> {
    let args_cel = json_to_cel(arguments);
    let mut out = HashMap::with_capacity(params.len());
    for p in params {
        let mut ctx = CelContext::default();
        ctx.add_variable("arguments", args_cel.clone())
            .map_err(|e| format!("params[{}]: bind arguments: {e}", p.placeholder))?;
        let value = p.program.execute(&ctx).map_err(|e| {
            format!(
                "params[{}] failed: {e} (source: {})",
                p.placeholder, p.source
            )
        })?;
        let av =
            cel_to_attribute_value(value).map_err(|e| format!("params[{}]: {e}", p.placeholder))?;
        out.insert(p.placeholder.clone(), av);
    }
    Ok(out)
}

/// Lower one CEL value to a scalar DynamoDB [`AttributeValue`]. Scalars only —
/// a single `:placeholder` can't carry arrays/objects. Pure (no transport), so
/// unit-testable in isolation. Numbers serialise to the DynamoDB `N` string
/// form; strings → `S`; bools → `BOOL`; null → `NULL`.
pub fn cel_to_attribute_value(value: CelValue) -> Result<AttributeValue, String> {
    match value {
        CelValue::Null => Ok(AttributeValue::Null(true)),
        CelValue::Bool(b) => Ok(AttributeValue::Bool(b)),
        CelValue::Int(i) => Ok(AttributeValue::N(i.to_string())),
        CelValue::UInt(u) => Ok(AttributeValue::N(u.to_string())),
        CelValue::Float(f) => {
            if f.is_finite() {
                Ok(AttributeValue::N(format_n(f)))
            } else {
                Err("non-finite float cannot be bound as a DynamoDB number".into())
            }
        }
        CelValue::String(s) => Ok(AttributeValue::S(s.as_ref().clone())),
        CelValue::Bytes(_)
        | CelValue::List(_)
        | CelValue::Map(_)
        | CelValue::Duration(_)
        | CelValue::Timestamp(_) => {
            Err("only scalar values (string/number/bool/null) can be bound as ExpressionAttributeValues".into())
        }
        other => Err(format!(
            "unsupported CEL value `{other:?}` cannot be bound as an ExpressionAttributeValue"
        )),
    }
}

/// Render a finite f64 in the DynamoDB `N` string form, dropping a trailing
/// `.0` so an integral float (`3.0`) binds as `3` rather than `3.0`.
fn format_n(f: f64) -> String {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        format!("{}", f as i64)
    } else {
        let mut s = format!("{f}");
        if !s.contains('.') && !s.contains('e') && !s.contains('E') {
            s.push_str(".0");
        }
        s
    }
}

/// serde_json → CEL value (recursive).
fn json_to_cel(v: &Value) -> CelValue {
    use cel::objects::{Key as CelKey, Map as CelMap};
    use std::sync::Arc;
    match v {
        Value::Null => CelValue::Null,
        Value::Bool(b) => CelValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                CelValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                CelValue::UInt(u)
            } else if let Some(f) = n.as_f64() {
                CelValue::Float(f)
            } else {
                CelValue::String(Arc::new(n.to_string()))
            }
        }
        Value::String(s) => CelValue::String(Arc::new(s.clone())),
        Value::Array(arr) => CelValue::List(Arc::new(arr.iter().map(json_to_cel).collect())),
        Value::Object(map) => {
            let mut out = std::collections::HashMap::new();
            for (k, v) in map {
                out.insert(CelKey::String(Arc::new(k.clone())), json_to_cel(v));
            }
            CelValue::Map(CelMap { map: Arc::new(out) })
        }
    }
}

/// The distinct `arguments.<ident>` names referenced across a binding's compiled
/// CEL params, preserving first-seen order. A best-effort hint for input-schema
/// derivation — pure string scan (no CEL deps), never a rejection surface.
pub fn arguments_referenced_by_params(params: &[CompiledParam]) -> Vec<String> {
    let mut names = Vec::new();
    for p in params {
        for name in extract_argument_idents(&p.source) {
            if !names.contains(&name) {
                names.push(name);
            }
        }
    }
    names
}

/// Extract identifiers appearing as `arguments.<ident>` in a CEL source string.
fn extract_argument_idents(source: &str) -> Vec<String> {
    const MARKER: &str = "arguments.";
    let mut out = Vec::new();
    let bytes = source.as_bytes();
    let mut search_from = 0;
    while let Some(rel) = source[search_from..].find(MARKER) {
        let start = search_from + rel + MARKER.len();
        let mut end = start;
        while end < bytes.len() {
            let c = bytes[end];
            if c.is_ascii_alphanumeric() || c == b'_' {
                end += 1;
            } else {
                break;
            }
        }
        if end > start {
            out.push(source[start..end].to_owned());
        }
        search_from = end.max(search_from + rel + MARKER.len());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn compile_rejects_invalid_cel() {
        let err = compile_params(&params(&[(":x", "this is not cel (((")])).unwrap_err();
        assert!(err.contains("params[:x]"), "{err}");
    }

    #[test]
    fn compile_rejects_placeholder_without_colon() {
        let err = compile_params(&params(&[("status", "arguments.status")])).unwrap_err();
        assert!(err.contains("must begin with `:`"), "{err}");
    }

    #[test]
    fn evaluates_and_lowers_scalars() {
        let compiled = compile_params(&params(&[
            (":id", "arguments.id"),
            (":name", "arguments.name"),
            (":active", "arguments.active"),
            (":page", "arguments.page * 10"),
        ]))
        .unwrap();
        let out = evaluate_params(
            &compiled,
            &json!({ "id": 7, "name": "alice", "active": true, "page": 3 }),
        )
        .unwrap();
        assert_eq!(out[":id"], AttributeValue::N("7".into()));
        assert_eq!(out[":name"], AttributeValue::S("alice".into()));
        assert_eq!(out[":active"], AttributeValue::Bool(true));
        assert_eq!(out[":page"], AttributeValue::N("30".into()));
    }

    #[test]
    fn lowers_null_and_float() {
        assert_eq!(
            cel_to_attribute_value(CelValue::Null).unwrap(),
            AttributeValue::Null(true)
        );
        assert_eq!(
            cel_to_attribute_value(CelValue::Float(19.99)).unwrap(),
            AttributeValue::N("19.99".into())
        );
        // Integral float drops the trailing `.0`.
        assert_eq!(
            cel_to_attribute_value(CelValue::Float(3.0)).unwrap(),
            AttributeValue::N("3".into())
        );
    }

    #[test]
    fn rejects_non_scalar() {
        use std::sync::Arc;
        let err =
            cel_to_attribute_value(CelValue::List(Arc::new(vec![CelValue::Int(1)]))).unwrap_err();
        assert!(err.contains("only scalar"), "{err}");
        let err = cel_to_attribute_value(CelValue::Bytes(Arc::new(vec![1, 2, 3]))).unwrap_err();
        assert!(err.contains("only scalar"), "{err}");
    }

    #[test]
    fn rejects_non_finite_float() {
        let err = cel_to_attribute_value(CelValue::Float(f64::NAN)).unwrap_err();
        assert!(err.contains("non-finite"), "{err}");
    }

    #[test]
    fn runtime_failure_reports_source() {
        let compiled = compile_params(&params(&[(":x", "arguments.missing.deeply")])).unwrap();
        let err = evaluate_params(&compiled, &json!({})).unwrap_err();
        assert!(
            err.contains("params[:x]") && err.contains("arguments.missing"),
            "{err}"
        );
    }

    #[test]
    fn extracts_referenced_argument_idents() {
        let compiled = compile_params(&params(&[
            (":a", "arguments.user_id"),
            (":b", "size(arguments.tags) + arguments.user_id"),
        ]))
        .unwrap();
        let names = arguments_referenced_by_params(&compiled);
        assert!(names.contains(&"user_id".to_owned()));
        assert!(names.contains(&"tags".to_owned()));
    }
}
