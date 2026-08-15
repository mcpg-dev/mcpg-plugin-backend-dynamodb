//! AWS client construction + per-operation dispatch for the DynamoDB
//! backend. Each operation marshals the caller's JSON arguments into a
//! DynamoDB request, runs it under a per-call timeout, and shapes the
//! response back into DynamoDB-JSON.

use std::collections::HashMap;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::types::AttributeValue;
use mcpg_plugin_protocol::BackendError;
use serde_json::{Value, json};

use crate::config::{DynamodbSpec, Operation};
use crate::marshal::{item_to_json, json_to_item};

/// An operation failure is either a caller/argument problem (surfaced as
/// an MCP tool error, `isError: true`) or a backend/transport problem
/// (surfaced as a `BackendError`, a 5xx to the caller).
#[derive(Debug)]
pub(crate) enum OpError {
    /// Bad caller arguments — becomes a tool-level error result.
    Tool(String),
    /// Backend/transport failure — becomes a `BackendError`.
    Backend(BackendError),
}

impl From<BackendError> for OpError {
    fn from(e: BackendError) -> Self {
        OpError::Backend(e)
    }
}

pub(crate) async fn build_client(spec: &DynamodbSpec) -> Client {
    let mut builder = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(spec.region.clone()));
    if let Some(ep) = &spec.endpoint_url {
        builder = builder.endpoint_url(ep.clone());
    }
    if let Some(c) = &spec.credentials {
        builder = builder.credentials_provider(Credentials::new(
            c.access_key_id.clone(),
            c.secret_access_key.clone(),
            c.session_token.clone(),
            None,
            "mcpg-dynamodb-static",
        ));
    } else {
        let defaults = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(spec.region.clone()))
            .load()
            .await;
        if let Some(provider) = defaults.credentials_provider() {
            builder = builder.credentials_provider(provider);
        }
    }
    Client::from_conf(builder.build())
}

/// Run an op future under the spec's per-call timeout.
async fn timed<F, T, E>(spec: &DynamodbSpec, fut: F) -> Result<Result<T, E>, OpError>
where
    F: std::future::Future<Output = Result<T, E>>,
{
    match tokio::time::timeout(Duration::from_millis(spec.limits.timeout_ms), fut).await {
        Ok(r) => Ok(r),
        Err(_) => Err(OpError::Backend(BackendError::Timeout {
            timeout_ms: spec.limits.timeout_ms,
        })),
    }
}

fn map_sdk_err<E: ProvideErrorMetadata>(err: aws_sdk_dynamodb::error::SdkError<E>) -> BackendError {
    let svc = err.as_service_error();
    let code = svc.and_then(|e| e.code()).unwrap_or("");
    let msg = svc.and_then(|e| e.message()).unwrap_or("");
    let detail = if code.is_empty() {
        format!("dynamodb request failed: {err}")
    } else {
        format!("dynamodb {code}: {msg}")
    };
    BackendError::Transport { message: detail }
}

fn args_obj(args: &Value) -> Result<&serde_json::Map<String, Value>, OpError> {
    args.as_object()
        .ok_or_else(|| OpError::Tool("arguments must be a JSON object".into()))
}

/// Marshal a DynamoDB-JSON object arg (`key`, `item`,
/// `expression_attribute_values`) into an item map, surfacing marshal
/// errors as tool errors.
fn marshal_item(v: &Value, field: &str) -> Result<HashMap<String, AttributeValue>, OpError> {
    json_to_item(v).map_err(|e| OpError::Tool(format!("`{field}`: {e}")))
}

fn opt_string(args: &serde_json::Map<String, Value>, k: &str) -> Result<Option<String>, OpError> {
    match args.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err(OpError::Tool(format!("`{k}` must be a string"))),
    }
}

fn opt_name_map(
    args: &serde_json::Map<String, Value>,
    k: &str,
) -> Result<Option<HashMap<String, String>>, OpError> {
    match args.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(m)) => {
            let mut out = HashMap::with_capacity(m.len());
            for (name, v) in m {
                let s = v
                    .as_str()
                    .ok_or_else(|| OpError::Tool(format!("`{k}.{name}` must be a string")))?;
                out.insert(name.clone(), s.to_owned());
            }
            Ok(Some(out))
        }
        Some(_) => Err(OpError::Tool(format!(
            "`{k}` must be an object of name->string"
        ))),
    }
}

/// Build the effective `ExpressionAttributeValues` map: the caller-supplied
/// `expression_attribute_values` (DynamoDB-JSON) merged with the binding's
/// CEL-computed `bound_params`. The binding params win on a placeholder
/// collision so a caller can't shadow an operator-fixed value, and they are
/// never interpolated into the expression string (injection-safe). `None` only
/// when both sources are empty.
fn merged_expr_values(
    args: &serde_json::Map<String, Value>,
    bound_params: &HashMap<String, AttributeValue>,
) -> Result<Option<HashMap<String, AttributeValue>>, OpError> {
    let mut map = match args.get("expression_attribute_values") {
        None | Some(Value::Null) => HashMap::new(),
        Some(v) => marshal_item(v, "expression_attribute_values")?,
    };
    for (k, v) in bound_params {
        map.insert(k.clone(), v.clone());
    }
    if map.is_empty() {
        Ok(None)
    } else {
        Ok(Some(map))
    }
}

/// Validate that a key map contains exactly the table's declared key
/// attribute names — a caller can't address attributes outside the key
/// schema.
fn validate_key(key: &HashMap<String, AttributeValue>, spec: &DynamodbSpec) -> Result<(), OpError> {
    let declared: Vec<&str> = spec.key_names();
    for name in key.keys() {
        if !declared.contains(&name.as_str()) {
            return Err(OpError::Tool(format!(
                "`key` contains attribute `{name}` which is not a declared key (expected {declared:?})"
            )));
        }
    }
    for d in &declared {
        if !key.contains_key(*d) {
            return Err(OpError::Tool(format!(
                "`key` is missing the declared key attribute `{d}`"
            )));
        }
    }
    Ok(())
}

fn clamp_limit(args: &serde_json::Map<String, Value>, spec: &DynamodbSpec) -> i32 {
    let max = spec.limits.max_page_size;
    match args.get("limit").and_then(Value::as_i64) {
        Some(n) if n >= 1 => (n as i32).min(max),
        _ => max,
    }
}

fn opt_start_key(
    args: &serde_json::Map<String, Value>,
) -> Result<Option<HashMap<String, AttributeValue>>, OpError> {
    match args.get("exclusive_start_key") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(marshal_item(v, "exclusive_start_key")?)),
    }
}

pub(crate) async fn execute_op(
    client: &Client,
    spec: &DynamodbSpec,
    args: &Value,
    bound_params: &HashMap<String, AttributeValue>,
) -> Result<Value, OpError> {
    match spec.operation {
        Operation::GetItem => get_item(client, spec, args).await,
        Operation::PutItem => put_item(client, spec, args, bound_params).await,
        Operation::DeleteItem => delete_item(client, spec, args, bound_params).await,
        Operation::UpdateItem => update_item(client, spec, args, bound_params).await,
        Operation::Query => query(client, spec, args, bound_params).await,
        Operation::Scan => scan(client, spec, args, bound_params).await,
        Operation::BatchGet => batch_get(client, spec, args).await,
        Operation::BatchWrite => batch_write(client, spec, args, bound_params).await,
    }
}

/// AWS hard limit on items per BatchGetItem / BatchWriteItem request.
const BATCH_MAX_ITEMS: usize = 25;

async fn get_item(client: &Client, spec: &DynamodbSpec, args: &Value) -> Result<Value, OpError> {
    let a = args_obj(args)?;
    let key_v = a
        .get("key")
        .ok_or_else(|| OpError::Tool("`key` is required".into()))?;
    let key = marshal_item(key_v, "key")?;
    validate_key(&key, spec)?;
    let consistent = a
        .get("consistent_read")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let projection = opt_string(a, "projection_expression")?;

    let out = timed(
        spec,
        client
            .get_item()
            .table_name(&spec.table)
            .set_key(Some(key))
            .consistent_read(consistent)
            .set_projection_expression(projection)
            .send(),
    )
    .await?
    .map_err(map_sdk_err)?;

    let item = out.item().map(item_to_json);
    Ok(json!({ "item": item }))
}

async fn put_item(
    client: &Client,
    spec: &DynamodbSpec,
    args: &Value,
    bound_params: &HashMap<String, AttributeValue>,
) -> Result<Value, OpError> {
    let a = args_obj(args)?;
    let item_v = a
        .get("item")
        .ok_or_else(|| OpError::Tool("`item` is required".into()))?;
    let item = marshal_item(item_v, "item")?;
    for d in spec.key_names() {
        if !item.contains_key(d) {
            return Err(OpError::Tool(format!(
                "`item` is missing the declared key attribute `{d}`"
            )));
        }
    }
    let condition = opt_string(a, "condition_expression")?;
    let expr_values = merged_expr_values(a, bound_params)?;
    let expr_names = opt_name_map(a, "expression_attribute_names")?;

    timed(
        spec,
        client
            .put_item()
            .table_name(&spec.table)
            .set_item(Some(item))
            .set_condition_expression(condition)
            .set_expression_attribute_values(expr_values)
            .set_expression_attribute_names(expr_names)
            .send(),
    )
    .await?
    .map_err(map_sdk_err)?;

    Ok(json!({ "ok": true }))
}

async fn delete_item(
    client: &Client,
    spec: &DynamodbSpec,
    args: &Value,
    bound_params: &HashMap<String, AttributeValue>,
) -> Result<Value, OpError> {
    let a = args_obj(args)?;
    let key_v = a
        .get("key")
        .ok_or_else(|| OpError::Tool("`key` is required".into()))?;
    let key = marshal_item(key_v, "key")?;
    validate_key(&key, spec)?;
    let condition = opt_string(a, "condition_expression")?;
    let expr_values = merged_expr_values(a, bound_params)?;
    let expr_names = opt_name_map(a, "expression_attribute_names")?;

    timed(
        spec,
        client
            .delete_item()
            .table_name(&spec.table)
            .set_key(Some(key))
            .set_condition_expression(condition)
            .set_expression_attribute_values(expr_values)
            .set_expression_attribute_names(expr_names)
            .send(),
    )
    .await?
    .map_err(map_sdk_err)?;

    Ok(json!({ "ok": true }))
}

/// Resolve the caller's optional `return_values` into the SDK `ReturnValue`,
/// defaulting to `ALL_NEW` so an `update_item` echoes the post-update item.
/// Unknown values are rejected as a tool error (the SDK `from(&str)` never
/// fails — it produces an `Unknown` variant — so the set is validated here).
fn resolve_return_values(
    args: &serde_json::Map<String, Value>,
) -> Result<aws_sdk_dynamodb::types::ReturnValue, OpError> {
    use aws_sdk_dynamodb::types::ReturnValue;
    match args.get("return_values") {
        None | Some(Value::Null) => Ok(ReturnValue::AllNew),
        Some(Value::String(s)) => match s.as_str() {
            "ALL_NEW" => Ok(ReturnValue::AllNew),
            "ALL_OLD" => Ok(ReturnValue::AllOld),
            "UPDATED_NEW" => Ok(ReturnValue::UpdatedNew),
            "UPDATED_OLD" => Ok(ReturnValue::UpdatedOld),
            "NONE" => Ok(ReturnValue::None),
            other => Err(OpError::Tool(format!(
                "`return_values` must be one of ALL_NEW/ALL_OLD/UPDATED_NEW/UPDATED_OLD/NONE, got `{other}`"
            ))),
        },
        Some(_) => Err(OpError::Tool("`return_values` must be a string".into())),
    }
}

async fn update_item(
    client: &Client,
    spec: &DynamodbSpec,
    args: &Value,
    bound_params: &HashMap<String, AttributeValue>,
) -> Result<Value, OpError> {
    let a = args_obj(args)?;
    let key_v = a
        .get("key")
        .ok_or_else(|| OpError::Tool("`key` is required".into()))?;
    let key = marshal_item(key_v, "key")?;
    validate_key(&key, spec)?;
    let update_expr = opt_string(a, "update_expression")?
        .ok_or_else(|| OpError::Tool("`update_expression` is required for update_item".into()))?;
    let condition = opt_string(a, "condition_expression")?;
    // Bound values arrive via the shared CEL→AttributeValue param path merged
    // with caller `expression_attribute_values`; they are referenced by
    // placeholder in the update expression, never interpolated into it.
    let expr_values = merged_expr_values(a, bound_params)?;
    let expr_names = opt_name_map(a, "expression_attribute_names")?;
    let return_values = resolve_return_values(a)?;

    let out = timed(
        spec,
        client
            .update_item()
            .table_name(&spec.table)
            .set_key(Some(key))
            .update_expression(update_expr)
            .set_condition_expression(condition)
            .set_expression_attribute_values(expr_values)
            .set_expression_attribute_names(expr_names)
            .return_values(return_values)
            .send(),
    )
    .await?
    .map_err(map_sdk_err)?;

    let attributes = out.attributes().map(item_to_json);
    Ok(json!({ "attributes": attributes }))
}

async fn query(
    client: &Client,
    spec: &DynamodbSpec,
    args: &Value,
    bound_params: &HashMap<String, AttributeValue>,
) -> Result<Value, OpError> {
    let a = args_obj(args)?;
    let kce = opt_string(a, "key_condition_expression")?
        .ok_or_else(|| OpError::Tool("`key_condition_expression` is required for query".into()))?;
    let filter = opt_string(a, "filter_expression")?;
    let projection = opt_string(a, "projection_expression")?;
    let expr_values = merged_expr_values(a, bound_params)?;
    let expr_names = opt_name_map(a, "expression_attribute_names")?;
    let start_key = opt_start_key(a)?;
    let consistent = a
        .get("consistent_read")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let forward = a
        .get("scan_index_forward")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let limit = clamp_limit(a, spec);

    let out = timed(
        spec,
        client
            .query()
            .table_name(&spec.table)
            .key_condition_expression(kce)
            .set_filter_expression(filter)
            .set_projection_expression(projection)
            .set_expression_attribute_values(expr_values)
            .set_expression_attribute_names(expr_names)
            .set_exclusive_start_key(start_key)
            .consistent_read(consistent)
            .scan_index_forward(forward)
            .limit(limit)
            .send(),
    )
    .await?
    .map_err(map_sdk_err)?;

    Ok(page_result(
        out.items(),
        out.count(),
        out.scanned_count(),
        out.last_evaluated_key(),
    ))
}

async fn scan(
    client: &Client,
    spec: &DynamodbSpec,
    args: &Value,
    bound_params: &HashMap<String, AttributeValue>,
) -> Result<Value, OpError> {
    let a = args_obj(args)?;
    let filter = opt_string(a, "filter_expression")?;
    let projection = opt_string(a, "projection_expression")?;
    let expr_values = merged_expr_values(a, bound_params)?;
    let expr_names = opt_name_map(a, "expression_attribute_names")?;
    let start_key = opt_start_key(a)?;
    let limit = clamp_limit(a, spec);

    let out = timed(
        spec,
        client
            .scan()
            .table_name(&spec.table)
            .set_filter_expression(filter)
            .set_projection_expression(projection)
            .set_expression_attribute_values(expr_values)
            .set_expression_attribute_names(expr_names)
            .set_exclusive_start_key(start_key)
            .limit(limit)
            .send(),
    )
    .await?
    .map_err(map_sdk_err)?;

    Ok(page_result(
        out.items(),
        out.count(),
        out.scanned_count(),
        out.last_evaluated_key(),
    ))
}

/// Read a required `keys` array arg, marshalling each element into an item
/// map and validating it against the declared key schema. Enforces the AWS
/// 25-item batch ceiling — a >25 request is rejected with a clear tool error
/// rather than silently truncated.
fn batch_keys(
    a: &serde_json::Map<String, Value>,
    spec: &DynamodbSpec,
) -> Result<Vec<HashMap<String, AttributeValue>>, OpError> {
    let arr = a.get("keys").and_then(Value::as_array).ok_or_else(|| {
        OpError::Tool("`keys` (array of DynamoDB-JSON key maps) is required".into())
    })?;
    if arr.is_empty() {
        return Err(OpError::Tool("`keys` must not be empty".into()));
    }
    if arr.len() > BATCH_MAX_ITEMS {
        return Err(OpError::Tool(format!(
            "`keys` has {} entries, exceeding the DynamoDB batch limit of {BATCH_MAX_ITEMS}",
            arr.len()
        )));
    }
    let mut keys = Vec::with_capacity(arr.len());
    for (i, k) in arr.iter().enumerate() {
        let key = marshal_item(k, &format!("keys[{i}]"))?;
        validate_key(&key, spec)?;
        keys.push(key);
    }
    Ok(keys)
}

async fn batch_get(client: &Client, spec: &DynamodbSpec, args: &Value) -> Result<Value, OpError> {
    let a = args_obj(args)?;
    let keys = batch_keys(a, spec)?;
    let consistent = a
        .get("consistent_read")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let projection = opt_string(a, "projection_expression")?;
    let expr_names = opt_name_map(a, "expression_attribute_names")?;

    let kaa = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
        .set_keys(Some(keys))
        .consistent_read(consistent)
        .set_projection_expression(projection)
        .set_expression_attribute_names(expr_names)
        .build()
        .map_err(|e| OpError::Tool(format!("invalid batch_get request: {e}")))?;

    let out = timed(
        spec,
        client
            .batch_get_item()
            .request_items(&spec.table, kaa)
            .send(),
    )
    .await?
    .map_err(map_sdk_err)?;

    // `responses` is keyed by table name; this binding addresses one table.
    let items: Vec<Value> = out
        .responses()
        .and_then(|m| m.get(&spec.table))
        .map(|rows| rows.iter().map(item_to_json).collect())
        .unwrap_or_default();
    // Surface any unprocessed keys (capacity throttling) so a caller can retry.
    let unprocessed = out
        .unprocessed_keys()
        .and_then(|m| m.get(&spec.table))
        .map(|kaa| kaa.keys().iter().map(item_to_json).collect::<Vec<_>>())
        .unwrap_or_default();
    Ok(json!({
        "items": items,
        "count": items.len(),
        "unprocessed_keys": unprocessed,
    }))
}

/// Build the `WriteRequest` list for a BatchWriteItem from the `requests`
/// arg: each entry is exactly one of `{ put: <item> }` / `{ delete: <key> }`.
/// Enforces the 25-item AWS ceiling (a larger request is rejected, not
/// truncated), the declared key schema (put must carry the key; delete keys
/// are validated), and the one-of shape. Network-free → unit-testable.
fn build_write_requests(
    a: &serde_json::Map<String, Value>,
    spec: &DynamodbSpec,
) -> Result<Vec<aws_sdk_dynamodb::types::WriteRequest>, OpError> {
    use aws_sdk_dynamodb::types::{DeleteRequest, PutRequest, WriteRequest};
    let reqs = a.get("requests").and_then(Value::as_array).ok_or_else(|| {
        OpError::Tool(
            "`requests` (array of { put: <item> } | { delete: <key> }) is required".into(),
        )
    })?;
    if reqs.is_empty() {
        return Err(OpError::Tool("`requests` must not be empty".into()));
    }
    if reqs.len() > BATCH_MAX_ITEMS {
        return Err(OpError::Tool(format!(
            "`requests` has {} entries, exceeding the DynamoDB batch limit of {BATCH_MAX_ITEMS}",
            reqs.len()
        )));
    }

    let mut writes = Vec::with_capacity(reqs.len());
    for (i, r) in reqs.iter().enumerate() {
        let obj = r
            .as_object()
            .ok_or_else(|| OpError::Tool(format!("`requests[{i}]` must be an object")))?;
        let put = obj.get("put");
        let del = obj.get("delete");
        let write = match (put, del) {
            (Some(item_v), None) => {
                let item = marshal_item(item_v, &format!("requests[{i}].put"))?;
                for d in spec.key_names() {
                    if !item.contains_key(d) {
                        return Err(OpError::Tool(format!(
                            "`requests[{i}].put` is missing the declared key attribute `{d}`"
                        )));
                    }
                }
                let pr = PutRequest::builder()
                    .set_item(Some(item))
                    .build()
                    .map_err(|e| OpError::Tool(format!("requests[{i}].put: {e}")))?;
                WriteRequest::builder().put_request(pr).build()
            }
            (None, Some(key_v)) => {
                let key = marshal_item(key_v, &format!("requests[{i}].delete"))?;
                validate_key(&key, spec)?;
                let dr = DeleteRequest::builder()
                    .set_key(Some(key))
                    .build()
                    .map_err(|e| OpError::Tool(format!("requests[{i}].delete: {e}")))?;
                WriteRequest::builder().delete_request(dr).build()
            }
            (Some(_), Some(_)) => {
                return Err(OpError::Tool(format!(
                    "`requests[{i}]` must have exactly one of `put` / `delete`, not both"
                )));
            }
            (None, None) => {
                return Err(OpError::Tool(format!(
                    "`requests[{i}]` must have one of `put` / `delete`"
                )));
            }
        };
        writes.push(write);
    }
    Ok(writes)
}

async fn batch_write(
    client: &Client,
    spec: &DynamodbSpec,
    args: &Value,
    _bound_params: &HashMap<String, AttributeValue>,
) -> Result<Value, OpError> {
    let a = args_obj(args)?;
    let writes = build_write_requests(a, spec)?;

    let out = timed(
        spec,
        client
            .batch_write_item()
            .request_items(&spec.table, writes)
            .send(),
    )
    .await?
    .map_err(map_sdk_err)?;

    // Unprocessed write requests (capacity throttling) round-trip back as
    // DynamoDB-JSON so a caller can resubmit them.
    let unprocessed: Vec<Value> = out
        .unprocessed_items()
        .and_then(|m| m.get(&spec.table))
        .map(|ws| ws.iter().map(write_request_to_json).collect())
        .unwrap_or_default();
    Ok(json!({ "ok": true, "unprocessed_items": unprocessed }))
}

/// Render an unprocessed `WriteRequest` back into the `{ put | delete }` arg
/// shape so a caller can resubmit it verbatim.
fn write_request_to_json(w: &aws_sdk_dynamodb::types::WriteRequest) -> Value {
    if let Some(pr) = w.put_request() {
        json!({ "put": item_to_json(pr.item()) })
    } else if let Some(dr) = w.delete_request() {
        json!({ "delete": item_to_json(dr.key()) })
    } else {
        json!({})
    }
}

const CURSOR_B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Encode a DynamoDB `last_evaluated_key` into an opaque pagination cursor —
/// base64(URL-safe) of its DynamoDB-JSON form.
pub(crate) fn encode_cursor(key: &HashMap<String, AttributeValue>) -> String {
    use base64::Engine as _;
    let json = item_to_json(key);
    CURSOR_B64.encode(serde_json::to_vec(&json).unwrap_or_default())
}

/// Decode an opaque pagination cursor back into a DynamoDB exclusive-start-key.
/// A malformed cursor is a caller/transport error, not an operator bug.
pub(crate) fn decode_cursor(cursor: &str) -> Result<HashMap<String, AttributeValue>, BackendError> {
    use base64::Engine as _;
    let bytes = CURSOR_B64
        .decode(cursor)
        .map_err(|_| BackendError::InvalidSpec {
            message: "list cursor is not valid base64".to_owned(),
        })?;
    let json: Value = serde_json::from_slice(&bytes).map_err(|_| BackendError::InvalidSpec {
        message: "list cursor is not valid JSON".to_owned(),
    })?;
    json_to_item(&json).map_err(|e| BackendError::InvalidSpec {
        message: format!("list cursor is not valid DynamoDB-JSON: {e}"),
    })
}

/// Run the operator-fixed listing Scan for `resources/list`. All inputs are
/// operator-fixed; `start_key` is the decoded pagination cursor. Returns the
/// raw items plus the next `last_evaluated_key` (the page cursor source).
pub(crate) async fn run_list_scan(
    client: &Client,
    spec: &DynamodbSpec,
    list: &crate::config::ListQueryConfig,
    start_key: Option<HashMap<String, AttributeValue>>,
) -> Result<
    (
        Vec<HashMap<String, AttributeValue>>,
        Option<HashMap<String, AttributeValue>>,
    ),
    BackendError,
> {
    let expr_values = match &list.expression_attribute_values {
        Some(v) => Some(json_to_item(v).map_err(|e| BackendError::InvalidSpec {
            message: format!("list_query.expression_attribute_values: {e}"),
        })?),
        None => None,
    };
    let limit = list.page_size.min(spec.limits.max_page_size).max(1);

    let out = timed(
        spec,
        client
            .scan()
            .table_name(&spec.table)
            .set_filter_expression(list.filter_expression.clone())
            .set_projection_expression(list.projection_expression.clone())
            .set_expression_attribute_values(expr_values)
            .set_expression_attribute_names(list.expression_attribute_names.clone())
            .set_exclusive_start_key(start_key)
            .limit(limit)
            .send(),
    )
    .await
    .map_err(|e| match e {
        OpError::Backend(b) => b,
        OpError::Tool(m) => BackendError::Transport { message: m },
    })?
    .map_err(map_sdk_err)?;

    Ok((out.items().to_vec(), out.last_evaluated_key().cloned()))
}

/// Run the operator-fixed completion Query, binding `:prefix` to the caller's
/// typed prefix as an `S` AttributeValue. Returns the raw matched items.
pub(crate) async fn run_completion_query(
    client: &Client,
    spec: &DynamodbSpec,
    cc: &crate::config::CompletionConfig,
    prefix: &str,
    max_results: i32,
) -> Result<Vec<HashMap<String, AttributeValue>>, BackendError> {
    let mut values: HashMap<String, AttributeValue> = match &cc.expression_attribute_values {
        Some(v) => json_to_item(v).map_err(|e| BackendError::InvalidSpec {
            message: format!("variable_completions.expression_attribute_values: {e}"),
        })?,
        None => HashMap::new(),
    };
    // The caller prefix crosses as a bound value, never interpolated into the
    // operator-fixed key-condition expression.
    values.insert(":prefix".to_owned(), AttributeValue::S(prefix.to_owned()));

    let limit = max_results.min(spec.limits.max_page_size).max(1);

    let out = timed(
        spec,
        client
            .query()
            .table_name(&spec.table)
            .key_condition_expression(cc.key_condition_expression.clone())
            .set_expression_attribute_values(Some(values))
            .set_expression_attribute_names(cc.expression_attribute_names.clone())
            .limit(limit)
            .send(),
    )
    .await
    .map_err(|e| match e {
        OpError::Backend(b) => b,
        OpError::Tool(m) => BackendError::Transport { message: m },
    })?
    .map_err(map_sdk_err)?;

    Ok(out.items().to_vec())
}

/// JSON Schema (draft 2020-12) for the operation result envelope DynamoDB
/// emits. Unlike the warehouse backends, the body is operation-specific:
/// `get_item` → `{item}`, `put_item`/`delete_item` → `{ok}`, `update_item`
/// → `{attributes}`, `query`/`scan` → `{items, count, scanned_count,
/// last_evaluated_key}`, `batch_get` → `{items, count, unprocessed_keys}`,
/// `batch_write` → `{ok, unprocessed_items}`, and any tool-level failure →
/// the gateway verbatim-result wrapper `{__mcpg_verbatim_result}`.
/// The schema lists every key these shapes produce and stays open
/// (`additionalProperties: true`) with untyped item shapes so no real result
/// ever fails validation.
pub(crate) fn result_envelope_schema() -> Value {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "item": { "type": ["object", "null"] },
            "ok": { "type": "boolean" },
            "items": { "type": "array", "items": {} },
            "count": { "type": "integer" },
            "scanned_count": { "type": "integer" },
            "last_evaluated_key": { "type": ["object", "null"] },
            "attributes": { "type": ["object", "null"] },
            "unprocessed_keys": { "type": "array", "items": {} },
            "unprocessed_items": { "type": "array", "items": {} },
            "__mcpg_verbatim_result": { "type": "object" }
        },
        "additionalProperties": true
    })
}

fn page_result(
    items: &[HashMap<String, AttributeValue>],
    count: i32,
    scanned: i32,
    last_key: Option<&HashMap<String, AttributeValue>>,
) -> Value {
    json!({
        "items": items.iter().map(item_to_json).collect::<Vec<_>>(),
        "count": count,
        "scanned_count": scanned,
        "last_evaluated_key": last_key.map(item_to_json),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> DynamodbSpec {
        DynamodbSpec::parse(&json!({
            "region": "us-east-1",
            "table": "orders",
            "operation": "get_item",
            "partition_key": { "name": "order_id", "type": "S" },
            "limits": { "max_page_size": 50 }
        }))
        .unwrap()
    }

    #[test]
    fn validate_key_accepts_declared() {
        let key = json_to_item(&json!({ "order_id": { "S": "o-1" } })).unwrap();
        assert!(validate_key(&key, &spec()).is_ok());
    }

    #[test]
    fn validate_key_rejects_extra_attr() {
        let key =
            json_to_item(&json!({ "order_id": { "S": "o-1" }, "evil": { "S": "x" } })).unwrap();
        assert!(matches!(validate_key(&key, &spec()), Err(OpError::Tool(_))));
    }

    #[test]
    fn validate_key_rejects_missing() {
        let key = json_to_item(&json!({ "other": { "S": "x" } })).unwrap();
        assert!(matches!(validate_key(&key, &spec()), Err(OpError::Tool(_))));
    }

    #[test]
    fn merged_expr_values_overrides_caller_with_binding_params() {
        let args = json!({
            "expression_attribute_values": {
                ":caller": { "S": "from-caller" },
                ":shared": { "S": "caller-wins?" }
            }
        });
        let mut bound = HashMap::new();
        bound.insert(":bound".to_owned(), AttributeValue::N("5".into()));
        bound.insert(
            ":shared".to_owned(),
            AttributeValue::S("binding-wins".into()),
        );
        let merged = merged_expr_values(args.as_object().unwrap(), &bound)
            .ok()
            .flatten()
            .expect("merged map");
        assert_eq!(merged[":caller"], AttributeValue::S("from-caller".into()));
        assert_eq!(merged[":bound"], AttributeValue::N("5".into()));
        // Binding param wins the collision (caller can't shadow it).
        assert_eq!(merged[":shared"], AttributeValue::S("binding-wins".into()));
    }

    #[test]
    fn merged_expr_values_none_when_both_empty() {
        let args = json!({});
        let bound = HashMap::new();
        assert!(
            merged_expr_values(args.as_object().unwrap(), &bound)
                .ok()
                .flatten()
                .is_none()
        );
    }

    #[test]
    fn clamp_limit_caps_and_defaults() {
        let s = spec();
        assert_eq!(
            clamp_limit(json!({ "limit": 9999 }).as_object().unwrap(), &s),
            50
        );
        assert_eq!(
            clamp_limit(json!({ "limit": 10 }).as_object().unwrap(), &s),
            10
        );
        assert_eq!(clamp_limit(json!({}).as_object().unwrap(), &s), 50);
    }

    #[test]
    fn page_result_shape() {
        let items = vec![json_to_item(&json!({ "order_id": { "S": "o-1" } })).unwrap()];
        let r = page_result(&items, 1, 1, None);
        assert_eq!(r["count"], 1);
        assert_eq!(r["items"][0]["order_id"]["S"], "o-1");
        assert!(r["last_evaluated_key"].is_null());
    }

    #[test]
    fn cursor_round_trips_last_evaluated_key() {
        let key = json_to_item(&json!({ "order_id": { "S": "o-9" }, "n": { "N": "3" } })).unwrap();
        let cursor = encode_cursor(&key);
        let back = decode_cursor(&cursor).unwrap();
        assert_eq!(item_to_json(&back), item_to_json(&key));
    }

    #[test]
    fn decode_cursor_rejects_garbage() {
        assert!(matches!(
            decode_cursor("!!!not base64!!!"),
            Err(BackendError::InvalidSpec { .. })
        ));
    }

    fn spec_with_op(op: &str) -> DynamodbSpec {
        DynamodbSpec::parse(&json!({
            "region": "us-east-1",
            "table": "orders",
            "operation": op,
            "partition_key": { "name": "order_id", "type": "S" },
            "limits": { "max_page_size": 50 }
        }))
        .unwrap()
    }

    #[test]
    fn resolve_return_values_defaults_all_new() {
        use aws_sdk_dynamodb::types::ReturnValue;
        let rv = resolve_return_values(json!({}).as_object().unwrap()).unwrap();
        assert_eq!(rv, ReturnValue::AllNew);
        let rv =
            resolve_return_values(json!({ "return_values": "NONE" }).as_object().unwrap()).unwrap();
        assert_eq!(rv, ReturnValue::None);
    }

    #[test]
    fn resolve_return_values_rejects_unknown() {
        let err =
            resolve_return_values(json!({ "return_values": "BOGUS" }).as_object().unwrap()).err();
        assert!(matches!(err, Some(OpError::Tool(_))));
    }

    #[test]
    fn update_item_binds_values_via_cel_param_path() {
        // The CEL→AttributeValue bound params merge into the request's
        // ExpressionAttributeValues; the update expression references them by
        // placeholder, never interpolating the value.
        let mut bound = HashMap::new();
        bound.insert(":status".to_owned(), AttributeValue::S("SHIPPED".into()));
        let args = json!({
            "key": { "order_id": { "S": "o-1" } },
            "update_expression": "SET #s = :status",
            "expression_attribute_names": { "#s": "status" },
            "expression_attribute_values": { ":caller": { "N": "1" } }
        });
        let merged = merged_expr_values(args.as_object().unwrap(), &bound)
            .ok()
            .flatten()
            .expect("merged values");
        assert_eq!(merged[":status"], AttributeValue::S("SHIPPED".into()));
        assert_eq!(merged[":caller"], AttributeValue::N("1".into()));
        // The bound value is carried as an AttributeValue, not spliced into the
        // expression text.
        assert!(
            !args["update_expression"]
                .as_str()
                .unwrap()
                .contains("SHIPPED")
        );
    }

    #[test]
    fn batch_keys_validates_and_caps_at_25() {
        let spec = spec_with_op("batch_get");
        // Valid set of keys.
        let args =
            json!({ "keys": [ { "order_id": { "S": "o-1" } }, { "order_id": { "S": "o-2" } } ] });
        let keys = batch_keys(args.as_object().unwrap(), &spec).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0]["order_id"], AttributeValue::S("o-1".into()));

        // 26 keys → rejected with a clear over-limit tool error.
        let many: Vec<Value> = (0..26)
            .map(|i| json!({ "order_id": { "S": format!("o-{i}") } }))
            .collect();
        let err = batch_keys(json!({ "keys": many }).as_object().unwrap(), &spec).err();
        match err {
            Some(OpError::Tool(m)) => assert!(m.contains("batch limit of 25"), "{m}"),
            other => panic!("expected over-limit tool error, got {other:?}"),
        }
    }

    #[test]
    fn batch_keys_rejects_non_key_attribute() {
        let spec = spec_with_op("batch_get");
        let args = json!({ "keys": [ { "evil": { "S": "x" } } ] });
        assert!(matches!(
            batch_keys(args.as_object().unwrap(), &spec),
            Err(OpError::Tool(_))
        ));
    }

    #[test]
    fn batch_keys_rejects_empty() {
        let spec = spec_with_op("batch_get");
        assert!(matches!(
            batch_keys(json!({ "keys": [] }).as_object().unwrap(), &spec),
            Err(OpError::Tool(_))
        ));
    }

    #[test]
    fn build_write_requests_shapes_put_and_delete() {
        let spec = spec_with_op("batch_write");
        let args = json!({
            "requests": [
                { "put": { "order_id": { "S": "o-1" }, "qty": { "N": "3" } } },
                { "delete": { "order_id": { "S": "o-2" } } }
            ]
        });
        let writes = build_write_requests(args.as_object().unwrap(), &spec).unwrap();
        assert_eq!(writes.len(), 2);
        // First is a PutRequest carrying the full item.
        let pr = writes[0].put_request().expect("put request");
        assert_eq!(pr.item()["order_id"], AttributeValue::S("o-1".into()));
        assert_eq!(pr.item()["qty"], AttributeValue::N("3".into()));
        // Second is a DeleteRequest carrying just the key.
        let dr = writes[1].delete_request().expect("delete request");
        assert_eq!(dr.key()["order_id"], AttributeValue::S("o-2".into()));
    }

    #[test]
    fn build_write_requests_rejects_over_25() {
        let spec = spec_with_op("batch_write");
        let many: Vec<Value> = (0..26)
            .map(|i| json!({ "put": { "order_id": { "S": format!("o-{i}") } } }))
            .collect();
        let err =
            build_write_requests(json!({ "requests": many }).as_object().unwrap(), &spec).err();
        match err {
            Some(OpError::Tool(m)) => assert!(m.contains("batch limit of 25"), "{m}"),
            other => panic!("expected over-limit tool error, got {other:?}"),
        }
    }

    #[test]
    fn build_write_requests_rejects_both_put_and_delete() {
        let spec = spec_with_op("batch_write");
        let args = json!({
            "requests": [ { "put": { "order_id": { "S": "o-1" } }, "delete": { "order_id": { "S": "o-1" } } } ]
        });
        assert!(matches!(
            build_write_requests(args.as_object().unwrap(), &spec),
            Err(OpError::Tool(_))
        ));
    }

    #[test]
    fn build_write_requests_rejects_put_missing_key() {
        let spec = spec_with_op("batch_write");
        let args = json!({ "requests": [ { "put": { "qty": { "N": "1" } } } ] });
        assert!(matches!(
            build_write_requests(args.as_object().unwrap(), &spec),
            Err(OpError::Tool(_))
        ));
    }

    #[test]
    fn build_write_requests_rejects_delete_non_key_attr() {
        let spec = spec_with_op("batch_write");
        let args = json!({ "requests": [ { "delete": { "evil": { "S": "x" } } } ] });
        assert!(matches!(
            build_write_requests(args.as_object().unwrap(), &spec),
            Err(OpError::Tool(_))
        ));
    }

    #[test]
    fn write_request_round_trips_to_json() {
        use aws_sdk_dynamodb::types::{DeleteRequest, PutRequest, WriteRequest};
        let put = WriteRequest::builder()
            .put_request(
                PutRequest::builder()
                    .set_item(Some(
                        json_to_item(&json!({ "order_id": { "S": "o-1" } })).unwrap(),
                    ))
                    .build()
                    .unwrap(),
            )
            .build();
        assert_eq!(
            write_request_to_json(&put),
            json!({ "put": { "order_id": { "S": "o-1" } } })
        );

        let del = WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .set_key(Some(
                        json_to_item(&json!({ "order_id": { "S": "o-2" } })).unwrap(),
                    ))
                    .build()
                    .unwrap(),
            )
            .build();
        assert_eq!(
            write_request_to_json(&del),
            json!({ "delete": { "order_id": { "S": "o-2" } } })
        );
    }

    #[test]
    fn output_schema_covers_op_result_keys() {
        let schema = result_envelope_schema();
        assert_eq!(schema["type"], json!("object"));
        let props = schema["properties"].as_object().expect("properties object");
        // Every key the per-op result shapes produce is declared.
        let items = vec![json_to_item(&json!({ "order_id": { "S": "o-1" } })).unwrap()];
        let page = page_result(&items, 1, 1, None);
        for key in page.as_object().expect("page object").keys() {
            assert!(props.contains_key(key), "schema missing key `{key}`");
        }
        for key in ["item", "ok", "__mcpg_verbatim_result"] {
            assert!(props.contains_key(key), "schema missing key `{key}`");
        }
        // Loose item typing — any returned item shape validates.
        assert_eq!(schema["properties"]["items"]["items"], json!({}));
    }
}
