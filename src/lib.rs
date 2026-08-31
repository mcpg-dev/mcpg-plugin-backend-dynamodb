//! `dev.mcpg.backend.dynamodb` — Amazon DynamoDB backend binding plugin.
//!
//! One binding == one DynamoDB operation on one operator-fixed table
//! (the `soap`/`ldap` envelope model): the operator declares
//! `backend: { kind: dynamodb, table, operation, partition_key, ... }`
//! and that binding becomes one MCP tool. Per call, the tool arguments
//! (DynamoDB-JSON) are marshalled into the operation, run via the modern
//! aws-lc-rs/rustls AWS SDK under a per-call timeout, and the response is
//! shaped back into DynamoDB-JSON.
//!
//! Operations: `get_item`, `put_item`, `delete_item`, `update_item`,
//! `query`, `scan`, `batch_get`, `batch_write`. The table is operator-fixed
//! (no caller-supplied table → no allowlist injection surface); key
//! attributes are validated against the declared key schema. Read ops
//! (`get_item` / `query` / `scan` / `batch_get`) are side-effect-free; the
//! rest mutate (surfaced as `aws.mutates` on the audit metadata). Batch ops
//! honour the AWS 25-item ceiling (a larger request is rejected). PartiQL
//! and the `expand_capabilities` table-per-tool catalog are deferred
//! follow-ups.

mod config;
mod exec;
mod marshal;
mod params;
mod surface;
pub mod watch;

#[cfg(any(feature = "cdylib-export", feature = "static-firstparty"))]
mod cdylib;

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mcpg_plugin_protocol::audit::{AuditEvent, AuditOutcome};
use mcpg_plugin_protocol::types::PluginIdentity;
use mcpg_plugin_protocol::{
    BackendError, BackendHost, BackendPlugin, BackendRequest, BackendResponse, PluginManifest,
    ResourcePage, firstparty_manifest,
};
use mcpg_plugin_sdk::{HostHandle, SpanGuard};
use serde_json::{Value, json};
use tracing::debug;

use crate::config::{DynamodbSpec, Operation};
use crate::exec::OpError;
use crate::params::{
    CompiledParam, arguments_referenced_by_params, compile_params, evaluate_params,
};

/// Sentinel wrapper the gateway projects verbatim as a `CallToolResult`
/// (so we can return `isError: true` tool-level errors). Matches the host
/// + mock-backend constant.
const VERBATIM_RESULT_KEY: &str = "__mcpg_verbatim_result";

struct ProfileRuntime {
    spec: DynamodbSpec,
    client: aws_sdk_dynamodb::Client,
    /// Compiled CEL `params` (placeholder → program), bound per call into
    /// `ExpressionAttributeValues`. Empty when the binding declares no `params`.
    compiled_params: Vec<CompiledParam>,
}

pub struct DynamodbBackendPlugin {
    manifest: PluginManifest,
    // std RwLock: guards are never held across an `.await` (the client is
    // built before the write lock is taken; execute clones the Arc out
    // before awaiting), so a sync lock is correct + lets the sync trait
    // methods (`input_schema`, `audit_metadata`) read it too.
    profiles: RwLock<BTreeMap<String, Arc<ProfileRuntime>>>,
    /// Make-time host handle for the per-call observability triad (latency
    /// histogram + call counter + failure audit). `None` until installed by the
    /// cdylib factory.
    host_handle: OnceLock<HostHandle>,
}

impl Default for DynamodbBackendPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamodbBackendPlugin {
    pub fn new() -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.dynamodb",
                name: "DynamoDB Binding",
                class: Backend,
            },
            profiles: RwLock::new(BTreeMap::new()),
            host_handle: OnceLock::new(),
        }
    }

    /// No plugin-level config — per-binding connection + table details
    /// arrive via `register_profile`.
    pub fn from_config_json(_config_json: &str) -> Self {
        Self::new()
    }

    /// Install the make-time host handle (idempotent — only the first wins).
    pub fn set_host_handle(&self, host: HostHandle) -> bool {
        self.host_handle.set(host).is_ok()
    }

    fn host_handle(&self) -> Option<&HostHandle> {
        self.host_handle.get()
    }

    fn profile(&self, name: &str) -> Option<Arc<ProfileRuntime>> {
        self.profiles
            .read()
            .expect("profiles lock poisoned")
            .get(name)
            .cloned()
    }

    /// Per-call observability triad: latency histogram + call counter (both
    /// labelled by `outcome`) and — on a failure outcome — a `dev.mcpg.backend.
    /// dynamodb.*` audit event carrying the `dynamodb.transport: plugin`
    /// metadata. Mirrors the sibling warehouse backends so the metric/audit
    /// vocab is uniform across the backend class.
    async fn emit_host_observability(
        &self,
        backend_name: &str,
        outcome_label: &'static str,
        reason: Option<&str>,
        identity: Option<&PluginIdentity>,
        request_id: &str,
        duration: Duration,
    ) {
        let Some(host) = self.host_handle() else {
            return;
        };
        host.histogram(
            "mcpg_dynamodb_backend_latency_seconds",
            duration.as_secs_f64(),
            &[("outcome", outcome_label)],
        );
        host.counter(
            "mcpg_dynamodb_backend_calls_total",
            1,
            &[("outcome", outcome_label)],
        );
        if let Some(action) = audit_action_for_outcome(outcome_label) {
            let actor = identity.cloned().unwrap_or_else(synthetic_system_identity);
            let mut details = json!({
                "backend": backend_name,
                "duration_ms": duration.as_millis() as u64,
                "outcome": outcome_label,
                "alias": host.alias(),
                "dynamodb.transport": "plugin",
            });
            if let Some(reason) = reason {
                details
                    .as_object_mut()
                    .expect("json object")
                    .insert("reason".into(), Value::String(reason.to_owned()));
            }
            let event = AuditEvent {
                event_id: format!("dynamodb-{}-{}", request_id, duration.as_nanos()),
                occurred_at: rfc3339_now(),
                actor,
                action: action.to_owned(),
                resource: Some(format!("dynamodb-binding://{backend_name}")),
                outcome: AuditOutcome::Failure,
                request_id: Some(request_id.to_owned()),
                upstream_request_id: None,
                node_id: None,
                details,
                prev_event_hash: None,
            };
            let host_for_audit = host.clone();
            if let Err(join_err) = tokio::task::spawn_blocking(move || {
                let _ = host_for_audit.audit_event(event);
            })
            .await
            {
                debug!(target: "mcpg::dynamodb::host_handle", error = %join_err, "audit spawn_blocking failed");
            }
        }
    }
}

// --------------------------------------------------------------------- obs

/// Map a failure outcome label to its `dev.mcpg.backend.dynamodb.*` audit
/// action. Success / not-found outcomes carry no audit event (metrics only).
fn audit_action_for_outcome(label: &str) -> Option<&'static str> {
    match label {
        "timeout" => Some("dev.mcpg.backend.dynamodb.request_timeout"),
        "transport_error" => Some("dev.mcpg.backend.dynamodb.request_failed"),
        "tool_error" => Some("dev.mcpg.backend.dynamodb.request_rejected"),
        "invalid_spec" => Some("dev.mcpg.backend.dynamodb.request_failed"),
        _ => None,
    }
}

fn rfc3339_now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn synthetic_system_identity() -> PluginIdentity {
    PluginIdentity {
        kind: "system".into(),
        trust_level: "verified".into(),
        subject_id: Some("dev.mcpg.backend.dynamodb".into()),
        auth_provider: None,
        issuer: None,
        roles: vec![],
        groups: vec![],
        scopes: vec![],
        attributes: Default::default(),
    }
}

/// Classify an operation result into the metric/audit outcome label.
fn outcome_label_for(result: &Result<Value, OpError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(OpError::Tool(_)) => "tool_error",
        Err(OpError::Backend(BackendError::Timeout { .. })) => "timeout",
        Err(OpError::Backend(_)) => "transport_error",
    }
}

/// The human-readable reason for the audit event (failure outcomes only).
fn audit_reason_for(result: &Result<Value, OpError>) -> Option<String> {
    match result {
        Ok(_) => None,
        Err(OpError::Tool(m)) => Some(m.clone()),
        Err(OpError::Backend(e)) => Some(e.to_string()),
    }
}

fn verbatim_error(msg: &str) -> Vec<u8> {
    let envelope = json!({
        VERBATIM_RESULT_KEY: {
            "content": [ { "type": "text", "text": msg } ],
            "isError": true,
        }
    });
    serde_json::to_vec(&envelope).unwrap_or_else(|_| b"{}".to_vec())
}

fn op_input_schema(spec: &DynamodbSpec) -> Value {
    // Each declared key attribute is a DynamoDB-JSON AttributeValue whose
    // single type tag is fixed by the declared key type (S/N/B).
    let key_props: serde_json::Map<String, Value> = spec
        .key_attrs()
        .into_iter()
        .map(|k| {
            let tag = k.key_type.av_tag();
            (
                k.name.clone(),
                json!({
                    "type": "object",
                    "description": format!("DynamoDB-JSON AttributeValue, e.g. {{\"{tag}\":...}}"),
                }),
            )
        })
        .collect();
    let key_schema = json!({
        "type": "object",
        "properties": key_props,
        "required": spec.key_names(),
        "additionalProperties": false,
    });
    match spec.operation {
        Operation::GetItem => json!({
            "type": "object",
            "properties": {
                "key": key_schema,
                "consistent_read": { "type": "boolean" },
                "projection_expression": { "type": "string" }
            },
            "required": ["key"]
        }),
        Operation::PutItem => json!({
            "type": "object",
            "properties": {
                "item": { "type": "object", "description": "Full item as a DynamoDB-JSON map (must include the declared key attributes)" },
                "condition_expression": { "type": "string" },
                "expression_attribute_values": { "type": "object" },
                "expression_attribute_names": { "type": "object" }
            },
            "required": ["item"]
        }),
        Operation::DeleteItem => json!({
            "type": "object",
            "properties": {
                "key": key_schema,
                "condition_expression": { "type": "string" },
                "expression_attribute_values": { "type": "object" },
                "expression_attribute_names": { "type": "object" }
            },
            "required": ["key"]
        }),
        Operation::UpdateItem => json!({
            "type": "object",
            "properties": {
                "key": key_schema,
                "update_expression": { "type": "string", "description": "DynamoDB UpdateExpression, e.g. `SET #s = :status`" },
                "condition_expression": { "type": "string" },
                "expression_attribute_values": { "type": "object", "description": "DynamoDB-JSON values; bound values may also come from the binding's CEL `params` (never interpolated)" },
                "expression_attribute_names": { "type": "object" },
                "return_values": { "type": "string", "enum": ["ALL_NEW", "ALL_OLD", "UPDATED_NEW", "UPDATED_OLD", "NONE"], "description": "Defaults to ALL_NEW" }
            },
            "required": ["key", "update_expression"]
        }),
        Operation::BatchGet => json!({
            "type": "object",
            "properties": {
                "keys": {
                    "type": "array",
                    "description": "Up to 25 DynamoDB-JSON key maps",
                    "items": key_schema,
                    "maxItems": 25
                },
                "consistent_read": { "type": "boolean" },
                "projection_expression": { "type": "string" },
                "expression_attribute_names": { "type": "object" }
            },
            "required": ["keys"]
        }),
        Operation::BatchWrite => json!({
            "type": "object",
            "properties": {
                "requests": {
                    "type": "array",
                    "description": "Up to 25 write requests; each is exactly one of { put: <DynamoDB-JSON item> } or { delete: <DynamoDB-JSON key> }",
                    "items": {
                        "type": "object",
                        "properties": {
                            "put": { "type": "object" },
                            "delete": { "type": "object" }
                        },
                        "additionalProperties": false
                    },
                    "maxItems": 25
                }
            },
            "required": ["requests"]
        }),
        Operation::Query => json!({
            "type": "object",
            "properties": {
                "key_condition_expression": { "type": "string" },
                "filter_expression": { "type": "string" },
                "projection_expression": { "type": "string" },
                "expression_attribute_values": { "type": "object" },
                "expression_attribute_names": { "type": "object" },
                "exclusive_start_key": { "type": "object" },
                "consistent_read": { "type": "boolean" },
                "scan_index_forward": { "type": "boolean" },
                "limit": { "type": "integer" }
            },
            "required": ["key_condition_expression"]
        }),
        Operation::Scan => json!({
            "type": "object",
            "properties": {
                "filter_expression": { "type": "string" },
                "projection_expression": { "type": "string" },
                "expression_attribute_values": { "type": "object" },
                "expression_attribute_names": { "type": "object" },
                "exclusive_start_key": { "type": "object" },
                "limit": { "type": "integer" }
            }
        }),
    }
}

#[async_trait]
impl BackendPlugin for DynamodbBackendPlugin {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        "dynamodb"
    }

    async fn register_profile(
        &self,
        profile_name: &str,
        spec: &Value,
        _host: Arc<dyn BackendHost>,
    ) -> Result<(), BackendError> {
        let parsed = DynamodbSpec::parse(spec).map_err(|e| BackendError::InvalidSpec {
            message: e.to_string(),
        })?;
        // Compile the CEL params once at register (offline) so a bad expression
        // fails fast rather than per call.
        let compiled_params = compile_params(&parsed.params)
            .map_err(|message| BackendError::InvalidSpec { message })?;
        // Build the client (async default-chain load) BEFORE taking the
        // write lock so no guard is held across an await.
        let client = exec::build_client(&parsed).await;
        let runtime = Arc::new(ProfileRuntime {
            spec: parsed,
            client,
            compiled_params,
        });
        self.profiles
            .write()
            .expect("profiles lock poisoned")
            .insert(profile_name.to_owned(), runtime);
        Ok(())
    }

    async fn execute(
        &self,
        profile_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        let started = Instant::now();
        let request_id = request.request_id.clone();
        let identity = request.identity.clone();
        let host_span: Option<SpanGuard> = self.host_handle().map(|h| {
            h.span(
                "dynamodb_backend.execute",
                json!({ "backend": profile_name, "request_id": request_id }),
            )
        });

        let Some(profile) = self.profile(profile_name) else {
            let err = BackendError::ProfileNotFound {
                backend_name: profile_name.to_owned(),
            };
            self.emit_host_observability(
                profile_name,
                "profile_not_found",
                Some(&err.to_string()),
                identity.as_ref(),
                &request_id,
                started.elapsed(),
            )
            .await;
            drop(host_span);
            return Err(err);
        };

        let args: Value = if request.payload.is_empty() {
            json!({})
        } else {
            match serde_json::from_slice(&request.payload) {
                Ok(v) => v,
                Err(e) => {
                    let message = format!("invalid tool arguments JSON: {e}");
                    self.emit_host_observability(
                        profile_name,
                        "tool_error",
                        Some(&message),
                        identity.as_ref(),
                        &request_id,
                        started.elapsed(),
                    )
                    .await;
                    drop(host_span);
                    return Ok(BackendResponse {
                        payload: verbatim_error(&message),
                        truncated: false,
                    });
                }
            }
        };

        // Evaluate the CEL params (connection-free) and lower each to a scalar
        // AttributeValue — a failure here is a caller/argument problem (tool
        // error), not a backend fault.
        let bound_params = match evaluate_params(&profile.compiled_params, &args) {
            Ok(map) => map,
            Err(e) => {
                let message = format!("evaluating params: {e}");
                self.emit_host_observability(
                    profile_name,
                    "invalid_spec",
                    Some(&message),
                    identity.as_ref(),
                    &request_id,
                    started.elapsed(),
                )
                .await;
                drop(host_span);
                return Ok(BackendResponse {
                    payload: verbatim_error(&message),
                    truncated: false,
                });
            }
        };

        let op_result =
            exec::execute_op(&profile.client, &profile.spec, &args, &bound_params).await;
        let outcome_label = outcome_label_for(&op_result);
        let audit_reason = audit_reason_for(&op_result);
        self.emit_host_observability(
            profile_name,
            outcome_label,
            audit_reason.as_deref(),
            identity.as_ref(),
            &request_id,
            started.elapsed(),
        )
        .await;
        drop(host_span);

        match op_result {
            Ok(result) => {
                // On the resource/prompt surfaces the gateway decoder requires a
                // surface-shaped body; the tool surface keeps the historical op
                // result. A resource read with no resolvable URI returns a
                // tool-level error (so the resource decoder sees a clean error
                // rather than an invalid `{contents}`).
                let body = match profile.spec.surface {
                    surface::Surface::Tool => result,
                    surface::Surface::Resource => {
                        match surface::resolve_resource_uri(profile.spec.uri.as_deref(), &args) {
                            Some(uri) => surface::resource_contents_body(uri, &result),
                            None => {
                                return Ok(BackendResponse {
                                    payload: verbatim_error(
                                        "resource surface requires a `uri` (set a static `uri` on the binding or invoke via a resources/read request)",
                                    ),
                                    truncated: false,
                                });
                            }
                        }
                    }
                    surface::Surface::Prompt => surface::prompt_messages_body(&result),
                };
                let payload = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
                let truncated = surface::surface_truncated(
                    profile.spec.surface,
                    payload.len(),
                    profile.spec.limits.max_response_bytes,
                );
                Ok(BackendResponse { payload, truncated })
            }
            Err(OpError::Tool(msg)) => Ok(BackendResponse {
                payload: verbatim_error(&msg),
                truncated: false,
            }),
            Err(OpError::Backend(e)) => Err(e),
        }
    }

    fn input_schema(&self, profile_name: &str) -> Option<Value> {
        self.profile(profile_name).map(|p| {
            let mut schema = op_input_schema(&p.spec);
            // Surface the argument names the binding's CEL `params` reference as
            // additional optional properties (untyped hints), so a client sees
            // the inputs the bound `:placeholder` values come from. The object
            // stays open, so this never rejects valid args.
            let names = arguments_referenced_by_params(&p.compiled_params);
            if !names.is_empty()
                && let Some(props) = schema.get_mut("properties").and_then(Value::as_object_mut)
            {
                for name in names {
                    props.entry(name).or_insert_with(|| json!({}));
                }
            }
            schema
        })
    }

    /// JSON Schema for the operation result envelope this binding emits.
    fn output_schema(&self, _profile_name: &str) -> Option<Value> {
        Some(exec::result_envelope_schema())
    }

    fn audit_metadata(&self, profile_name: &str) -> serde_json::Map<String, Value> {
        let mut m = serde_json::Map::new();
        m.insert("dynamodb.transport".into(), Value::String("plugin".into()));
        if let Some(p) = self.profile(profile_name) {
            m.insert("aws.table".into(), Value::String(p.spec.table.clone()));
            m.insert(
                "aws.operation".into(),
                Value::String(p.spec.operation.as_str().to_owned()),
            );
            m.insert("aws.region".into(), Value::String(p.spec.region.clone()));
            // Whether this binding's operation writes table state, so the
            // gateway/operator can reason about write surfaces uniformly.
            m.insert(
                "aws.mutates".into(),
                Value::Bool(p.spec.operation.mutates()),
            );
        }
        m
    }

    /// Enumerate resources for `resources/list` via the operator-fixed
    /// `list_query` Scan. The pagination cursor is the opaque encoded
    /// `last_evaluated_key`. Bindings without a `list_query` inherit the empty
    /// page.
    async fn list_resources(
        &self,
        profile_name: &str,
        cursor: Option<&str>,
    ) -> Result<ResourcePage, BackendError> {
        let profile = self
            .profile(profile_name)
            .ok_or_else(|| BackendError::ProfileNotFound {
                backend_name: profile_name.to_owned(),
            })?;
        let Some(list) = profile.spec.list_query.clone() else {
            return Ok(ResourcePage::empty());
        };
        let start_key = match cursor {
            Some(c) => Some(exec::decode_cursor(c)?),
            None => None,
        };

        let (items, lek) =
            exec::run_list_scan(&profile.client, &profile.spec, &list, start_key).await?;
        let json_items: Vec<Value> = items.iter().map(marshal::item_to_json).collect();
        let next_cursor = lek.as_ref().map(exec::encode_cursor);

        Ok(surface::items_to_resource_page(
            &json_items,
            &list.uri_attribute,
            list.name_attribute.as_deref(),
            list.description_attribute.as_deref(),
            next_cursor,
        ))
    }

    /// Return completion candidates for a resource-template variable via the
    /// operator-fixed `variable_completions[<variable_name>]` Query. The
    /// `:prefix` placeholder is bound to the caller's typed prefix as an `S`
    /// value — never interpolated. Unconfigured variables inherit the empty
    /// list.
    async fn complete_template_variable(
        &self,
        profile_name: &str,
        variable_name: &str,
        prefix: &str,
        _config: &Value,
        _context: &std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<String>, BackendError> {
        let profile = self
            .profile(profile_name)
            .ok_or_else(|| BackendError::ProfileNotFound {
                backend_name: profile_name.to_owned(),
            })?;
        let Some(cc) = profile
            .spec
            .variable_completions
            .get(variable_name)
            .cloned()
        else {
            return Ok(vec![]);
        };
        let max = cc.max_results.unwrap_or(100);
        let items =
            exec::run_completion_query(&profile.client, &profile.spec, &cc, prefix, max).await?;
        let json_items: Vec<Value> = items.iter().map(marshal::item_to_json).collect();
        Ok(surface::items_to_completion_values(
            &json_items,
            &cc.value_attribute,
            max.max(0) as usize,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plugin_with(spec: Value) -> DynamodbBackendPlugin {
        let p = DynamodbBackendPlugin::new();
        // register synchronously via a throwaway runtime (no nested
        // runtime — these unit tests run outside one).
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let host = mcpg_plugin_protocol::noop_backend_host();
            BackendPlugin::register_profile(&p, "t", &spec, host)
                .await
                .unwrap();
        });
        p
    }

    fn get_spec() -> Value {
        json!({
            "region": "us-east-1",
            "endpoint_url": "http://localhost:4566",
            "credentials": { "access_key_id": "test", "secret_access_key": "test" },
            "table": "orders",
            "operation": "get_item",
            "partition_key": { "name": "order_id", "type": "S" }
        })
    }

    #[test]
    fn kind_and_manifest() {
        let p = DynamodbBackendPlugin::new();
        assert_eq!(BackendPlugin::kind(&p), "dynamodb");
        assert_eq!(p.manifest.id, "dev.mcpg.backend.dynamodb");
    }

    #[test]
    fn register_rejects_bad_spec() {
        let p = DynamodbBackendPlugin::new();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let err = rt.block_on(async {
            let host = mcpg_plugin_protocol::noop_backend_host();
            BackendPlugin::register_profile(&p, "t", &json!({ "region": "" }), host).await
        });
        assert!(matches!(err, Err(BackendError::InvalidSpec { .. })));
    }

    #[test]
    fn input_schema_get_item_requires_key() {
        let p = plugin_with(get_spec());
        let schema = BackendPlugin::input_schema(&p, "t").unwrap();
        assert_eq!(schema["required"][0], "key");
        assert!(schema["properties"]["key"]["properties"]["order_id"].is_object());
    }

    #[test]
    fn input_schema_update_item_requires_key_and_update_expression() {
        let mut v = get_spec();
        v["operation"] = json!("update_item");
        let p = plugin_with(v);
        let schema = BackendPlugin::input_schema(&p, "t").unwrap();
        let required: Vec<&str> = schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap())
            .collect();
        assert!(required.contains(&"key"));
        assert!(required.contains(&"update_expression"));
        assert!(schema["properties"]["return_values"]["enum"].is_array());
    }

    #[test]
    fn input_schema_batch_get_requires_keys_array() {
        let mut v = get_spec();
        v["operation"] = json!("batch_get");
        let p = plugin_with(v);
        let schema = BackendPlugin::input_schema(&p, "t").unwrap();
        assert_eq!(schema["required"][0], "keys");
        assert_eq!(schema["properties"]["keys"]["type"], "array");
        assert_eq!(schema["properties"]["keys"]["maxItems"], 25);
    }

    #[test]
    fn input_schema_batch_write_requires_requests_array() {
        let mut v = get_spec();
        v["operation"] = json!("batch_write");
        let p = plugin_with(v);
        let schema = BackendPlugin::input_schema(&p, "t").unwrap();
        assert_eq!(schema["required"][0], "requests");
        assert_eq!(schema["properties"]["requests"]["maxItems"], 25);
    }

    #[test]
    fn audit_metadata_marks_mutation_for_write_ops() {
        let mut v = get_spec();
        v["operation"] = json!("update_item");
        let p = plugin_with(v);
        let m = BackendPlugin::audit_metadata(&p, "t");
        assert_eq!(m["aws.mutates"], true);

        // A read op is marked non-mutating.
        let p2 = plugin_with(get_spec());
        let m2 = BackendPlugin::audit_metadata(&p2, "t");
        assert_eq!(m2["aws.mutates"], false);
    }

    #[test]
    fn audit_metadata_carries_table_op_region() {
        let p = plugin_with(get_spec());
        let m = BackendPlugin::audit_metadata(&p, "t");
        assert_eq!(m["aws.table"], "orders");
        assert_eq!(m["aws.operation"], "get_item");
        assert_eq!(m["aws.region"], "us-east-1");
        // The transport marker is present on the audit metadata (matches the
        // sibling-backend convention + the failure-audit details).
        assert_eq!(m["dynamodb.transport"], "plugin");
    }

    #[test]
    fn input_schema_surfaces_param_argument_names() {
        let mut v = get_spec();
        v["operation"] = json!("query");
        v["params"] = json!({ ":status": "arguments.status" });
        let p = plugin_with(v);
        let schema = BackendPlugin::input_schema(&p, "t").unwrap();
        // The CEL param references `arguments.status` → surfaced as a property.
        assert!(schema["properties"]["status"].is_object());
    }

    #[test]
    fn execute_unknown_profile_is_profile_not_found() {
        let p = DynamodbBackendPlugin::new();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let err = rt.block_on(async {
            BackendPlugin::execute(
                &p,
                "missing",
                BackendRequest {
                    payload: b"{}".to_vec(),
                    headers: vec![],
                    request_id: "r".into(),
                    session_id: None,
                    identity: None,
                    idempotency: None,
                },
            )
            .await
        });
        assert!(matches!(err, Err(BackendError::ProfileNotFound { .. })));
    }

    #[test]
    fn list_resources_empty_when_unconfigured() {
        let p = plugin_with(get_spec());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let page = rt
            .block_on(async { BackendPlugin::list_resources(&p, "t", None).await })
            .expect("list");
        assert!(page.resources.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn complete_template_variable_empty_when_unconfigured() {
        let p = plugin_with(get_spec());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let got = rt
            .block_on(async {
                BackendPlugin::complete_template_variable(
                    &p,
                    "t",
                    "v",
                    "x",
                    &json!({}),
                    &std::collections::BTreeMap::new(),
                )
                .await
            })
            .expect("complete");
        assert!(got.is_empty());
    }

    #[test]
    fn list_resources_unknown_profile_is_profile_not_found() {
        let p = DynamodbBackendPlugin::new();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let err = rt.block_on(async { BackendPlugin::list_resources(&p, "missing", None).await });
        assert!(matches!(err, Err(BackendError::ProfileNotFound { .. })));
    }

    #[test]
    fn register_stores_list_query_and_completions() {
        let mut v = get_spec();
        v["surface"] = json!("resource");
        v["list_query"] = json!({ "uri_attribute": "uri", "page_size": 25 });
        v["variable_completions"] = json!({
            "order_id": {
                "key_condition_expression": "begins_with(order_id, :prefix)",
                "value_attribute": "order_id"
            }
        });
        let p = plugin_with(v);
        let prof = p.profile("t").unwrap();
        assert!(prof.spec.list_query.is_some());
        assert!(prof.spec.variable_completions.contains_key("order_id"));
    }

    #[test]
    fn execute_bad_args_json_returns_tool_error() {
        let p = plugin_with(get_spec());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let resp = rt
            .block_on(async {
                BackendPlugin::execute(
                    &p,
                    "t",
                    BackendRequest {
                        payload: b"{ not json".to_vec(),
                        headers: vec![],
                        request_id: "r".into(),
                        session_id: None,
                        identity: None,
                        idempotency: None,
                    },
                )
                .await
            })
            .unwrap();
        let v: Value = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(v[VERBATIM_RESULT_KEY]["isError"], true);
    }
}
