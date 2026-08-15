//! `watch_strategy` entity (`dynamodb_poll`) — the POLLING change-watch path.
//!
//! DynamoDB has no SQL tracking query and (here) no Streams dependency — real
//! CDC via `aws-sdk-dynamodbstreams` is a deferred follow-up. Instead this
//! strategy polls a monotonic cursor attribute on a table or GSI: query ordered
//! by the cursor attribute descending, `Limit=1`, and take the top item's cursor
//! value as the high-water mark. A change is signalled whenever that value
//! advances. The poll thread, the cursor diff, the stop signal and the opaque
//! handle round-trip all live in the shared [`mcpg_plugin_sdk::watch`] helper —
//! this entity only supplies the per-tick `poll` closure over its own client.
//!
//! The helper's loop is synchronous and the aws-sdk is async, so a single
//! current-thread tokio runtime is built once in [`watch`] and moved into the
//! closure; each tick `block_on`s one query (sequential ticks, so a single
//! thread is enough). Client-build / query failures map to the closure's
//! `Err(String)` — the helper logs and retries on the next tick.

use std::collections::HashMap;
use std::time::Duration;

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use mcpg_plugin_protocol::backend::WatchError;
use mcpg_plugin_protocol::{PluginManifest, firstparty_manifest};
use mcpg_plugin_sdk::HostHandle;
use mcpg_plugin_sdk::ffi::{SyncWatchStrategyPlugin, WatchHandleBox};
use mcpg_plugin_sdk::watch::{cancel_polling_watch, spawn_polling_watch};
use serde::Deserialize;
use serde_json::Value;

use crate::config::StaticCredentials;

pub const PLUGIN_ID: &str = "dev.mcpg.backend.dynamodb";

/// The strategy discriminator this entity handles.
pub const WATCH_KIND: &str = "dynamodb_poll";

/// Default poll cadence when `interval_ms` is omitted (1 minute).
fn default_interval_ms() -> u64 {
    60_000
}

/// Default per-tick query budget when `timeout_ms` is omitted (10 seconds).
fn default_timeout_ms() -> u64 {
    10_000
}

/// Per-watch spec: the connection fields needed to build a client (reusing the
/// backend's connection shape) plus the table/index, the monotonic cursor
/// attribute, and the poll cadence. The connection is carried per-watch (not at
/// plugin level), so a watcher is self-contained.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WatchSpec {
    /// AWS region (required by the SDK even against LocalStack).
    region: String,
    /// Endpoint override for LocalStack / dynamodb-local / a VPC endpoint.
    #[serde(default)]
    endpoint_url: Option<String>,
    /// Static credentials (local/testing). Absent => default chain.
    #[serde(default)]
    credentials: Option<StaticCredentials>,
    /// The operator-fixed table to poll. REQUIRED.
    table: String,
    /// Optional GSI to query instead of the base table. The cursor attribute
    /// must be the index's sort key.
    #[serde(default)]
    index: Option<String>,
    /// The monotonic attribute whose top value is the cursor (e.g. a ULID or
    /// timestamp sort key). Used both as the `key_condition` sort key and as the
    /// extracted high-water value. REQUIRED.
    cursor_attr: String,
    /// Optional operator-fixed key-condition expression scoping the query to a
    /// single partition (e.g. `pk = :p`). Paired with `expression_attribute_*`.
    /// When omitted a `scan` is used instead of a `query` (full-table high-water).
    #[serde(default)]
    key_condition: Option<String>,
    /// Operator-fixed expression-attribute-value map (DynamoDB-JSON) for the
    /// `key_condition`.
    #[serde(default)]
    expression_attribute_values: Option<Value>,
    /// Operator-fixed expression-attribute-name map for the `key_condition`.
    #[serde(default)]
    expression_attribute_names: Option<HashMap<String, String>>,
    /// Poll cadence in milliseconds (default 60000; floored by the SDK helper).
    #[serde(default = "default_interval_ms")]
    interval_ms: u64,
    /// Per-tick wall-clock query budget in milliseconds (default 10000).
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

/// `watch_strategy` entity. Stateless beyond its manifest — every watcher's
/// connection + table arrive on the per-watch spec.
pub struct DynamoDbWatchCdylib {
    manifest: PluginManifest,
}

impl DynamoDbWatchCdylib {
    /// Infallible cdylib factory. `config_json` + host are ignored — the watch
    /// carries no plugin-level config (the connection + table arrive via the
    /// per-watch spec).
    pub fn from_host_config(_config_json: &str, _host: HostHandle) -> Self {
        Self {
            manifest: firstparty_manifest! {
                id: "dev.mcpg.backend.dynamodb",
                name: "DynamoDB Poll Watch Strategy",
                class: WatchStrategy,
            },
        }
    }
}

/// Build the aws-sdk client for a watch spec. Mirrors the backend's
/// `exec::build_client` connection shape (region / endpoint / static-or-default
/// creds) but is self-contained so the watch entity carries no backend spec.
async fn build_watch_client(spec: &WatchSpec) -> Client {
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
            "mcpg-dynamodb-watch-static",
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

/// Extract the cursor scalar from the top item: the `cursor_attr` value
/// stringified (S verbatim, N as its string form, BOOL/NULL stringified).
/// `None` when the item lacks the attribute (no signal this tick).
fn cursor_from_item(item: &HashMap<String, AttributeValue>, cursor_attr: &str) -> Option<String> {
    match item.get(cursor_attr)? {
        AttributeValue::S(s) => Some(s.clone()),
        AttributeValue::N(n) => Some(n.clone()),
        AttributeValue::Bool(b) => Some(b.to_string()),
        AttributeValue::Null(_) => None,
        // Binary / sets / nested shapes aren't sensible monotonic cursors.
        _ => None,
    }
}

impl SyncWatchStrategyPlugin for DynamoDbWatchCdylib {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    fn kind(&self) -> &str {
        WATCH_KIND
    }

    fn watch(
        &self,
        resource_uri: &str,
        spec: &Value,
        emit_event: Box<dyn Fn(&str) + Send + Sync + 'static>,
    ) -> Result<WatchHandleBox, WatchError> {
        let parsed: WatchSpec =
            serde_json::from_value(spec.clone()).map_err(|e| WatchError::InvalidSpec {
                message: format!("invalid dynamodb_poll watch spec: {e}"),
            })?;

        let invalid = |m: String| WatchError::InvalidSpec { message: m };
        if parsed.table.trim().is_empty() {
            return Err(invalid("table must not be empty".into()));
        }
        if parsed.cursor_attr.trim().is_empty() {
            return Err(invalid("cursor_attr must not be empty".into()));
        }

        // Pre-marshal the operator-fixed expression values once (a malformed
        // map is a spec error, not a per-tick failure).
        let expr_values = match &parsed.expression_attribute_values {
            Some(v) => Some(
                crate::marshal::json_to_item(v)
                    .map_err(|e| invalid(format!("expression_attribute_values: {e}")))?,
            ),
            None => None,
        };

        // One current-thread runtime, moved into the closure: ticks are
        // sequential, so a single thread is enough to `block_on` each query.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| WatchError::Subscribe {
                message: format!("dynamodb_poll: tokio runtime init failed: {e}"),
            })?;

        // Build the client up front (the default-chain load is async, so it runs
        // on the same runtime). A build failure is a subscribe error.
        let client = rt.block_on(build_watch_client(&parsed));

        let table = parsed.table.clone();
        let index = parsed.index.clone();
        let cursor_attr = parsed.cursor_attr.clone();
        let key_condition = parsed.key_condition.clone();
        let expr_names = parsed.expression_attribute_names.clone();
        let timeout = Duration::from_millis(parsed.timeout_ms);

        let poll = move || -> Result<Option<String>, String> {
            let fut = async {
                // Query (scoped by key_condition) or scan (full-table), ordered by
                // the cursor sort key descending, taking only the top item.
                let item = if let Some(kc) = &key_condition {
                    let out = client
                        .query()
                        .table_name(&table)
                        .set_index_name(index.clone())
                        .key_condition_expression(kc)
                        .set_expression_attribute_values(expr_values.clone())
                        .set_expression_attribute_names(expr_names.clone())
                        .scan_index_forward(false)
                        .limit(1)
                        .send()
                        .await
                        .map_err(|e| format!("dynamodb query failed: {e}"))?;
                    out.items().first().cloned()
                } else {
                    let out = client
                        .scan()
                        .table_name(&table)
                        .set_index_name(index.clone())
                        .set_expression_attribute_names(expr_names.clone())
                        .limit(1)
                        .send()
                        .await
                        .map_err(|e| format!("dynamodb scan failed: {e}"))?;
                    out.items().first().cloned()
                };
                Ok::<_, String>(item)
            };
            let item = match rt.block_on(tokio::time::timeout(timeout, fut)) {
                Ok(inner) => inner?,
                Err(_) => return Err("dynamodb_poll: tick timed out".to_owned()),
            };
            Ok(item.and_then(|i| cursor_from_item(&i, &cursor_attr)))
        };

        Ok(spawn_polling_watch(
            resource_uri,
            Duration::from_millis(parsed.interval_ms),
            emit_event,
            poll,
        ))
    }

    fn cancel(&self, watch_handle: WatchHandleBox) {
        cancel_polling_watch(watch_handle);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn stub_host() -> HostHandle {
        // SAFETY: `stub_host_ref` returns a process-static no-op host ref; the
        // factory ignores the host entirely.
        #[allow(unsafe_code)]
        unsafe {
            HostHandle::from_ffi(mcpg_plugin_sdk::testing::stub_host_ref())
        }
    }

    fn plugin() -> DynamoDbWatchCdylib {
        DynamoDbWatchCdylib::from_host_config("", stub_host())
    }

    fn emit_noop() -> Box<dyn Fn(&str) + Send + Sync + 'static> {
        Box::new(|_| {})
    }

    #[test]
    fn manifest_and_kind_are_correct() {
        use mcpg_plugin_protocol::PluginClass;
        let p = plugin();
        let m = SyncWatchStrategyPlugin::manifest(&p);
        assert_eq!(m.id, PLUGIN_ID);
        assert_eq!(m.plugin_class, PluginClass::WatchStrategy);
        assert_eq!(p.kind(), WATCH_KIND);
    }

    #[test]
    fn spec_parses_with_defaults() {
        let parsed: WatchSpec = serde_json::from_value(json!({
            "region": "us-east-1",
            "table": "events",
            "cursor_attr": "seq",
        }))
        .unwrap();
        assert_eq!(parsed.interval_ms, 60_000);
        assert_eq!(parsed.timeout_ms, 10_000);
        assert!(parsed.index.is_none());
        assert!(parsed.key_condition.is_none());
        assert!(parsed.credentials.is_none());
    }

    #[test]
    fn spec_parses_overrides() {
        let parsed: WatchSpec = serde_json::from_value(json!({
            "region": "us-west-2",
            "endpoint_url": "http://localhost:4566",
            "credentials": { "access_key_id": "a", "secret_access_key": "b" },
            "table": "events",
            "index": "by_seq",
            "cursor_attr": "seq",
            "key_condition": "pk = :p",
            "expression_attribute_values": { ":p": { "S": "TENANT#1" } },
            "interval_ms": 30_000,
            "timeout_ms": 5_000,
        }))
        .unwrap();
        assert_eq!(parsed.index.as_deref(), Some("by_seq"));
        assert_eq!(parsed.key_condition.as_deref(), Some("pk = :p"));
        assert_eq!(parsed.interval_ms, 30_000);
        assert_eq!(parsed.timeout_ms, 5_000);
        assert!(parsed.credentials.is_some());
    }

    #[test]
    fn unknown_field_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "ddb://events",
                &json!({
                    "region": "us-east-1",
                    "table": "events",
                    "cursor_attr": "seq",
                    "bogus": true,
                }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn empty_table_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "ddb://events",
                &json!({ "region": "us-east-1", "table": "  ", "cursor_attr": "seq" }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn empty_cursor_attr_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "ddb://events",
                &json!({ "region": "us-east-1", "table": "events", "cursor_attr": "" }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn bad_expression_values_is_invalid_spec() {
        let p = plugin();
        assert!(matches!(
            p.watch(
                "ddb://events",
                &json!({
                    "region": "us-east-1",
                    "table": "events",
                    "cursor_attr": "seq",
                    "key_condition": "pk = :p",
                    "expression_attribute_values": { ":p": { "ZZ": "bad-tag" } },
                }),
                emit_noop(),
            ),
            Err(WatchError::InvalidSpec { .. })
        ));
    }

    #[test]
    fn cursor_from_item_extracts_scalar() {
        let mut item = HashMap::new();
        item.insert("seq".to_owned(), AttributeValue::S("01HXYZ".into()));
        assert_eq!(cursor_from_item(&item, "seq").as_deref(), Some("01HXYZ"));

        let mut numeric = HashMap::new();
        numeric.insert("seq".to_owned(), AttributeValue::N("42".into()));
        assert_eq!(cursor_from_item(&numeric, "seq").as_deref(), Some("42"));
    }

    #[test]
    fn cursor_from_item_none_when_missing_or_null() {
        let empty: HashMap<String, AttributeValue> = HashMap::new();
        assert_eq!(cursor_from_item(&empty, "seq"), None);

        let mut null = HashMap::new();
        null.insert("seq".to_owned(), AttributeValue::Null(true));
        assert_eq!(cursor_from_item(&null, "seq"), None);

        // A non-scalar (list) attribute is not a usable cursor.
        let mut listy = HashMap::new();
        listy.insert("seq".to_owned(), AttributeValue::L(vec![]));
        assert_eq!(cursor_from_item(&listy, "seq"), None);
    }
}
