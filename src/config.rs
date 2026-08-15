//! Per-binding spec for `dev.mcpg.backend.dynamodb`.
//!
//! One binding == one DynamoDB operation on one operator-fixed table
//! (the `soap`/`ldap` envelope model). The gateway's
//! `DynamodbBackendConfig` serialises to this spec for `register_profile`.
//! No `deny_unknown_fields` on the spec struct — the gateway injects
//! `__mcpg_secret_refs` / `__mcpg_id_sig` attributes before registration
//! (the typed gateway-side config carries `deny_unknown_fields` instead).

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct DynamodbSpec {
    /// AWS region (required by the SDK even against LocalStack).
    pub region: String,
    /// Endpoint override for LocalStack / dynamodb-local / a VPC endpoint.
    #[serde(default)]
    pub endpoint_url: Option<String>,
    /// Static credentials (local/testing). Absent => default chain
    /// (IRSA / instance role / env / profile).
    #[serde(default)]
    pub credentials: Option<StaticCredentials>,
    /// The operator-fixed table this binding operates on.
    pub table: String,
    /// The single operation this binding exposes as a tool.
    pub operation: Operation,
    pub partition_key: KeyAttr,
    #[serde(default)]
    pub sort_key: Option<KeyAttr>,
    #[serde(default)]
    pub limits: Limits,
    /// MCP surface this binding serves. `tool` (default) emits the unchanged op
    /// result body; `resource` reshapes the successful op result into the
    /// `resources/read` `{contents:[…]}` body; `prompt` reshapes it into the
    /// `prompts/get` `{messages:[…]}` body. Set to match the capability list the
    /// binding is placed under (`resources[]` / `prompts[]`).
    #[serde(default)]
    pub surface: crate::surface::Surface,
    /// Optional static resource URI for `surface: resource`. When set it is used
    /// verbatim as the emitted content `uri`; when omitted the binding uses the
    /// requested URI the gateway passes in the call arguments (`uri`). Ignored
    /// for `tool` / `prompt` surfaces.
    #[serde(default)]
    pub uri: Option<String>,

    /// Optional listing config for `resources/list`. On a `surface: resource`
    /// binding this runs an operator-fixed Scan against `table` to enumerate
    /// concrete resource URIs. All inputs are operator-fixed (filter /
    /// projection / expression values); the only caller-derived value is the
    /// opaque pagination cursor. Empty → the binding returns no dynamic listing
    /// (the trait default).
    #[serde(default)]
    pub list_query: Option<ListQueryConfig>,

    /// Optional per-template-variable completion config for
    /// `completion/complete`. Keyed by the URI template variable name; each
    /// entry runs an operator-fixed Query whose `:prefix` placeholder is bound
    /// to the caller-typed prefix as an `S` AttributeValue (never interpolated
    /// — injection-safe). Empty → no completion candidates (the trait default).
    #[serde(default)]
    pub variable_completions: std::collections::BTreeMap<String, CompletionConfig>,

    /// Optional CEL-computed `ExpressionAttributeValues`, keyed by the `:name`
    /// placeholder the operator-fixed expression references. Each value is a CEL
    /// expression over the call `arguments`; per call it is evaluated and the
    /// resulting scalar (`S` / `N` / `BOOL` / `NULL`) is bound server-side as the
    /// placeholder's AttributeValue — never string-interpolated into the
    /// expression (injection-safe). Keys must begin with `:`. Caller-supplied
    /// `expression_attribute_values` for the same placeholder are overridden by
    /// the binding's compiled `params`. Empty → no CEL params (the default).
    #[serde(default)]
    pub params: std::collections::HashMap<String, String>,
}

/// Operator-fixed Scan that enumerates resources for `resources/list`.
#[derive(Debug, Clone, Deserialize)]
pub struct ListQueryConfig {
    /// Item attribute (declared per-row by the scan) holding the resource URI.
    /// Required — rows without a string value here are skipped.
    pub uri_attribute: String,
    /// Optional item attribute holding the resource display name.
    #[serde(default)]
    pub name_attribute: Option<String>,
    /// Optional item attribute holding the resource description.
    #[serde(default)]
    pub description_attribute: Option<String>,
    /// Operator-fixed DynamoDB filter expression (e.g. `attribute_exists(uri)`).
    /// Caller input never reaches this; any placeholders resolve from
    /// `expression_attribute_values` below.
    #[serde(default)]
    pub filter_expression: Option<String>,
    /// Operator-fixed projection expression.
    #[serde(default)]
    pub projection_expression: Option<String>,
    /// Operator-fixed expression-attribute-value map (DynamoDB-JSON).
    #[serde(default)]
    pub expression_attribute_values: Option<serde_json::Value>,
    /// Operator-fixed expression-attribute-name map.
    #[serde(default)]
    pub expression_attribute_names: Option<std::collections::HashMap<String, String>>,
    /// Rows per page (1..=1000). Defaults to 100; clamped to `max_page_size`.
    #[serde(default = "d_list_page")]
    pub page_size: i32,
}

/// Operator-fixed Query producing completion candidates for one template
/// variable. The `:prefix` placeholder is bound to the caller prefix.
#[derive(Debug, Clone, Deserialize)]
pub struct CompletionConfig {
    /// DynamoDB key-condition expression. MUST reference `:prefix` (bound to
    /// the caller's typed prefix as an `S` value), e.g.
    /// `pk = :p AND begins_with(sk, :prefix)`. Operator-fixed otherwise.
    pub key_condition_expression: String,
    /// Item attribute whose string value is returned as a candidate. Required.
    pub value_attribute: String,
    /// Operator-fixed extra expression-attribute-value map (DynamoDB-JSON),
    /// merged with the bound `:prefix`. Use for fixed partition-key values.
    #[serde(default)]
    pub expression_attribute_values: Option<serde_json::Value>,
    /// Operator-fixed expression-attribute-name map.
    #[serde(default)]
    pub expression_attribute_names: Option<std::collections::HashMap<String, String>>,
    /// Optional cap on returned candidates; defaults to 100; clamped to
    /// `max_page_size`.
    #[serde(default)]
    pub max_results: Option<i32>,
}

fn d_list_page() -> i32 {
    100
}

#[derive(Clone, Deserialize)]
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
}

// Manual redacting Debug so a `{:?}` of a spec never prints secrets.
impl std::fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticCredentials")
            .field("access_key_id", &"***")
            .field("secret_access_key", &"***")
            .field("session_token", &self.session_token.as_ref().map(|_| "***"))
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    GetItem,
    PutItem,
    DeleteItem,
    UpdateItem,
    Query,
    Scan,
    BatchGet,
    BatchWrite,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::GetItem => "get_item",
            Operation::PutItem => "put_item",
            Operation::DeleteItem => "delete_item",
            Operation::UpdateItem => "update_item",
            Operation::Query => "query",
            Operation::Scan => "scan",
            Operation::BatchGet => "batch_get",
            Operation::BatchWrite => "batch_write",
        }
    }

    /// Whether the operation mutates table state. Read-only ops
    /// (`get_item` / `query` / `scan` / `batch_get`) are side-effect-free;
    /// `put_item` / `delete_item` / `update_item` / `batch_write` mutate.
    /// Surfaced on the binding's audit metadata (`aws.mutates`) so the
    /// gateway/operator can reason about write surfaces uniformly.
    pub fn mutates(self) -> bool {
        match self {
            Operation::GetItem | Operation::Query | Operation::Scan | Operation::BatchGet => false,
            Operation::PutItem
            | Operation::DeleteItem
            | Operation::UpdateItem
            | Operation::BatchWrite => true,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct KeyAttr {
    pub name: String,
    #[serde(rename = "type")]
    pub key_type: KeyType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum KeyType {
    S,
    N,
    B,
}

impl KeyType {
    /// The DynamoDB-JSON type tag a value for this key must carry.
    pub fn av_tag(self) -> &'static str {
        match self {
            KeyType::S => "S",
            KeyType::N => "N",
            KeyType::B => "B",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Limits {
    #[serde(default = "d_page")]
    pub max_page_size: i32,
    #[serde(default = "d_bytes")]
    pub max_response_bytes: usize,
    #[serde(default = "d_ms")]
    pub timeout_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_page_size: d_page(),
            max_response_bytes: d_bytes(),
            timeout_ms: d_ms(),
        }
    }
}

fn d_page() -> i32 {
    100
}
fn d_bytes() -> usize {
    1_048_576
}
fn d_ms() -> u64 {
    8000
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SpecError {
    #[error("dynamodb spec JSON: {0}")]
    Json(String),
    #[error("dynamodb: region is empty")]
    EmptyRegion,
    #[error("dynamodb: table name `{0}` is invalid (allowed: A-Za-z0-9_.- , 3..=255 chars)")]
    InvalidTable(String),
    #[error("dynamodb: key attribute name `{0}` is invalid")]
    InvalidKeyName(String),
    #[error("dynamodb: endpoint_url must be https:// (or http://localhost for tests)")]
    InvalidEndpoint,
    #[error("dynamodb: credentials.access_key_id is empty")]
    EmptyAccessKey,
    #[error("dynamodb: credentials.secret_access_key is empty")]
    EmptySecretKey,
    #[error("dynamodb: max_page_size must be 1..=1000")]
    InvalidPageSize,
    #[error("dynamodb: max_response_bytes must be > 0")]
    InvalidResponseBytes,
    #[error("dynamodb: timeout_ms must be > 0")]
    InvalidTimeout,
    #[error("dynamodb: `uri` is only valid with `surface: resource`")]
    UriRequiresResourceSurface,
    #[error("dynamodb: `uri` must not be empty")]
    EmptyUri,
    #[error("dynamodb: list_query.uri_attribute must not be empty")]
    EmptyListUriAttribute,
    #[error("dynamodb: list_query.page_size must be 1..=1000")]
    InvalidListPageSize,
    #[error("dynamodb: list_query.expression_attribute_values is not valid DynamoDB-JSON: {0}")]
    InvalidListExprValues(String),
    #[error("dynamodb: variable_completions.{0}.key_condition_expression must reference `:prefix`")]
    CompletionMissingPrefix(String),
    #[error("dynamodb: variable_completions.{0}.value_attribute must not be empty")]
    EmptyCompletionValueAttribute(String),
    #[error(
        "dynamodb: variable_completions.{0}.expression_attribute_values is not valid DynamoDB-JSON: {1}"
    )]
    InvalidCompletionExprValues(String, String),
    #[error(
        "dynamodb: params key `{0}` must begin with `:` (DynamoDB expression-attribute-value placeholder)"
    )]
    InvalidParamPlaceholder(String),
}

/// A DynamoDB table name: `[A-Za-z0-9_.-]`, 3..=255 chars (AWS rule).
pub fn is_valid_table_name(name: &str) -> bool {
    (3..=255).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

/// An attribute name used as a key — reject empties / control chars.
/// (DynamoDB allows up to 64KB names, but a key attr is operator-declared
/// and should be a clean identifier.)
fn is_valid_attr_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 255 && !name.chars().any(|c| c.is_control())
}

fn is_allowed_endpoint(url: &str) -> bool {
    if let Some(rest) = url.strip_prefix("https://") {
        return !rest.is_empty();
    }
    if let Some(rest) = url.strip_prefix("http://") {
        let host = rest.split(['/', ':']).next().unwrap_or("");
        return matches!(host, "localhost" | "127.0.0.1" | "[::1]");
    }
    false
}

impl DynamodbSpec {
    pub fn parse(spec: &serde_json::Value) -> Result<Self, SpecError> {
        let parsed: Self =
            serde_json::from_value(spec.clone()).map_err(|e| SpecError::Json(e.to_string()))?;
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> Result<(), SpecError> {
        if self.region.trim().is_empty() {
            return Err(SpecError::EmptyRegion);
        }
        if !is_valid_table_name(&self.table) {
            return Err(SpecError::InvalidTable(self.table.clone()));
        }
        if !is_valid_attr_name(&self.partition_key.name) {
            return Err(SpecError::InvalidKeyName(self.partition_key.name.clone()));
        }
        if let Some(sk) = &self.sort_key
            && !is_valid_attr_name(&sk.name)
        {
            return Err(SpecError::InvalidKeyName(sk.name.clone()));
        }
        if let Some(ep) = &self.endpoint_url
            && !is_allowed_endpoint(ep)
        {
            return Err(SpecError::InvalidEndpoint);
        }
        if let Some(c) = &self.credentials {
            if c.access_key_id.trim().is_empty() {
                return Err(SpecError::EmptyAccessKey);
            }
            if c.secret_access_key.is_empty() {
                return Err(SpecError::EmptySecretKey);
            }
        }
        if !(1..=1000).contains(&self.limits.max_page_size) {
            return Err(SpecError::InvalidPageSize);
        }
        if self.limits.max_response_bytes == 0 {
            return Err(SpecError::InvalidResponseBytes);
        }
        if self.limits.timeout_ms == 0 {
            return Err(SpecError::InvalidTimeout);
        }
        // Surface coherence: `uri` is only meaningful on the resource surface; a
        // static `uri` on a tool/prompt binding is a config mistake worth a
        // fail-closed rejection rather than a silent no-op.
        if self.uri.is_some() && self.surface != crate::surface::Surface::Resource {
            return Err(SpecError::UriRequiresResourceSurface);
        }
        if let Some(u) = &self.uri
            && u.trim().is_empty()
        {
            return Err(SpecError::EmptyUri);
        }
        // Operator-fixed listing: non-empty uri attr, bounded page size, and a
        // well-formed (marshallable) expression-value map — fail-closed so a
        // misconfigured listing never reaches a `resources/list` call.
        if let Some(lq) = &self.list_query {
            if lq.uri_attribute.trim().is_empty() {
                return Err(SpecError::EmptyListUriAttribute);
            }
            if !(1..=1000).contains(&lq.page_size) {
                return Err(SpecError::InvalidListPageSize);
            }
            if let Some(v) = &lq.expression_attribute_values {
                crate::marshal::json_to_item(v)
                    .map_err(|e| SpecError::InvalidListExprValues(e.to_string()))?;
            }
        }
        // CEL params: placeholder keys must use the DynamoDB `:name` sigil so
        // they actually resolve in an expression (the CEL bodies are compiled
        // separately at register).
        for key in self.params.keys() {
            if !key.starts_with(':') {
                return Err(SpecError::InvalidParamPlaceholder(key.clone()));
            }
        }
        // Operator-fixed completion: key condition must bind `:prefix`, the
        // returned attribute is named, and any fixed expr-values marshal.
        for (name, cc) in &self.variable_completions {
            if !cc.key_condition_expression.contains(":prefix") {
                return Err(SpecError::CompletionMissingPrefix(name.clone()));
            }
            if cc.value_attribute.trim().is_empty() {
                return Err(SpecError::EmptyCompletionValueAttribute(name.clone()));
            }
            if let Some(v) = &cc.expression_attribute_values {
                crate::marshal::json_to_item(v).map_err(|e| {
                    SpecError::InvalidCompletionExprValues(name.clone(), e.to_string())
                })?;
            }
        }
        Ok(())
    }

    /// The declared key attribute names (partition + optional sort).
    pub fn key_names(&self) -> Vec<&str> {
        self.key_attrs().iter().map(|k| k.name.as_str()).collect()
    }

    /// The declared key attributes (partition + optional sort).
    pub fn key_attrs(&self) -> Vec<&KeyAttr> {
        let mut v = vec![&self.partition_key];
        if let Some(sk) = &self.sort_key {
            v.push(sk);
        }
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal() -> serde_json::Value {
        json!({
            "region": "us-east-1",
            "table": "orders",
            "operation": "get_item",
            "partition_key": { "name": "order_id", "type": "S" }
        })
    }

    #[test]
    fn parses_minimal() {
        let s = DynamodbSpec::parse(&minimal()).unwrap();
        assert_eq!(s.table, "orders");
        assert_eq!(s.operation, Operation::GetItem);
        assert_eq!(s.limits.max_page_size, 100);
        assert_eq!(s.key_names(), vec!["order_id"]);
    }

    #[test]
    fn parses_with_sort_key_and_creds() {
        let mut v = minimal();
        v["sort_key"] = json!({ "name": "created_at", "type": "N" });
        v["credentials"] = json!({ "access_key_id": "a", "secret_access_key": "b" });
        v["endpoint_url"] = json!("http://localhost:4566");
        let s = DynamodbSpec::parse(&v).unwrap();
        assert_eq!(s.key_names(), vec!["order_id", "created_at"]);
        assert!(s.credentials.is_some());
    }

    #[test]
    fn rejects_empty_region() {
        let mut v = minimal();
        v["region"] = json!("");
        assert_eq!(DynamodbSpec::parse(&v).unwrap_err(), SpecError::EmptyRegion);
    }

    #[test]
    fn surface_defaults_to_tool() {
        let s = DynamodbSpec::parse(&minimal()).unwrap();
        assert_eq!(s.surface, crate::surface::Surface::Tool);
        assert!(s.uri.is_none());
    }

    #[test]
    fn parses_resource_surface_with_uri() {
        let mut v = minimal();
        v["surface"] = json!("resource");
        v["uri"] = json!("dynamodb://orders/all");
        let s = DynamodbSpec::parse(&v).unwrap();
        assert_eq!(s.surface, crate::surface::Surface::Resource);
        assert_eq!(s.uri.as_deref(), Some("dynamodb://orders/all"));
    }

    #[test]
    fn rejects_uri_on_tool_surface() {
        let mut v = minimal();
        v["uri"] = json!("dynamodb://x");
        assert_eq!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::UriRequiresResourceSurface
        );
    }

    #[test]
    fn rejects_bad_table() {
        let mut v = minimal();
        v["table"] = json!("bad/table");
        assert!(matches!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::InvalidTable(_)
        ));
        v["table"] = json!("ab"); // too short
        assert!(matches!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::InvalidTable(_)
        ));
    }

    #[test]
    fn rejects_plain_http_nonlocal_endpoint() {
        let mut v = minimal();
        v["endpoint_url"] = json!("http://dynamodb.evil.com");
        assert_eq!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::InvalidEndpoint
        );
    }

    #[test]
    fn rejects_empty_static_creds() {
        let mut v = minimal();
        v["credentials"] = json!({ "access_key_id": "", "secret_access_key": "b" });
        assert_eq!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::EmptyAccessKey
        );
    }

    #[test]
    fn rejects_out_of_range_page_size() {
        let mut v = minimal();
        v["limits"] = json!({ "max_page_size": 0 });
        assert_eq!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::InvalidPageSize
        );
    }

    #[test]
    fn parses_list_query_and_completions() {
        let mut v = minimal();
        v["surface"] = json!("resource");
        v["list_query"] = json!({
            "uri_attribute": "uri",
            "name_attribute": "title",
            "filter_expression": "attribute_exists(uri)",
            "page_size": 50,
        });
        v["variable_completions"] = json!({
            "order_id": {
                "key_condition_expression": "pk = :p AND begins_with(sk, :prefix)",
                "value_attribute": "order_id",
                "expression_attribute_values": { ":p": { "S": "ORDER" } },
            }
        });
        let s = DynamodbSpec::parse(&v).unwrap();
        let lq = s.list_query.expect("list_query");
        assert_eq!(lq.uri_attribute, "uri");
        assert_eq!(lq.name_attribute.as_deref(), Some("title"));
        assert_eq!(lq.page_size, 50);
        assert!(s.variable_completions.contains_key("order_id"));
    }

    #[test]
    fn rejects_list_query_empty_uri_attribute() {
        let mut v = minimal();
        v["list_query"] = json!({ "uri_attribute": "  " });
        assert_eq!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::EmptyListUriAttribute
        );
    }

    #[test]
    fn rejects_list_query_bad_page_size() {
        let mut v = minimal();
        v["list_query"] = json!({ "uri_attribute": "uri", "page_size": 0 });
        assert_eq!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::InvalidListPageSize
        );
    }

    #[test]
    fn rejects_completion_without_prefix() {
        let mut v = minimal();
        v["variable_completions"] = json!({
            "x": { "key_condition_expression": "pk = :p", "value_attribute": "sk" }
        });
        assert!(matches!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::CompletionMissingPrefix(_)
        ));
    }

    #[test]
    fn rejects_completion_empty_value_attribute() {
        let mut v = minimal();
        v["variable_completions"] = json!({
            "x": { "key_condition_expression": "begins_with(sk, :prefix)", "value_attribute": "" }
        });
        assert!(matches!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::EmptyCompletionValueAttribute(_)
        ));
    }

    #[test]
    fn parses_cel_params() {
        let mut v = minimal();
        v["operation"] = json!("query");
        v["params"] = json!({ ":status": "arguments.status", ":id": "arguments.id" });
        let s = DynamodbSpec::parse(&v).unwrap();
        assert_eq!(s.params.len(), 2);
        assert_eq!(s.params[":status"], "arguments.status");
    }

    #[test]
    fn rejects_param_placeholder_without_colon() {
        let mut v = minimal();
        v["params"] = json!({ "status": "arguments.status" });
        assert!(matches!(
            DynamodbSpec::parse(&v).unwrap_err(),
            SpecError::InvalidParamPlaceholder(_)
        ));
    }

    #[test]
    fn parses_write_and_batch_operations() {
        for (op, mutates) in [
            ("update_item", true),
            ("batch_get", false),
            ("batch_write", true),
        ] {
            let mut v = minimal();
            v["operation"] = json!(op);
            let s = DynamodbSpec::parse(&v).unwrap();
            assert_eq!(s.operation.as_str(), op);
            assert_eq!(s.operation.mutates(), mutates, "op {op}");
        }
    }

    #[test]
    fn mutation_classification_matches_put_delete() {
        assert!(Operation::PutItem.mutates());
        assert!(Operation::DeleteItem.mutates());
        assert!(Operation::UpdateItem.mutates());
        assert!(Operation::BatchWrite.mutates());
        assert!(!Operation::GetItem.mutates());
        assert!(!Operation::Query.mutates());
        assert!(!Operation::Scan.mutates());
        assert!(!Operation::BatchGet.mutates());
    }

    #[test]
    fn table_name_validation() {
        assert!(is_valid_table_name("orders"));
        assert!(is_valid_table_name("my-table.v2_prod"));
        assert!(!is_valid_table_name("ab"));
        assert!(!is_valid_table_name("a b"));
        assert!(!is_valid_table_name("tbl/../other"));
    }
}
