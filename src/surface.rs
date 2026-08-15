//! MCP surface shaping for resource / prompt bindings.
//!
//! A binding is a tool by default; the operator may instead place it under
//! `mcp.capabilities.resources[]` / `resource_templates[]` / `prompts[]`. The
//! gateway routes those reads to the same `execute()` path but applies a strict
//! decoder over the response body — `{contents:[…]}` for `resources/read` and
//! `{messages:[…]}` for `prompts/get`. The tool surface keeps the raw op result.
//!
//! DynamoDB's primary result body is a single op-specific JSON value (item / page
//! / write ack), so the surface helpers wrap that whole value into one content
//! entry. On the resource surface the requested URI arrives in the call
//! arguments as a top-level `uri` field (the gateway materializes it from the
//! resource read request); an operator may also pin a static `uri` on the
//! binding. The prompt surface carries no URI.

use mcpg_plugin_protocol::{ListedResource, ResourcePage};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Read a DynamoDB-JSON string attribute (`{"<attr>": {"S": "..."}}`) off an
/// item, returning the inner string. `None` when absent or not an `S` value.
fn item_string_attr(item: &Value, attr: &str) -> Option<String> {
    item.get(attr)
        .and_then(|av| av.get("S"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

/// Map operator-fixed Scan items (DynamoDB-JSON) into a [`ResourcePage`].
///
/// Each item's `uri_attribute` string is the resource URI (required); optional
/// `name` / `description` attributes fill the display fields. Items without a
/// string URI attribute are skipped. `next_cursor` is the opaque encoded
/// `last_evaluated_key`, or `None` when the scan is exhausted.
pub fn items_to_resource_page(
    items: &[Value],
    uri_attribute: &str,
    name_attribute: Option<&str>,
    description_attribute: Option<&str>,
    next_cursor: Option<String>,
) -> ResourcePage {
    let mut resources: Vec<ListedResource> = Vec::with_capacity(items.len());
    for item in items {
        let Some(uri) = item_string_attr(item, uri_attribute) else {
            continue;
        };
        resources.push(ListedResource {
            uri,
            name: name_attribute.and_then(|a| item_string_attr(item, a)),
            description: description_attribute.and_then(|a| item_string_attr(item, a)),
            mime_type: None,
        });
    }
    ResourcePage {
        resources,
        next_cursor,
    }
}

/// Extract completion candidates from Query items: each item's
/// `value_attribute` string, capped at `max`.
pub fn items_to_completion_values(
    items: &[Value],
    value_attribute: &str,
    max: usize,
) -> Vec<String> {
    items
        .iter()
        .take(max)
        .filter_map(|item| item_string_attr(item, value_attribute))
        .collect()
}

/// Which MCP surface a binding serves. `Tool` (default) keeps the historical
/// tool result body byte-for-byte; `Resource` / `Prompt` reshape the successful
/// op result into the surface-correct body the gateway decoder requires.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Surface {
    /// Tool surface — unchanged op result body.
    #[default]
    Tool,
    /// `resources/read` surface — `{contents:[{uri,text,mimeType}]}`.
    Resource,
    /// `prompts/get` surface — `{messages:[{role,content}]}`.
    Prompt,
}

impl Surface {
    /// Stable label for diagnostics.
    #[allow(dead_code)]
    pub fn as_str(self) -> &'static str {
        match self {
            Surface::Tool => "tool",
            Surface::Resource => "resource",
            Surface::Prompt => "prompt",
        }
    }
}

/// Whether a serialized response body should carry the transport
/// `truncated` flag. Only the tool surface may: its body is opaque text the
/// gateway is free to suffix with `[response truncated]`. The resource /
/// prompt bodies are complete JSON documents the gateway decodes strictly,
/// so a truncation suffix would corrupt the decode — they are never marked
/// truncated regardless of size.
pub fn surface_truncated(surface: Surface, payload_len: usize, cap: usize) -> bool {
    matches!(surface, Surface::Tool) && payload_len > cap
}

/// Resolve the resource URI for a `resources/read`: a static binding `uri`
/// wins, otherwise the gateway-supplied `uri` argument. Returns `None` when
/// neither is available so the caller can surface a clean error envelope
/// instead of emitting a decoder-invalid `{contents}` body.
pub fn resolve_resource_uri<'a>(
    static_uri: Option<&'a str>,
    arguments: &'a Value,
) -> Option<&'a str> {
    if let Some(u) = static_uri
        && !u.trim().is_empty()
    {
        return Some(u);
    }
    arguments
        .get("uri")
        .and_then(Value::as_str)
        .filter(|u| !u.trim().is_empty())
}

/// Wrap the op result body into the `resources/read` contract body —
/// `{contents:[{uri, text, mimeType:"application/json"}]}` — a single content
/// entry whose `text` is the JSON-serialized op result.
pub fn resource_contents_body(uri: &str, body: &Value) -> Value {
    let text = serde_json::to_string(body).unwrap_or_else(|_| "null".to_owned());
    json!({
        "contents": [
            {
                "uri": uri,
                "text": text,
                "mimeType": "application/json",
            }
        ]
    })
}

/// Wrap the op result body into the `prompts/get` contract body —
/// `{messages:[{role:"user", content:{type:"text", text:<body-as-json>}}]}`.
pub fn prompt_messages_body(body: &Value) -> Value {
    let text = serde_json::to_string(body).unwrap_or_else(|_| "null".to_owned());
    json!({
        "messages": [
            {
                "role": "user",
                "content": { "type": "text", "text": text }
            }
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surface_default_is_tool() {
        assert_eq!(Surface::default(), Surface::Tool);
    }

    #[test]
    fn surface_parses_snake_case() {
        let s: Surface = serde_json::from_value(json!("resource")).unwrap();
        assert_eq!(s, Surface::Resource);
        let s: Surface = serde_json::from_value(json!("prompt")).unwrap();
        assert_eq!(s, Surface::Prompt);
    }

    #[test]
    fn static_uri_wins_over_argument() {
        let args = json!({ "uri": "dynamodb://from-arg" });
        assert_eq!(
            resolve_resource_uri(Some("dynamodb://static"), &args),
            Some("dynamodb://static")
        );
    }

    #[test]
    fn falls_back_to_argument_uri() {
        let args = json!({ "uri": "dynamodb://orders/42" });
        assert_eq!(
            resolve_resource_uri(None, &args),
            Some("dynamodb://orders/42")
        );
    }

    #[test]
    fn no_uri_available_returns_none() {
        assert_eq!(resolve_resource_uri(None, &json!({})), None);
        assert_eq!(resolve_resource_uri(Some("  "), &json!({})), None);
    }

    #[test]
    fn resource_body_satisfies_decoder_shape() {
        let result = json!({ "item": { "order_id": { "S": "42" } } });
        let body = resource_contents_body("dynamodb://orders/42", &result);
        let contents = body["contents"].as_array().expect("contents array");
        assert_eq!(contents.len(), 1);
        assert_eq!(contents[0]["uri"], json!("dynamodb://orders/42"));
        assert!(contents[0]["text"].is_string());
        assert!(contents[0].get("blob").is_none());
        assert_eq!(contents[0]["mimeType"], json!("application/json"));
        let decoded: Value = serde_json::from_str(contents[0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(decoded, result);
    }

    #[test]
    fn prompt_body_satisfies_decoder_shape() {
        let result = json!({ "items": [] });
        let body = prompt_messages_body(&result);
        let messages = body["messages"].as_array().expect("messages array");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["role"], json!("user"));
        assert_eq!(messages[0]["content"]["type"], json!("text"));
        assert!(messages[0]["content"]["text"].is_string());
    }

    #[test]
    fn items_map_to_resource_page() {
        let items = vec![
            json!({ "uri": { "S": "dynamodb://orders/1" }, "title": { "S": "Order 1" } }),
            json!({ "uri": { "S": "dynamodb://orders/2" } }),
            json!({ "title": { "S": "no uri" } }),
            json!({ "uri": { "N": "5" } }),
        ];
        let page = items_to_resource_page(&items, "uri", Some("title"), None, Some("cur".into()));
        assert_eq!(page.resources.len(), 2);
        assert_eq!(page.resources[0].uri, "dynamodb://orders/1");
        assert_eq!(page.resources[0].name.as_deref(), Some("Order 1"));
        assert!(page.resources[1].name.is_none());
        assert_eq!(page.next_cursor.as_deref(), Some("cur"));
    }

    #[test]
    fn items_map_to_completion_values() {
        let items = vec![
            json!({ "order_id": { "S": "ord-1" } }),
            json!({ "order_id": { "S": "ord-2" } }),
            json!({ "order_id": { "N": "3" } }),
        ];
        let got = items_to_completion_values(&items, "order_id", 10);
        assert_eq!(got, vec!["ord-1".to_owned(), "ord-2".to_owned()]);
        assert_eq!(
            items_to_completion_values(&items, "order_id", 1),
            vec!["ord-1".to_owned()]
        );
    }

    #[test]
    fn resource_and_prompt_surfaces_never_truncate() {
        // A body far over the cap must NOT be marked truncated on the
        // resource/prompt surfaces — the gateway decodes those bodies
        // strictly and a `[response truncated]` suffix would corrupt them.
        let big = 10_000usize;
        let cap = 16usize;
        assert!(!surface_truncated(Surface::Resource, big, cap));
        assert!(!surface_truncated(Surface::Prompt, big, cap));
        // The tool surface still flags byte-cap truncation.
        assert!(surface_truncated(Surface::Tool, big, cap));
        assert!(!surface_truncated(Surface::Tool, cap, cap));
    }
}
