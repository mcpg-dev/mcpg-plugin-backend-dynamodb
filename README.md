# DynamoDB Binding (`dev.mcpg.backend.dynamodb`)

A **backend (binding)** plugin that exposes Amazon DynamoDB operations
as MCP tools. Each binding declares **one operator-fixed table + one
operation**, and that binding becomes one tool (the `soap`/`ldap`
envelope model). Uses the modern aws-lc-rs/rustls AWS HTTPS client.

## Operations

`get_item`, `put_item`, `delete_item`, `update_item`, `query`, `scan`,
`batch_get`, `batch_write`. (PartiQL and a `expand_capabilities`
table-per-tool catalog are deferred follow-ups.)

Read ops (`get_item` / `query` / `scan` / `batch_get`) are
side-effect-free; `put_item` / `delete_item` / `update_item` /
`batch_write` **mutate** — this is surfaced as `aws.mutates` (bool) on
the binding's audit metadata so the gateway/operator can reason about
write surfaces uniformly.

## Tool arguments — DynamoDB-JSON

Keys/items/expression-values use the **DynamoDB-JSON** convention at the
tool boundary (lossless, unambiguous): `{"S":"x"}`, `{"N":"5"}`,
`{"BOOL":true}`, `{"NULL":true}`, `{"M":{...}}`, `{"L":[...]}`,
`{"SS":[...]}`, `{"NS":[...]}`, `{"BS":["<base64>"]}`, `{"B":"<base64>"}`.

| Operation | Required args | Optional args | Result |
|---|---|---|---|
| `get_item` | `key` | `consistent_read`, `projection_expression` | `{ item: <DynamoDB-JSON>\|null }` |
| `put_item` | `item` | `condition_expression`, `expression_attribute_{values,names}` | `{ ok: true }` |
| `delete_item` | `key` | `condition_expression`, `expression_attribute_{values,names}` | `{ ok: true }` |
| `update_item` | `key`, `update_expression` | `condition_expression`, `expression_attribute_{values,names}`, `return_values` | `{ attributes: <DynamoDB-JSON>\|null }` |
| `query` | `key_condition_expression` | `filter_expression`, `expression_attribute_{values,names}`, `limit`, `exclusive_start_key`, `consistent_read`, `scan_index_forward`, `projection_expression` | `{ items, count, scanned_count, last_evaluated_key }` |
| `scan` | — | `filter_expression`, `expression_attribute_{values,names}`, `limit`, `exclusive_start_key`, `projection_expression` | `{ items, count, scanned_count, last_evaluated_key }` |
| `batch_get` | `keys` (≤25) | `consistent_read`, `projection_expression`, `expression_attribute_names` | `{ items, count, unprocessed_keys }` |
| `batch_write` | `requests` (≤25) | — | `{ ok: true, unprocessed_items }` |

`limit` is clamped to `limits.max_page_size`. `last_evaluated_key` is the
opaque pagination cursor — pass it back as `exclusive_start_key`.

### `update_item`

`update_expression` is an operator/caller DynamoDB UpdateExpression
(e.g. `SET #s = :status, qty = qty + :delta`); its `:placeholder` values
bind from the caller's `expression_attribute_values` and/or the binding's
CEL `params` (the binding param wins a collision). Bound values are
**never string-interpolated** into the expression — they cross as
`ExpressionAttributeValues` server-side (injection-safe), exactly like
`put_item` / `query`. `return_values` ∈ `ALL_NEW` (default) | `ALL_OLD` |
`UPDATED_NEW` | `UPDATED_OLD` | `NONE`; the returned `attributes` is the
selected projection of the item (`null` when `NONE`).

```yaml
        backend:
          kind: dynamodb
          region: us-east-1
          table: orders
          operation: update_item
          partition_key: { name: order_id, type: S }
          # `:status` binds from arguments.status via the CEL param path;
          # the update expression references it by placeholder only.
          params: { ":status": "arguments.status" }
```

A caller then invokes with
`{ "key": { "order_id": { "S": "o-1" } }, "update_expression": "SET #s = :status", "expression_attribute_names": { "#s": "status" } }`.

### `batch_get` / `batch_write`

Both honour the AWS **25-item** ceiling: a request with more than 25
`keys` / `requests` is rejected up-front with a clear tool error (never
silently truncated). Each `batch_write` `requests[]` entry is exactly one
of `{ put: <DynamoDB-JSON item> }` or `{ delete: <DynamoDB-JSON key> }`
(put items must include the declared key; delete keys are validated
against the key schema). Capacity-throttled remainders round-trip back as
`unprocessed_keys` / `unprocessed_items` in the same arg shape so a caller
can resubmit them.

```jsonc
// batch_get arguments
{ "keys": [ { "order_id": { "S": "o-1" } }, { "order_id": { "S": "o-2" } } ] }

// batch_write arguments
{ "requests": [
    { "put": { "order_id": { "S": "o-3" }, "qty": { "N": "5" } } },
    { "delete": { "order_id": { "S": "o-4" } } }
] }
```

## Binding config (`backend: { kind: dynamodb, ... }`)

| Field | Type | Default | Description |
|---|---|---|---|
| `region` | string | *(required)* | AWS region. |
| `endpoint_url` | string | *(none)* | Override for LocalStack / dynamodb-local / a VPC endpoint. `https://` (or `http://localhost`). |
| `credentials` | object | *(none → default chain)* | `{ access_key_id, secret_access_key, session_token? }`. Omit in prod to use IRSA / instance role / env / profile. |
| `table` | string | *(required)* | The operator-fixed table (validated `[A-Za-z0-9_.-]`, 3..=255). |
| `operation` | enum | *(required)* | `get_item` \| `put_item` \| `delete_item` \| `update_item` \| `query` \| `scan` \| `batch_get` \| `batch_write`. |
| `partition_key` | `{name,type}` | *(required)* | `type` ∈ `S`\|`N`\|`B`. |
| `sort_key` | `{name,type}` | *(none)* | Optional. |
| `params` | map | *(none)* | CEL-computed `ExpressionAttributeValues`, keyed by `:name` placeholder → CEL expression over `arguments`. See below. |
| `limits` | object | — | `max_page_size` (100), `max_response_bytes` (1 MiB), `timeout_ms` (8000). |

## CEL bind-params (`params`)

DynamoDB isn't SQL, but operator-fixed `key_condition_expression` /
`filter_expression` / `condition_expression` reference
`ExpressionAttributeValues` placeholders (`:name`). The `params` map binds
those placeholders from CEL expressions evaluated against the call
`arguments`:

```yaml
        backend:
          kind: dynamodb
          region: us-east-1
          table: orders
          operation: query
          partition_key: { name: customer_id, type: S }
          sort_key: { name: status, type: S }
          # Operator-fixed expression; placeholders are bound, never interpolated.
          params:
            ":cid": "arguments.customer_id"
            ":status": "arguments.status"
```

A call would invoke this with `key_condition_expression` referencing the
`:cid` / `:status` placeholders. Each CEL result is lowered to a scalar
`AttributeValue` — `S` (string), `N` (number), `BOOL`, `NULL` — and merged
into `ExpressionAttributeValues` **server-side via the aws-sdk**; the value
is never string-interpolated into the expression (injection-safe). Keys must
begin with `:`. A binding param **overrides** a caller-supplied
`expression_attribute_values` entry for the same placeholder (a caller can't
shadow an operator-fixed value). Non-scalar CEL results (arrays / objects /
bytes) are rejected as a tool error. The argument names referenced by
`params` are surfaced in `input_schema` as untyped optional properties.

## Observability

Every call emits the backend observability triad (matching the sibling
warehouse backends):

- `mcpg_dynamodb_backend_latency_seconds` — latency histogram, labelled by
  `outcome` (`ok` / `tool_error` / `timeout` / `transport_error` /
  `invalid_spec` / `profile_not_found`).
- `mcpg_dynamodb_backend_calls_total` — call counter, same `outcome` label.
- a `dev.mcpg.backend.dynamodb.*` audit event on a failure outcome
  (`request_timeout` / `request_failed` / `request_rejected`), carrying
  `dynamodb.transport: plugin` in its details. Success / not-found outcomes
  are metric-only.

## Example

```yaml
# 1. Load the backend plugin artifact (top-level `plugins:` is a flat list).
plugins:
  - id: dev.mcpg.backend.dynamodb
    class: backend
    source: { oci: "oci://ghcr.io/mcpg-dev/plugins/backend-dynamodb:protocol-1" }

# 2. Declare each binding as a tool under `mcp.capabilities.tools[]`.
#    Each entry's `backend.kind: dynamodb` routes to the plugin above.
mcp:
  capabilities:
    tools:
      - name: orders.get
        backend:
          kind: dynamodb
          region: us-east-1
          table: orders
          operation: get_item
          partition_key: { name: order_id, type: S }
      - name: orders.query
        backend:
          kind: dynamodb
          region: us-east-1
          table: orders
          operation: query
          partition_key: { name: order_id, type: S }
```

## MCP surfaces & composition

The same binding works on every MCP surface. The surface is selected by the
capability list the binding sits under plus a `surface:` knob; composition is via
`pipeline` steps and child tools.

### As a pipeline step

Inside a `kind: pipeline` binding, a DynamoDB step uses the `dynamodb` step
discriminator. The backend config fields are flattened next to `id` / `kind`;
`input_transform` shapes the step's arguments from prior steps.

```yaml
      backend:
        kind: pipeline
        pipeline_timeout_ms: 10000
        steps:
          - id: fetch
            kind: dynamodb
            region: us-east-1
            table: orders
            operation: get_item
            partition_key: { name: order_id, type: S }
            input_transform: "${arguments}"
          - id: summarize
            kind: transform
            expression: "{ 'order': steps.fetch.response }"
```

### As a resource

Place the binding under `mcp.capabilities.resources[]` with `surface: resource`.
Successful results are reshaped into the `resources/read` `{contents:[…]}` body.
Set a static `uri:` or let the binding use the requested URI from the read call.

```yaml
  capabilities:
    resources:
      - name: orders.item
        uri: "dynamodb://orders/item"
        backend:
          kind: dynamodb
          region: us-east-1
          table: orders
          operation: get_item
          partition_key: { name: order_id, type: S }
          surface: resource
          uri: "dynamodb://orders/item"
```

### As a prompt

Under `mcp.capabilities.prompts[]` with `surface: prompt`, results are reshaped
into the `prompts/get` `{messages:[…]}` body.

```yaml
  capabilities:
    prompts:
      - name: orders.context
        backend:
          kind: dynamodb
          region: us-east-1
          table: orders
          operation: get_item
          partition_key: { name: order_id, type: S }
          surface: prompt
```

### As a child tool

An LLM / generator binding can list this binding in its child-tool set, letting
the model call it during a turn. Child dispatch is governed by
`governance.child_invoke.enforce_gates` (depth cap + self-call cycle refusal
apply). Use a read operation (`get_item` / `query` / `scan` /
`batch_get`) as a child.

### Schemas & annotations

`output_schema` for the envelope wrapper is advertised in `tools/list`, and
`input_schema` is advertised too. Operators should mark read-operation bindings
explicitly so clients treat them as side-effect-free:

```yaml
        annotations: { read_only: true, open_world: false }
```

## Watch strategy (`dynamodb_poll`)

A second `watch_strategy` entity (kind `dynamodb_poll`) lets a `resource`
binding subscribe to DynamoDB changes by **polling** a monotonic cursor
attribute (a ULID / timestamp sort key) on a table or GSI. There is no
Streams dependency here — real CDC is a deferred follow-up; this reuses the
same aws-sdk client. Each tick queries (optionally scoped by
`key_condition`) ordered by `cursor_attr` descending, `Limit=1`, and emits
`notifications/resources/updated` whenever the top cursor value advances. The
first tick only establishes the baseline (no spurious startup fire).

```yaml
mcp:
  configurations:
    watches:
      - resource_uri: "dynamodb://orders/recent"
        strategy:
          kind: dynamodb_poll
          region: us-east-1
          table: orders
          index: by_updated_at        # optional GSI; cursor_attr = its sort key
          cursor_attr: updated_at      # monotonic attribute (e.g. ULID / timestamp)
          key_condition: "pk = :p"     # optional; scope to one partition
          expression_attribute_values: { ":p": { "S": "TENANT#1" } }
          interval_ms: 60000           # default 60000, floored at 250 ms
          timeout_ms: 10000            # default 10000
```

| Field | Type | Default | Description |
|---|---|---|---|
| `region` | string | *(required)* | AWS region. |
| `endpoint_url` | string | *(none)* | LocalStack / VPC endpoint override. |
| `credentials` | object | *(none → default chain)* | Static creds (as on the backend). |
| `table` | string | *(required)* | Table to poll. |
| `index` | string | *(none)* | GSI to query (its sort key must be `cursor_attr`). |
| `cursor_attr` | string | *(required)* | Monotonic attribute used as the cursor. |
| `key_condition` | string | *(none → scan)* | Scopes the query; omit for a full-table high-water scan. |
| `expression_attribute_{values,names}` | object | *(none)* | Operator-fixed binds for `key_condition`. |
| `interval_ms` | integer | 60000 | Poll cadence (floored at 250 ms). |
| `timeout_ms` | integer | 10000 | Per-tick wall-clock budget. |

Empty `table` / `cursor_attr` (or a malformed `expression_attribute_values`)
is rejected at watch start (`InvalidSpec`).

## Security

- **Table is operator-fixed** (not a caller argument) — there is no
  table-allowlist injection surface; the binding's table is baked in.
- **Key validation:** `get_item`/`delete_item`/`update_item` and every
  `batch_get` key / `batch_write` delete key must name exactly the
  declared key attributes; `put_item` and `batch_write` put items must
  include them. A caller can't address attributes outside the key schema
  (bad args → tool error, `isError: true`, never a 5xx).
- **Expression-value binding:** `update_item` (and query/scan/condition)
  `:placeholder` values are always carried as `ExpressionAttributeValues`
  via the caller args or the CEL `params` path — never string-interpolated
  into the expression (injection-safe).
- **Batch ceiling:** `batch_get` / `batch_write` reject a >25-item
  request up-front rather than truncating it.
- **Credentials** are operator-config (static / default chain). For
  per-caller scoping, layer `dev.mcpg.credential.aws-sts` in front. The
  static-credentials struct has a redacting `Debug`; secrets never reach
  logs/results.
- `network_outbound` capability; `endpoint_url` constrained to
  `https://`-or-localhost.

## Testing

Unit tests (`cargo test -p mcpg-plugin-backend-dynamodb --lib`) cover
config validation + op/mutation classification, the JSON⇄AttributeValue
marshaller (every type + rejections), key-schema guards, limit clamping,
the CEL→AttributeValue value binding (incl. the `update_item` path), the
batch >25-item rejection and the batch request builders, and the
tool-error/backend-error split — all offline. A LocalStack integration
suite drives a real put → get → query → delete round-trip:

```bash
cargo test -p mcpg-plugin-backend-dynamodb --features integration-tests --test integration
```

(needs Docker; runs in the `--config=integration` CI lane.)

## Notes

- rustls-only: the AWS SDK uses `default-https-client` (aws-lc-rs /
  rustls 0.23), **not** the legacy `rustls` feature.
- Registered in the gateway via the closed `BackendImpl` enum (`kind:
  dynamodb`) like the other envelope backends.
