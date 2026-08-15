//! cdylib sync bridge — adapts the async [`DynamodbBackendPlugin`]
//! ([`BackendPlugin`]) onto the sync FFI [`SyncBackendPlugin`] the cdylib
//! vtable expects. Owns a private multi-thread runtime and `block_on`s
//! the async logic; the make-time [`HostHandle`] is installed on the inner
//! plugin (for the per-call observability triad) and wrapped as an
//! `Arc<dyn BackendHost>` for `register_profile`.

use std::sync::Arc;

use mcpg_plugin_protocol::{
    BackendError, BackendPlugin, BackendRequest, BackendResponse, PluginManifest, ResourcePage,
};
use mcpg_plugin_sdk::ffi::SyncBackendPlugin;
use mcpg_plugin_sdk::{HostHandle, HostHandleBackendHost};
use serde_json::Value;

use crate::DynamodbBackendPlugin;
use crate::watch::DynamoDbWatchCdylib;

pub struct DynamodbBackendCdylib {
    inner: DynamodbBackendPlugin,
    host: Arc<dyn mcpg_plugin_protocol::BackendHost>,
    rt: tokio::runtime::Runtime,
}

impl DynamodbBackendCdylib {
    pub fn from_host_config(config_json: &str, host: HostHandle) -> Self {
        let inner = DynamodbBackendPlugin::from_config_json(config_json);
        // Install the make-time host handle on the inner plugin so execute() can
        // emit the latency/counter metrics + failure audit triad.
        let _installed = inner.set_host_handle(host.clone());
        Self {
            inner,
            host: Arc::new(HostHandleBackendHost::new(host)),
            rt: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("mcpg-backend-dynamodb".to_owned())
                .enable_all()
                .build()
                .unwrap_or_else(|e| panic!("dynamodb cdylib: tokio runtime init failed: {e}")),
        }
    }
}

impl SyncBackendPlugin for DynamodbBackendCdylib {
    fn manifest(&self) -> &PluginManifest {
        BackendPlugin::manifest(&self.inner)
    }

    fn kind(&self) -> &str {
        BackendPlugin::kind(&self.inner)
    }

    fn register_profile(&self, profile_name: &str, spec: &Value) -> Result<(), BackendError> {
        self.rt.block_on(BackendPlugin::register_profile(
            &self.inner,
            profile_name,
            spec,
            Arc::clone(&self.host),
        ))
    }

    fn execute(
        &self,
        profile_name: &str,
        request: BackendRequest,
    ) -> Result<BackendResponse, BackendError> {
        self.rt
            .block_on(BackendPlugin::execute(&self.inner, profile_name, request))
    }

    fn input_schema(&self, profile_name: &str) -> Option<Value> {
        BackendPlugin::input_schema(&self.inner, profile_name)
    }

    fn output_schema(&self, profile_name: &str) -> Option<Value> {
        BackendPlugin::output_schema(&self.inner, profile_name)
    }

    fn audit_metadata(&self, profile_name: &str) -> serde_json::Map<String, Value> {
        BackendPlugin::audit_metadata(&self.inner, profile_name)
    }

    fn list_resources(
        &self,
        profile_name: &str,
        cursor: Option<&str>,
    ) -> Result<ResourcePage, BackendError> {
        self.rt.block_on(BackendPlugin::list_resources(
            &self.inner,
            profile_name,
            cursor,
        ))
    }

    fn complete_template_variable(
        &self,
        profile_name: &str,
        variable_name: &str,
        prefix: &str,
        config: &Value,
        context: &std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<String>, BackendError> {
        self.rt.block_on(BackendPlugin::complete_template_variable(
            &self.inner,
            profile_name,
            variable_name,
            prefix,
            config,
            context,
        ))
    }
}

// cdylib export — two entities under `dev.mcpg.backend.dynamodb`: the `backend`
// binding and the `watch_strategy` poller (kind `dynamodb_poll`). DynamoDB is
// network-only (HTTPS), so the single static capability is `NetworkOutbound` —
// matching the plugin.yaml `network_outbound` entry; the poll watcher uses the
// same outbound capability for its tracking query. The watch entity
// self-describes via its `manifest()` slot and is distinguished by its
// `inner_name` slug.
mcpg_plugin_sdk::declare_plugin! {
    plugin_id: "dev.mcpg.backend.dynamodb",
    plugin_version: env!("CARGO_PKG_VERSION"),
    descriptor_yaml: include_str!("../plugin.yaml"),
    capabilities: &[::mcpg_plugin_protocol::capability::Capability::NetworkOutbound],
    backend_profile: ::mcpg_plugin_protocol::manifest::BackendProfile {
        pipeline_capable: true,
        ..::core::default::Default::default()
    },
    entities: [
        backend as binding {
            inner_name: "",
            plugin_type: DynamodbBackendCdylib,
            factory: |cfg, host: ::mcpg_plugin_sdk::HostHandle|
                DynamodbBackendCdylib::from_host_config(cfg, host),
        },
        watch_strategy as watch {
            inner_name: "watch",
            plugin_type: DynamoDbWatchCdylib,
            factory: |cfg, host: ::mcpg_plugin_sdk::HostHandle|
                DynamoDbWatchCdylib::from_host_config(cfg, host),
        },
    ],
}
