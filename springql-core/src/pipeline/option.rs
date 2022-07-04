// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod can_options;
mod in_memory_queue_options;
mod net_protocol;
mod net_server_options;
mod options_builder;
mod source_net_client_options;
mod sink_net_client_options;

pub use can_options::CANOptions;
pub use in_memory_queue_options::InMemoryQueueOptions;
pub use net_protocol::NetProtocol;
pub use net_server_options::NetServerOptions;
pub use options_builder::OptionsBuilder;
pub use source_net_client_options::SourceNetClientOptions;

use std::collections::HashMap;

use anyhow::Context;

use crate::api::error::{Result, SpringError};

/// Options in CREATE statement.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Options(HashMap<String, String>);

impl Options {
    /// # Failure
    ///
    /// - `SpringError::InvalidOption` when:
    ///   - key is not found in this Options.
    pub fn get<V, F>(&self, key: &str, value_parser: F) -> Result<V>
    where
        F: FnOnce(&String) -> std::result::Result<V, anyhow::Error>,
    {
        self.0
            .get(key)
            .context("key is not found in options")
            .and_then(value_parser)
            .map_err(|e| SpringError::InvalidOption {
                key: key.to_string(),
                value: "(not found)".to_string(),
                source: e,
            })
    }
}

impl From<OptionsBuilder> for Options {
    fn from(options_builder: OptionsBuilder) -> Self {
        options_builder.build()
    }
}
