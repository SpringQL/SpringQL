// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod options_builder;

pub(crate) mod can_options;
pub(crate) mod in_memory_queue_options;
pub(crate) mod net_options;

use crate::error::{Result, SpringError};
use anyhow::Context;

use std::collections::HashMap;

use self::options_builder::OptionsBuilder;

/// Options in CREATE statement.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Options(HashMap<String, String>);

impl Options {
    /// # Failure
    ///
    /// - [SpringError::InvalidOption](crate SpringError::InvalidOption) when:
    ///   - key is not found in this Options.
    pub(crate) fn get<V, F>(&self, key: &str, value_parser: F) -> Result<V>
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
