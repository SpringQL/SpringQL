pub(crate) mod foreign_stream;
pub(crate) mod options_builder;

use crate::error::{Result, SpringError};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use self::options_builder::OptionsBuilder;

/// Options in CREATE statement.
#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
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
                value: "".to_string(),
                source: e,
            })
    }
}

impl From<OptionsBuilder> for Options {
    fn from(options_builder: OptionsBuilder) -> Self {
        options_builder.build()
    }
}
