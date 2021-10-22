pub(in crate::stream_engine) mod options_builder;

pub(in crate::stream_engine) mod foreign_stream;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use self::options_builder::OptionsBuilder;

/// Options in CREATE statement.
#[derive(Debug, Serialize, Deserialize)]
pub(in crate::stream_engine) struct Options(HashMap<String, String>);

impl From<OptionsBuilder> for Options {
    fn from(options_builder: OptionsBuilder) -> Self {
        options_builder.build()
    }
}
