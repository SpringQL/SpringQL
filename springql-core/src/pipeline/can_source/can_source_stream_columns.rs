// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::{
    api::error::Result,
    pipeline::can_source::can_source_stream_model::CANSourceStreamModel,
    stream_engine::autonomous_executor::{ColumnValues, StreamColumns},
};

/// Column values in a CAN source stream.
#[derive(Clone, PartialEq, Debug)]
pub struct CANSourceStreamColumns(StreamColumns);

impl CANSourceStreamColumns {
    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - `column_values` lacks any of `stream_model.columns()`.
    ///   - Type mismatch (and failed to convert type) with `stream_model` and `column_values`.
    pub fn new(stream_model: &CANSourceStreamModel, column_values: ColumnValues) -> Result<Self> {
        let stream_model = Arc::new(stream_model.as_stream_model().clone());
        let stream_columns = StreamColumns::new(stream_model, column_values)?;
        Ok(Self(stream_columns))
    }

    pub fn into_stream_columns(self) -> StreamColumns {
        self.0
    }
}
