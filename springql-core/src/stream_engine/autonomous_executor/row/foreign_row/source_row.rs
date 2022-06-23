// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod can_frame_source_row;
mod json_source_row;
mod source_row_format;

pub use can_frame_source_row::CANFrameSourceRow;
pub use json_source_row::JsonSourceRow;
pub use source_row_format::SourceRowFormat;

use std::sync::Arc;

use anyhow::Context;

use crate::{
    api::{error::Result, SpringError},
    pipeline::{CANSourceStreamModel, StreamModel},
    stream_engine::{autonomous_executor::row::schemaless_row::SchemalessRow, StreamRow},
};

/// Input row from foreign sources (retrieved from SourceReader).
///
/// Immediately converted into `Row` on stream-engine boundary.
#[derive(Clone, PartialEq, Debug)]
pub enum SourceRow {
    Json(JsonSourceRow),
    CANFrame(CANFrameSourceRow),
    Raw(SchemalessRow),
}

impl SourceRow {
    /// # Failure
    ///
    /// - `SpringError::InvalidFormat` when:
    ///   - `bytes` cannot be parsed as the `format`
    pub fn from_bytes(format: SourceRowFormat, bytes: &[u8]) -> Result<Self> {
        match format {
            SourceRowFormat::Json => {
                let json_s = std::str::from_utf8(bytes)
                    .context("failed to parse bytes into UTF-8")
                    .map_err(|e| SpringError::InvalidFormat {
                        s: "(binary data)".to_string(),
                        source: e,
                    })?;
                let json_source_row = JsonSourceRow::parse(json_s)?;
                Ok(Self::Json(json_source_row))
            }
        }
    }

    /// # Failure
    ///
    /// - `SpringError::InvalidFormat` when:
    ///   - This source row cannot be converted into row.
    pub fn into_row(self, stream_model: Arc<StreamModel>) -> Result<StreamRow> {
        match self {
            SourceRow::Json(json_source_row) => json_source_row.into_row(stream_model),
            SourceRow::CANFrame(can_frame_source_row) => {
                let stream = CANSourceStreamModel::try_from(stream_model.as_ref())?;
                can_frame_source_row.into_row(&stream)
            }
            SourceRow::Raw(schemaless_row) => {
                StreamRow::from_schemaless_row(schemaless_row, stream_model)
            }
        }
    }
}
