// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(in crate::stream_engine::autonomous_executor) mod json_source_row;

use std::sync::Arc;

use crate::{api::error::Result, pipeline::stream_model::StreamModel, stream_engine::Row};

/// Input row from foreign sources (retrieved from SourceReader).
///
/// Immediately converted into `Row` on stream-engine boundary.
pub(crate) trait SourceRow {
    /// # Failure
    ///
    /// - `SpringError::InvalidFormat` when:
    ///   - This source row cannot be converted into row.
    fn into_row(self, stream_model: Arc<StreamModel>) -> Result<Row>;
}
