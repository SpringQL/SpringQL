use crate::{error::Result, stream_engine::model::stream_model::StreamModel};

use super::Row;

pub(in crate::stream_engine::executor) trait RowRepository {
    /// Get the next row from `source_stream`.
    fn collect_next(&self, source_stream: &StreamModel) -> Result<&Row>;

    /// Move `row` to `row.stream()`.
    fn emit(&self, row: Row) -> Result<()>;
}
