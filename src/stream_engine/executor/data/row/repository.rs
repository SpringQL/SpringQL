use crate::{error::Result, model::name::StreamName};

use super::Row;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) struct RowRef;

impl RowRef {
    pub(in crate::stream_engine::executor) fn get(&self) -> &Row {
        todo!()
    }
}

pub(crate) trait RowRepository {
    /// Get ref to Row from RowRef.
    fn get(&self, row_ref: &RowRef) -> &Row;

    /// Get the next RowRef from `source_stream`.
    fn collect_next(&self, source_stream: &StreamName) -> Result<RowRef>;

    /// Move `row` to `dest_stream`.
    fn emit(&self, row: Row, dest_stream: &StreamName) -> Result<()>;
}

#[derive(Debug, Default)]
pub(crate) struct RefCntGcRowRepository;

impl RowRepository for RefCntGcRowRepository {
    fn get(&self, row_ref: &RowRef) -> &Row {
        todo!()
    }

    fn collect_next(&self, source_stream: &StreamName) -> Result<RowRef> {
        todo!()
    }

    fn emit(&self, row: Row, dest_stream: &StreamName) -> Result<()> {
        todo!()
    }
}
