use crate::error::Result;
use crate::model::name::PumpName;
use crate::{model::name::StreamName, stream_engine::executor::data::row::Row};

use super::{RowRef, RowRepository};

///
#[derive(Debug, Default)]
pub(crate) struct NaiveRowRepository;

impl RowRepository for NaiveRowRepository {
    fn get(&self, row_ref: &RowRef) -> &Row {
        todo!()
    }

    fn collect_next(&self, pump: &PumpName) -> Result<RowRef> {
        todo!()
    }

    fn emit(&self, row: Row, dest_stream: &StreamName) -> Result<()> {
        todo!()
    }
}
