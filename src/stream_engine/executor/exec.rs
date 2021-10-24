use super::data::row::{repository::RowRepository, Row};
use crate::error::Result;

pub(in crate::stream_engine::executor::exec) trait Emit {
    fn emit(&self, row: Row) -> Result<()> {
        let repo = self.row_repository();
        repo.emit(row)
    }

    fn row_repository(&self) -> &dyn RowRepository;
}
