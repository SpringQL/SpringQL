use std::fmt::Debug;
use std::rc::Rc;
use std::sync::Mutex;

use anyhow::Context;

use crate::error::{Result, SpringError};
use crate::stream_engine::executor::data::row::{repository::RowRepository, Row};
use crate::stream_engine::executor::server::input::InputServerActive;
use crate::stream_engine::model::stream_model::StreamModel;

#[derive(Debug, new)]
pub(in crate::stream_engine::executor::exec) struct ForeignInputPump<S>
where
    S: InputServerActive + Debug,
{
    /// 1 server can be shared to 2 or more foreign streams.
    in_server: Rc<Mutex<S>>,

    dest_stream: Rc<StreamModel>,
}

impl<S: InputServerActive + Debug> ForeignInputPump<S> {
    fn collect_next(&self) -> Result<Row> {
        let foreign_row = self
            .in_server
            .lock()
            .unwrap_or_else(|e| panic!("failed to lock input foreign server ({:?}) because another thread sharing the same server got poisoned: {:?}", self.in_server, e))
            .next_row()?;
        foreign_row.into_row(&self.dest_stream)
    }

    fn emit(&self, row: Row) -> Result<()> {
        let repo = self.row_repository();
        repo.emit(row)
    }

    fn row_repository(&self) -> &dyn RowRepository {
        todo!()
    }
}
