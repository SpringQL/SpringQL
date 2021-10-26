use std::rc::Rc;

use crate::error::Result;
use crate::model::name::StreamName;
use crate::stream_engine::executor::data::row::Row;

#[derive(Debug, new)]
pub(in crate::stream_engine::executor::exec::query_executor) struct CollectExecutor {
    stream: StreamName,
}

impl CollectExecutor {
    pub(in crate::stream_engine::executor::exec::query_executor) fn run(&self) -> Result<Rc<Row>> {
        todo!()
    }
}
