use std::rc::Rc;

use crate::error::Result;
use crate::model::name::PumpName;
use crate::stream_engine::autonomous_executor::data::row::Row;
use crate::stream_engine::dependency_injection::DependencyInjection;
use crate::stream_engine::RowRepository;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) struct CollectExecutor {
    pump: PumpName,
}

impl CollectExecutor {
    pub(in crate::stream_engine::autonomous_executor::exec::query_executor) fn run<
        DI: DependencyInjection,
    >(
        &self,
        row_repo: &DI::RowRepositoryType,
    ) -> Result<Rc<Row>> {
        row_repo.collect_next(&self.pump)
    }
}
