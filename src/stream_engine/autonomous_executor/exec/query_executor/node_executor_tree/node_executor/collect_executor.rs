use std::rc::Rc;

use crate::dependency_injection::DependencyInjection;
use crate::error::Result;
use crate::model::name::PumpName;
use crate::stream_engine::autonomous_executor::data::row::Row;
use crate::stream_engine::RowRepository;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) struct CollectExecutor<DI>
where
    DI: DependencyInjection,
{
    di: Rc<DI>,

    pump: PumpName,
}

impl<DI> CollectExecutor<DI>
where
    DI: DependencyInjection,
{
    pub(in crate::stream_engine::autonomous_executor::exec::query_executor) fn run(
        &self,
    ) -> Result<Rc<Row>> {
        let row_repo = self.di.row_repository();
        row_repo.collect_next(&self.pump)
    }
}
