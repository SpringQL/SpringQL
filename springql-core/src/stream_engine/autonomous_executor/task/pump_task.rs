use super::subtask::insert_subtask::{self, InsertSubtask};
use super::subtask::query_subtask::QuerySubtask;
use super::task_state::TaskState;
use super::{task_context::TaskContext, task_id::TaskId};
use crate::error::Result;
use crate::pipeline::pump_model::PumpModel;
use crate::stream_engine::autonomous_executor::RowRepository;
use crate::stream_engine::dependency_injection::DependencyInjection;

#[derive(Debug)]
pub(crate) struct PumpTask {
    id: TaskId,
    state: TaskState,
    query_subtask: QuerySubtask,
    insert_subtask: InsertSubtask,
}

impl From<&PumpModel> for PumpTask {
    fn from(pump: &PumpModel) -> Self {
        let id = TaskId::from_pump(pump.name().clone());
        let query_subtask = QuerySubtask::from(pump.query_plan());
        let insert_subtask = InsertSubtask::from(pump.insert_plan());
        Self {
            id,
            state: TaskState::from(pump.state()),
            query_subtask,
            insert_subtask,
        }
    }
}

impl PumpTask {
    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine) fn state(&self) -> &TaskState {
        &self.state
    }

    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<()> {
        let row = self.query_subtask.run(context)?;
        self.insert_subtask.run(row, context)
    }
}
