use super::task_state::TaskState;
use super::{task_context::TaskContext, task_id::TaskId};
use crate::error::Result;
use crate::stream_engine::autonomous_executor::RowRepository;
use crate::stream_engine::{
    dependency_injection::DependencyInjection, pipeline::pump_model::PumpModel,
};

#[derive(Debug)]
pub(crate) struct PumpTask {
    id: TaskId,
    state: TaskState,
}

impl From<&PumpModel> for PumpTask {
    fn from(pump: &PumpModel) -> Self {
        let id = TaskId::from_pump(pump.name().clone());
        Self {
            id,
            state: TaskState::Stopped,
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
        let row_repo = context.row_repository();

        let row = row_repo.collect_next(&context.task())?;

        // TODO modify row if necessary (run query subtask)

        row_repo.emit(row, &context.downstream_tasks())
    }
}
