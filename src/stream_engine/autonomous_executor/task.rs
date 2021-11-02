pub(super) mod task_context;
pub(super) mod task_graph;
pub(super) mod task_id;

mod pump_task;
mod sink_task;
mod source_task;

use crate::{error::Result, stream_engine::dependency_injection::DependencyInjection};

use self::{
    pump_task::PumpTask, sink_task::SinkTask, source_task::SourceTask, task_context::TaskContext,
    task_id::TaskId,
};

#[derive(Debug)]
pub(crate) enum Task {
    Pump(PumpTask),
    Source(SourceTask),
    Sink(SinkTask),
}

impl Task {
    pub(super) fn id(&self) -> &TaskId {
        match self {
            Task::Pump(t) => t.id(),
            Task::Source(s) => s.id(),
            Task::Sink(s) => s.id(),
        }
    }

    pub(super) fn run<DI: DependencyInjection>(&self, context: &TaskContext<DI>) -> Result<()> {
        match self {
            Task::Pump(_) => todo!(),
            Task::Source(source_task) => source_task.run(context),
            Task::Sink(_) => todo!(),
        }
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for Task {}
