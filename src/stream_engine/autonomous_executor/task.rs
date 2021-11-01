pub(super) mod task_graph;
pub(super) mod task_id;

mod pump_task;
mod sink_task;
mod source_task;

use crate::error::Result;

use self::{pump_task::PumpTask, sink_task::SinkTask, source_task::SourceTask, task_id::TaskId};

#[derive(Debug)]
pub(in crate::stream_engine) enum Task {
    Pump(PumpTask),
    Source(SourceTask),
    Sink(SinkTask),
}

impl Task {
    pub(super) fn id(&self) -> TaskId {
        todo!()
    }

    pub(super) fn run(&self) -> Result<()> {
        todo!()
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for Task {}
