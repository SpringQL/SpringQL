// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod tuple;

mod pump_task;
mod sink_task;
mod source_task;
mod task_context;
mod window;

mod processed_rows;

pub use processed_rows::ProcessedRows;
pub use sink_task::SinkWriterRepository;
pub use source_task::{
    NetClientSourceReader, NetServerSourceReader, SourceReader, SourceReaderRepository, SourceTask,
};
pub use task_context::TaskContext;
pub use tuple::Tuple;
pub use window::Window;

use crate::{
    api::error::Result,
    pipeline::{Edge, PipelineGraph},
    stream_engine::autonomous_executor::{
        performance_metrics::MetricsUpdateByTaskExecution,
        task::{pump_task::PumpTask, sink_task::SinkTask},
        task_graph::TaskId,
    },
};

#[derive(Debug)]
pub enum Task {
    Pump(Box<PumpTask>),
    Source(SourceTask),
    Sink(SinkTask),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TaskRunResult {
    pub processed_rows: ProcessedRows,
    pub metrics: MetricsUpdateByTaskExecution,
}

impl Task {
    pub fn new(edge: &Edge, pipeline_graph: &PipelineGraph) -> Self {
        match edge {
            Edge::Pump { pump_model, .. } => {
                Self::Pump(Box::new(PumpTask::new(pump_model.as_ref(), pipeline_graph)))
            }
            Edge::Source(s) => Self::Source(SourceTask::new(s)),
            Edge::Sink(s) => Self::Sink(SinkTask::new(s)),
        }
    }

    pub fn id(&self) -> TaskId {
        match self {
            Task::Pump(t) => t.id().clone(),
            Task::Source(source_task) => source_task.id().clone(),
            Task::Sink(s) => s.id().clone(),
        }
    }

    pub fn run(&self, context: &TaskContext) -> Result<TaskRunResult> {
        match self {
            Task::Pump(pump_task) => pump_task.run(context),
            Task::Source(source_task) => source_task.run(context),
            Task::Sink(sink_task) => sink_task.run(context),
        }
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for Task {}
