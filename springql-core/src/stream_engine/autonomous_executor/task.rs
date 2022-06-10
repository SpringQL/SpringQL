// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub mod tuple;

pub mod pump_task;
pub mod sink_task;
pub mod source_task;
pub mod task_context;
pub mod window;

use crate::{
    api::error::Result,
    pipeline::{Edge, PipelineGraph},
    stream_engine::autonomous_executor::{
        performance_metrics::metrics_update_command::MetricsUpdateByTaskExecution,
        task::{
            pump_task::PumpTask, sink_task::SinkTask, source_task::SourceTask,
            task_context::TaskContext,
        },
        task_graph::task_id::TaskId,
    },
};

#[derive(Debug)]
pub enum Task {
    Pump(Box<PumpTask>),
    Source(SourceTask),
    Sink(SinkTask),
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

    pub fn run(&self, context: &TaskContext) -> Result<MetricsUpdateByTaskExecution> {
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
