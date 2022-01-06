// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod pump_task;
pub(super) mod sink_task;
pub(super) mod source_task;
pub(super) mod task_context;

use crate::{error::Result, pipeline::pipeline_graph::edge::Edge};

use self::{
    pump_task::PumpTask, sink_task::SinkTask, source_task::SourceTask, task_context::TaskContext,
};

use super::task_graph::task_id::TaskId;

#[derive(Debug)]
pub(crate) enum Task {
    Pump(PumpTask),
    Source(SourceTask),
    Sink(SinkTask),
}

impl Task {
    pub(super) fn id(&self) -> TaskId {
        match self {
            Task::Pump(t) => t.id().clone(),
            Task::Source(source_task) => source_task.id().clone(),
            Task::Sink(s) => s.id().clone(),
        }
    }

    pub(super) fn run(&self, context: &TaskContext) -> Result<()> {
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

impl From<&Edge> for Task {
    fn from(edge: &Edge) -> Self {
        match edge {
            Edge::Pump(p) => Self::Pump(PumpTask::from(p.as_ref())),
            Edge::Source(s) => Self::Source(SourceTask::new(s)),
            Edge::Sink(s) => Self::Sink(SinkTask::new(s)),
        }
    }
}
