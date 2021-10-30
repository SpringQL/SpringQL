mod flow_efficient_scheduler;

pub(crate) use flow_efficient_scheduler::FlowEfficientScheduler;

use super::{pipeline_read::PipelineRead, task::Task};

pub(in crate::stream_engine) trait Scheduler {
    fn new(pipeline: PipelineRead) -> Self;

    fn next_task(&mut self) -> Option<Task>;
}
