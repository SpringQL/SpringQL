mod flow_efficient_scheduler;

pub(crate) use flow_efficient_scheduler::FlowEfficientScheduler;

pub(crate) trait Scheduler {
    fn new(pipeline: PipelineRead) -> Self;

    fn next_task(&mut self) -> Option<Task>;
}
