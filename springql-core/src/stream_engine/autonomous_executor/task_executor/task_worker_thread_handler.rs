//! Task execution logics commonly used by GenericWorkerThread and SourceWorkerThread.

use std::{fmt::Display, sync::Arc, thread, time::Duration};

use crate::stream_engine::autonomous_executor::{
    event_queue::{event::Event, EventQueue},
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecutionOrPurge,
        PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
    repositories::Repositories,
    task::task_context::TaskContext,
    task_graph::task_id::TaskId,
    worker::worker_thread::WorkerThreadLoopState,
    AutonomousExecutor,
};

use super::{scheduler::Scheduler, task_executor_lock::TaskExecutorLock};

const TASK_WAIT_MSEC: u64 = 10;

#[derive(Debug)]
pub(super) struct TaskWorkerThreadHandler;

#[derive(Copy, Clone, Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct TaskWorkerId(u16);
impl Display for TaskWorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct TaskWorkerThreadArg {
    pub(super) worker_id: TaskWorkerId,
    task_executor_lock: Arc<TaskExecutorLock>,
    repos: Arc<Repositories>,
}

#[derive(Debug)]
pub(super) struct TaskWorkerLoopState<S: Scheduler> {
    pub(super) pipeline_derivatives: Option<Arc<PipelineDerivatives>>,
    pub(super) metrics: Option<Arc<PerformanceMetrics>>,
    pub(super) scheduler: S,
}

impl<S: Scheduler> WorkerThreadLoopState for TaskWorkerLoopState<S> {
    type ThreadArg = TaskWorkerThreadArg;

    fn new(_thread_arg: &Self::ThreadArg) -> Self
    where
        Self: Sized,
    {
        Self {
            pipeline_derivatives: None,
            metrics: None,
            scheduler: S::default(),
        }
    }

    fn is_integral(&self) -> bool {
        match (&self.pipeline_derivatives, &self.metrics) {
            (Some(p), Some(m)) => p.pipeline_version() == *m.pipeline_version(),
            _ => false,
        }
    }
}

impl TaskWorkerThreadHandler {
    pub(super) fn main_loop_cycle<S>(
        current_state: TaskWorkerLoopState<S>,
        thread_arg: &TaskWorkerThreadArg,
        event_queue: &EventQueue,
    ) -> TaskWorkerLoopState<S>
    where
        S: Scheduler,
    {
        if let (Some(pipeline_derivatives), Some(metrics)) =
            (&current_state.pipeline_derivatives, &current_state.metrics)
        {
            let task_executor_lock = &thread_arg.task_executor_lock;

            if let Ok(_lock) = task_executor_lock.try_task_execution() {
                let task_series = current_state
                    .scheduler
                    .next_task_series(pipeline_derivatives.task_graph(), metrics.as_ref());
                if !task_series.is_empty() {
                    Self::execute_task_series::<S>(
                        &task_series,
                        pipeline_derivatives.clone(),
                        thread_arg,
                        event_queue,
                    );
                } else {
                    thread::sleep(Duration::from_millis(TASK_WAIT_MSEC));
                }
            }

            current_state
        } else {
            unreachable!("by integrity check")
        }
    }

    fn execute_task_series<S>(
        task_series: &[TaskId],
        pipeline_derivatives: Arc<PipelineDerivatives>,
        thread_arg: &TaskWorkerThreadArg,
        event_queue: &EventQueue,
    ) where
        S: Scheduler,
    {
        for task_id in task_series {
            let context = TaskContext::new(
                task_id.clone(),
                pipeline_derivatives.clone(),
                thread_arg.repos.clone(),
            );

            let task = pipeline_derivatives
                .get_task(task_id)
                .expect("task id got from scheduler");

            task.run(&context)
                .map(|metrics_diff| {
                    event_queue.publish(Event::IncrementalUpdateMetrics {
                        metrics_update_by_task_execution_or_purge: Arc::new(
                            MetricsUpdateByTaskExecutionOrPurge::TaskExecution(metrics_diff),
                        ),
                    })
                })
                .unwrap_or_else(AutonomousExecutor::handle_error);
        }
    }
}
