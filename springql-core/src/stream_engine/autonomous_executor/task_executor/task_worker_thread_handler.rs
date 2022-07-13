// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Task execution logics commonly used by GenericWorkerThread and SourceWorkerThread.

use std::{fmt::Display, sync::Arc, thread, time::Duration};

use crate::stream_engine::autonomous_executor::{
    event_queue::{Event, NonBlockingEventQueue},
    performance_metrics::{MetricsUpdateByTaskExecutionOrPurge, PerformanceMetrics},
    pipeline_derivatives::PipelineDerivatives,
    repositories::Repositories,
    task::{ProcessedRows, TaskContext},
    task_executor::{scheduler::Scheduler, task_executor_lock::TaskExecutorLock},
    task_graph::TaskId,
    worker::WorkerThreadLoopState,
    AutonomousExecutor,
};

/// Sleep duration for when no tasks are available for the (source / generic) worker.
const TASK_WAIT_MSEC: u64 = 10;

#[derive(Debug)]
pub struct TaskWorkerThreadHandler;

#[derive(Copy, Clone, Eq, PartialEq, Debug, new)]
pub struct TaskWorkerId(u16);
impl Display for TaskWorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, new)]
pub struct TaskWorkerThreadArg {
    pub worker_id: TaskWorkerId,
    task_executor_lock: Arc<TaskExecutorLock>,
    repos: Arc<Repositories>,
    sleep_msec_no_row: u64,
}

#[derive(Debug)]
pub struct TaskWorkerLoopState<S: Scheduler> {
    pub pipeline_derivatives: Option<Arc<PipelineDerivatives>>,
    pub metrics: Option<Arc<PerformanceMetrics>>,
    pub scheduler: S,
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
    pub fn main_loop_cycle<S>(
        current_state: TaskWorkerLoopState<S>,
        thread_arg: &TaskWorkerThreadArg,
        event_queue: &NonBlockingEventQueue,
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
                    let processed_rows = Self::execute_task_series::<S>(
                        &task_series,
                        pipeline_derivatives.clone(),
                        thread_arg,
                        event_queue,
                    );
                    if processed_rows.is_empty() {
                        // Wait for rows to process
                        thread::sleep(Duration::from_millis(thread_arg.sleep_msec_no_row));
                    }
                } else {
                    // Wait for tasks to execute
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
        event_queue: &NonBlockingEventQueue,
    ) -> ProcessedRows
    where
        S: Scheduler,
    {
        task_series
            .iter()
            .fold(ProcessedRows::default(), |acc_processed_rows, task_id| {
                let context = TaskContext::new(
                    task_id.clone(),
                    pipeline_derivatives.clone(),
                    thread_arg.repos.clone(),
                );

                let task = pipeline_derivatives
                    .get_task(task_id)
                    .expect("task id got from scheduler");

                let processed_rows = task
                    .run(&context)
                    .map(|run_result| {
                        event_queue.publish(Event::IncrementalUpdateMetrics {
                            metrics_update_by_task_execution_or_purge: Arc::new(
                                MetricsUpdateByTaskExecutionOrPurge::TaskExecution(
                                    run_result.metrics,
                                ),
                            ),
                        });
                        run_result.processed_rows
                    })
                    .unwrap_or_else(|e| {
                        AutonomousExecutor::handle_error(e);
                        ProcessedRows::default()
                    });

                acc_processed_rows + processed_rows
            })
    }
}
