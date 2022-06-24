// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod sink_writer;

pub use sink_writer::{NetSinkWriter, SinkWriter, SinkWriterRepository};

use std::sync::Arc;

use crate::{
    api::error::Result,
    mem_size::MemSize,
    pipeline::{SinkWriterModel, SinkWriterName, StreamName},
    stream_engine::{
        autonomous_executor::{
            performance_metrics::{
                InQueueMetricsUpdateByCollect, InQueueMetricsUpdateByTask,
                MetricsUpdateByTaskExecution, TaskMetricsUpdateByTask,
            },
            repositories::Repositories,
            row::StreamRow,
            task::task_context::TaskContext,
            task_graph::{QueueId, TaskId},
        },
        time::WallClockStopwatch,
    },
};

#[derive(Debug)]
pub struct SinkTask {
    id: TaskId,
    upstream: StreamName,
    sink_writer_name: SinkWriterName,
}

impl SinkTask {
    pub fn new(sink_writer: &SinkWriterModel) -> Self {
        let id = TaskId::from_sink(sink_writer);
        Self {
            id,
            upstream: sink_writer.sink_upstream().clone(),
            sink_writer_name: sink_writer.name().clone(),
        }
    }

    pub fn id(&self) -> &TaskId {
        &self.id
    }

    pub fn run(&self, context: &TaskContext) -> Result<MetricsUpdateByTaskExecution> {
        let stopwatch = WallClockStopwatch::start();

        let repos = context.repos();

        let opt_in_queue_id = context
            .pipeline_derivatives()
            .task_graph()
            .input_queue(&context.task(), &self.upstream);

        let in_queues_metrics = if let Some(in_queue_id) = opt_in_queue_id {
            if let Some((row, in_queue_metrics)) = self.use_row_from(in_queue_id, repos) {
                self.emit(row, context)?;
                vec![in_queue_metrics]
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let execution_time = stopwatch.stop();

        let out_queues_metrics = vec![];
        let task_metrics = TaskMetricsUpdateByTask::new(context.task(), execution_time);
        Ok(MetricsUpdateByTaskExecution::new(
            task_metrics,
            in_queues_metrics,
            out_queues_metrics,
        ))
    }

    fn use_row_from(
        &self,
        queue_id: QueueId,
        repos: Arc<Repositories>,
    ) -> Option<(StreamRow, InQueueMetricsUpdateByTask)> {
        match queue_id {
            QueueId::Row(queue_id) => {
                let row_q_repo = repos.row_queue_repository();
                let queue = row_q_repo.get(&queue_id);
                queue.use_().map(|row| {
                    let bytes_used = row.mem_size();
                    (
                        row,
                        InQueueMetricsUpdateByTask::new(
                            InQueueMetricsUpdateByCollect::Row {
                                queue_id,
                                rows_used: 1,
                                bytes_used: bytes_used as u64,
                            },
                            None,
                        ),
                    )
                })
            }
            QueueId::Window(_) => unreachable!("sink task must have row input queue"),
        }
    }

    fn emit(&self, row: StreamRow, context: &TaskContext) -> Result<()> {
        let sink_writer = context
            .repos()
            .sink_writer_repository()
            .get_sink_writer(&self.sink_writer_name);

        sink_writer
            .lock()
            .expect("other worker threads sharing the same sink subtask must not get panic")
            .send_row(row.into())?;

        Ok(())
    }
}
