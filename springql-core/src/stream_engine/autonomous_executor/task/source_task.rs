// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod source_reader;

pub use source_reader::{
    NetClientSourceReader, NetServerSourceReader, SourceReader, SourceReaderRepository,
};

use std::fmt::Debug;
use std::sync::Arc;

use crate::{
    api::error::Result,
    mem_size::MemSize,
    pipeline::{SourceReaderModel, SourceReaderName, StreamName},
    stream_engine::{
        autonomous_executor::{
            performance_metrics::{
                MetricsUpdateByTaskExecution, OutQueueMetricsUpdateByTask, TaskMetricsUpdateByTask,
            },
            repositories::Repositories,
            row::{SchemalessRow, StreamRow},
            task::{task_context::TaskContext, ProcessedRows, TaskRunResult},
            task_graph::{QueueId, RowQueueId, TaskId, WindowQueueId},
            AutonomousExecutor,
        },
        time::WallClockStopwatch,
    },
};

#[derive(Debug)]
pub struct SourceTask {
    id: TaskId,
    source_reader_name: SourceReaderName,
    source_stream_name: StreamName,
}

impl SourceTask {
    pub fn new(source_reader: &SourceReaderModel) -> Self {
        let id = TaskId::from_source(source_reader);
        Self {
            id,
            source_reader_name: source_reader.name().clone(),
            source_stream_name: source_reader.dest_source_stream().clone(),
        }
    }

    pub fn id(&self) -> &TaskId {
        &self.id
    }

    pub fn run(&self, context: &TaskContext) -> Result<TaskRunResult> {
        let stopwatch = WallClockStopwatch::start();

        let (processed_rows, out_queue_metrics_seq) = match self.collect_next(context) {
            Some(row) => {
                let out_queue_metrics_seq = context
                    .output_queues()
                    .into_iter()
                    .map(|out_qid| self.put_row_into(out_qid, row.clone(), context)) // remove None metrics
                    .collect::<Vec<OutQueueMetricsUpdateByTask>>();
                (ProcessedRows::new(1), out_queue_metrics_seq)
            }
            None => (ProcessedRows::default(), vec![]),
        };

        let execution_time = stopwatch.stop();

        let task_metrics = TaskMetricsUpdateByTask::new(context.task(), execution_time);
        let metrics =
            MetricsUpdateByTaskExecution::new(task_metrics, vec![], out_queue_metrics_seq);
        Ok(TaskRunResult {
            processed_rows,
            metrics,
        })
    }

    fn put_row_into(
        &self,
        queue_id: QueueId,
        row: StreamRow,
        context: &TaskContext,
    ) -> OutQueueMetricsUpdateByTask {
        let repos = context.repos();

        match queue_id {
            QueueId::Row(queue_id) => self.put_row_into_row_queue(row, queue_id, repos),
            QueueId::Window(queue_id) => self.put_row_into_window_queue(row, queue_id, repos),
        }
    }
    fn put_row_into_row_queue(
        &self,
        row: StreamRow,
        queue_id: RowQueueId,
        repos: Arc<Repositories>,
    ) -> OutQueueMetricsUpdateByTask {
        let row_q_repo = repos.row_queue_repository();
        let queue = row_q_repo.get(&queue_id);
        let bytes_put = row.mem_size();

        queue.put(row);
        OutQueueMetricsUpdateByTask::new(queue_id.into(), 1, bytes_put as u64)
    }
    fn put_row_into_window_queue(
        &self,
        row: StreamRow,
        queue_id: WindowQueueId,
        repos: Arc<Repositories>,
    ) -> OutQueueMetricsUpdateByTask {
        let window_q_repo = repos.window_queue_repository();
        let queue = window_q_repo.get(&queue_id);
        let bytes_put = row.mem_size();

        queue.put(row);
        OutQueueMetricsUpdateByTask::new(queue_id.into(), 1, bytes_put as u64)
    }

    fn collect_next(&self, context: &TaskContext) -> Option<StreamRow> {
        let source_reader = context
            .repos()
            .source_reader_repository()
            .get_source_reader(&self.source_reader_name);

        let source_stream = context
            .pipeline_derivatives()
            .pipeline()
            .get_stream(&self.source_stream_name)
            .expect("cannot do anything if source stream name is wrong here");

        let mut source_reader = source_reader
            .lock()
            .expect("other worker threads sharing the same subtask must not get panic");
        source_reader
            .next_row()
            .and_then(|source_row| {
                let schemaless_row = SchemalessRow::try_from(source_row)?;
                StreamRow::from_schemaless_row(schemaless_row, source_stream)
            })
            .map_or_else(
                |e| {
                    AutonomousExecutor::handle_error(e);
                    None
                },
                Some,
            )
    }
}
