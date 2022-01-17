// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(in crate::stream_engine::autonomous_executor) mod source_reader;

use std::fmt::Debug;

use crate::error::Result;
use crate::mem_size::MemSize;
use crate::pipeline::name::{SourceReaderName, StreamName};
use crate::pipeline::source_reader_model::SourceReaderModel;
use crate::stream_engine::autonomous_executor::AutonomousExecutor;
use crate::stream_engine::autonomous_executor::performance_metrics::metrics_update_command::metrics_update_by_task_execution::{MetricsUpdateByTaskExecution, OutQueueMetricsUpdateByTaskExecution, TaskMetricsUpdateByTaskExecution};
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::QueueId;
use crate::stream_engine::autonomous_executor::task_graph::task_id::TaskId;
use crate::stream_engine::time::duration::wall_clock_duration::wall_clock_stopwatch::WallClockStopwatch;

use super::task_context::TaskContext;

#[derive(Debug)]
pub(crate) struct SourceTask {
    id: TaskId,
    source_reader_name: SourceReaderName,
    source_stream_name: StreamName,
}

impl SourceTask {
    pub(in crate::stream_engine) fn new(source_reader: &SourceReaderModel) -> Self {
        let id = TaskId::from_source(source_reader);
        Self {
            id,
            source_reader_name: source_reader.name().clone(),
            source_stream_name: source_reader.dest_source_stream().clone(),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Result<MetricsUpdateByTaskExecution> {
        let stopwatch = WallClockStopwatch::start();

        let out_qid = context
            .output_queues()
            .first()
            .expect("source task must have 1 output queue")
            .clone();
        let out_queue_metrics = self.put_row_into(out_qid, context);

        let execution_time = stopwatch.stop();

        let task_metrics = TaskMetricsUpdateByTaskExecution::new(context.task(), execution_time);
        Ok(MetricsUpdateByTaskExecution::new(
            task_metrics,
            vec![],
            out_queue_metrics.map_or_else(Vec::new, |o| vec![o]),
        ))
    }

    fn put_row_into(
        &self,
        queue_id: QueueId,
        context: &TaskContext,
    ) -> Option<OutQueueMetricsUpdateByTaskExecution> {
        let row = self.collect_next(context)?;
        let repos = context.repos();

        let out_queue_metrics = match queue_id {
            QueueId::Row(queue_id) => {
                let row_q_repo = repos.row_queue_repository();
                let queue = row_q_repo.get(&queue_id);
                let bytes_put = row.mem_size();

                queue.put(row);
                OutQueueMetricsUpdateByTaskExecution::new(queue_id.into(), 1, bytes_put as u64)
            }
            QueueId::Window(_) => todo!(),
        };
        Some(out_queue_metrics)
    }

    fn collect_next(&self, context: &TaskContext) -> Option<Row> {
        let source_reader = context
            .repos()
            .source_reader_repository()
            .get_source_reader(&self.source_reader_name);

        let source_stream = context
            .pipeline_derivatives()
            .pipeline()
            .get_source_stream(&self.source_stream_name)
            .expect("cannot do anything if source stream name is wrong here");

        let mut source_reader = source_reader
            .lock()
            .expect("other worker threads sharing the same subtask must not get panic");
        source_reader
            .next_row()
            .and_then(|source_row| source_row.into_row(source_stream.shape()))
            .map_or_else(
                |e| {
                    AutonomousExecutor::handle_error(e);
                    None
                },
                Some,
            )
    }
}
