// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::stream_engine::autonomous_executor::performance_metrics::metrics_update_command::metrics_update_by_task_execution::InQueueMetricsUpdateByTaskExecution;
use crate::stream_engine::autonomous_executor::task::pump_task::pump_subtask::query_subtask::QuerySubtaskOut;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::QueueId;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct CollectSubtask;

impl CollectSubtask {
    /// # Returns
    ///
    /// None when input queue does not exist or is empty.
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Option<QuerySubtaskOut> {
        context
            .input_queue()
            .map(|queue_id| {
                let repos = context.repos();

                let opt_out = match queue_id {
                    QueueId::Row(queue_id) => {
                        let row_q_repo = repos.row_queue_repository();
                        let queue = row_q_repo.get(&queue_id);
                        let opt_row = queue.use_();
                        opt_row.map(|row| {
                            QuerySubtaskOut::new(
                                row,
                                InQueueMetricsUpdateByTaskExecution::Row {
                                    queue_id,
                                    rows_used: 1,
                                    bytes_used: 100, // TODO calc bytes
                                },
                            )
                        })
                    }
                    QueueId::Window(_) => todo!(),
                };
                opt_out
            })
            .flatten()
    }
}
