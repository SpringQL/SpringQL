// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::stream_engine::autonomous_executor::row::Row;
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
    ) -> Option<Row> {
        context
            .input_queue()
            .map(|queue_id| {
                let repos = context.repos();

                let opt_row = match queue_id {
                    QueueId::Row(qid) => {
                        let row_q_repo = repos.row_queue_repository();
                        let queue = row_q_repo.get(&qid);
                        queue.use_()
                    }
                    QueueId::Window(_) => todo!(),
                };
                opt_row
            })
            .flatten()
    }
}
