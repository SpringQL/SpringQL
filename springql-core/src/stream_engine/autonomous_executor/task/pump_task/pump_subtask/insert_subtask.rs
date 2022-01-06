// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::QueueId;
use crate::stream_engine::command::insert_plan::InsertPlan;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct InsertSubtask {}

impl From<&InsertPlan> for InsertSubtask {
    fn from(_plan: &InsertPlan) -> Self {
        Self {
            // TODO
            // stream: plan.stream().clone(),
            // insert_columns: plan.insert_columns().clone(),
        }
    }
}

impl InsertSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        row: Row,
        context: &TaskContext,
    ) {
        let repos = context.repos();
        let row_q_repo = repos.row_queue_repository();

        let output_queues = context.output_queues();

        // TODO modify row if necessary
        output_queues.into_iter().for_each(|q| match q {
            QueueId::Row(q) => {
                let row_q = row_q_repo.get(&q);
                row_q.put(row.fixme_clone()); // Ahhhhhhhhhhhhhhhhhhhhh
            }
            QueueId::Window(_) => todo!(),
        });
    }
}
