// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::stream_engine::autonomous_executor::performance_metrics::metrics_update_command::metrics_update_by_task_execution::OutQueueMetricsUpdateByTaskExecution;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::QueueId;
use crate::stream_engine::command::insert_plan::InsertPlan;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct InsertSubtask {}

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct InsertSubtaskOut {
    pub(in crate::stream_engine::autonomous_executor) out_queues_metrics_update:
        Vec<OutQueueMetricsUpdateByTaskExecution>,
}

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
    ) -> InsertSubtaskOut {
        let repos = context.repos();
        let row_q_repo = repos.row_queue_repository();

        let output_queues = context.output_queues();

        // TODO modify row on INSERT if necessary
        let out_queues_metrics_update = output_queues
            .into_iter()
            .map(|q| match q {
                QueueId::Row(queue_id) => {
                    let row_q = row_q_repo.get(&queue_id);
                    row_q.put(row.fixme_clone()); // Ahhhhhhhhhhhhhhhhhhhhh
                    OutQueueMetricsUpdateByTaskExecution::new(
                        queue_id.into(), 1, 100, // TODO calc row size
                    )
                }
                QueueId::Window(_) => todo!(),
            })
            .collect();

        InsertSubtaskOut::new(out_queues_metrics_update)
    }
}
