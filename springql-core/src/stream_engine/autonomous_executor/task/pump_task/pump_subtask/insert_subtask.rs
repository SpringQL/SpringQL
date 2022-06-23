// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::{
    mem_size::MemSize,
    pipeline::{ColumnName, PipelineGraph, StreamModel},
    stream_engine::{
        autonomous_executor::{
            performance_metrics::OutQueueMetricsUpdateByTask,
            row::StreamRow,
            task::{pump_task::pump_subtask::query_subtask::SqlValues, task_context::TaskContext},
            task_graph::QueueId,
        },
        command::InsertPlan,
    },
};

#[derive(Debug)]
pub struct InsertSubtask {
    into_stream: Arc<StreamModel>,

    /// INSERT INTO stream (c2, c3, c1) -- this one!
    column_order: Vec<ColumnName>,
}

#[derive(Debug, new)]
pub struct InsertSubtaskOut {
    pub out_queues_metrics_update: Vec<OutQueueMetricsUpdateByTask>,
}

impl InsertSubtask {
    /// # Panics
    ///
    /// `plan` has invalid stream name
    pub fn new(plan: &InsertPlan, pipeline_graph: &PipelineGraph) -> Self {
        let into_stream = pipeline_graph
            .get_stream(plan.stream())
            .expect("plan has invalid stream name");
        Self {
            into_stream,
            column_order: plan.column_order().to_vec(),
        }
    }

    pub fn run(&self, values_seq: Vec<SqlValues>, context: &TaskContext) -> InsertSubtaskOut {
        if values_seq.is_empty() {
            InsertSubtaskOut::new(vec![])
        } else {
            let repos = context.repos();
            let row_q_repo = repos.row_queue_repository();
            let window_q_repo = repos.window_queue_repository();
            let output_queues = context.output_queues();

            let rows = values_seq
                .into_iter()
                .map(|values| values.into_row(self.into_stream.clone(), self.column_order.clone()))
                .collect::<Vec<_>>();

            let out_queues_metrics_update = output_queues
                .into_iter()
                .map(|q| match q {
                    QueueId::Row(queue_id) => {
                        let row_q = row_q_repo.get(&queue_id);
                        let out = self.out_queue_metrics_update(queue_id.into(), &rows);
                        for row in rows.clone() {
                            row_q.put(row);
                        }
                        out
                    }
                    QueueId::Window(queue_id) => {
                        let window_queue = window_q_repo.get(&queue_id);
                        let out = self.out_queue_metrics_update(queue_id.into(), &rows);
                        for row in rows.clone() {
                            window_queue.put(row);
                        }
                        out
                    }
                })
                .collect();

            InsertSubtaskOut::new(out_queues_metrics_update)
        }
    }

    fn out_queue_metrics_update(
        &self,
        queue_id: QueueId,
        rows: &[StreamRow],
    ) -> OutQueueMetricsUpdateByTask {
        let bytes_put: usize = rows.iter().map(|row| row.mem_size()).sum();
        OutQueueMetricsUpdateByTask::new(queue_id, rows.len() as u64, bytes_put as u64)
    }
}
