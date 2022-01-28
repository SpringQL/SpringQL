// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::mem_size::MemSize;
use crate::pipeline::name::ColumnName;
use crate::pipeline::pipeline_graph::PipelineGraph;
use crate::pipeline::stream_model::StreamModel;
use crate::stream_engine::autonomous_executor::performance_metrics::metrics_update_command::metrics_update_by_task_execution::OutQueueMetricsUpdateByTaskExecution;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::QueueId;
use crate::stream_engine::command::insert_plan::InsertPlan;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct InsertSubtask {
    into_stream: Arc<StreamModel>,

    /// INSERT INTO stream (c2, c3, c1) -- this one!
    column_order: Vec<ColumnName>,
}

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct InsertSubtaskOut {
    pub(in crate::stream_engine::autonomous_executor) out_queues_metrics_update:
        Vec<OutQueueMetricsUpdateByTaskExecution>,
}

impl InsertSubtask {
    /// # Panics
    ///
    /// `plan` has invalid stream name
    pub(in crate::stream_engine::autonomous_executor) fn new(
        plan: &InsertPlan,
        pipeline_graph: &PipelineGraph,
    ) -> Self {
        let into_stream = pipeline_graph
            .get_stream(plan.stream())
            .expect("plan has invalid stream name");
        Self {
            into_stream,
            column_order: plan.column_order().to_vec(),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        tuples: Vec<Tuple>,
        context: &TaskContext,
    ) -> InsertSubtaskOut {
        let repos = context.repos();
        let row_q_repo = repos.row_queue_repository();
        let output_queues = context.output_queues();

        let rows = tuples
            .into_iter()
            .map(|tuple| tuple.into_row(self.into_stream.clone(), self.column_order.clone()))
            .collect::<Vec<_>>();

        // TODO modify row on INSERT if necessary
        let out_queues_metrics_update = output_queues
            .into_iter()
            .map(|q| match q {
                QueueId::Row(queue_id) => {
                    let row_q = row_q_repo.get(&queue_id);

                    let mut bytes_put = 0;
                    for row in rows.clone() {
                        bytes_put += row.mem_size();
                        row_q.put(row);
                    }

                    OutQueueMetricsUpdateByTaskExecution::new(
                        queue_id.into(),
                        rows.len() as u64,
                        bytes_put as u64,
                    )
                }
                QueueId::Window(_) => todo!(),
            })
            .collect();

        InsertSubtaskOut::new(out_queues_metrics_update)
    }
}
