// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::mem_size::MemSize;
use crate::pipeline::name::StreamName;
use crate::stream_engine::autonomous_executor::performance_metrics::metrics_update_command::metrics_update_by_task_execution::InQueueMetricsUpdateByCollect;
use crate::stream_engine::autonomous_executor::repositories::Repositories;
use crate::stream_engine::autonomous_executor::task::task_context::TaskContext;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::QueueId;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::row_queue_id::RowQueueId;
use crate::stream_engine::autonomous_executor::task_graph::queue_id::window_queue_id::WindowQueueId;
use crate::stream_engine::command::query_plan::query_plan_operation::CollectOp;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct CollectSubtask {
    upstream: StreamName,
}

impl CollectSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn from_collect_op(
        collect_op: CollectOp,
    ) -> Self {
        let upstream = collect_op.stream;
        Self { upstream }
    }

    /// # Returns
    ///
    /// None when input queue does not exist or is empty.
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Option<(Tuple, InQueueMetricsUpdateByCollect)> {
        let repos = context.repos();
        let pump_task_id = context.task();

        let pipeline_derivatives = context.pipeline_derivatives();
        let task_graph = pipeline_derivatives.task_graph();

        let queue_id = task_graph.input_queue(&pump_task_id, &self.upstream);
        match queue_id {
            QueueId::Row(queue_id) => self.collect_from_row_queue(queue_id, repos),
            QueueId::Window(queue_id) => self.collect_from_window_queue(queue_id, repos),
        }
    }

    /// # Returns
    ///
    /// None when input queue does not exist or is empty.
    fn collect_from_row_queue(
        &self,
        queue_id: RowQueueId,
        repos: Arc<Repositories>,
    ) -> Option<(Tuple, InQueueMetricsUpdateByCollect)> {
        let row_q_repo = repos.row_queue_repository();
        let queue = row_q_repo.get(&queue_id);
        let opt_row = queue.use_();
        opt_row.map(|row| {
            let bytes_used = row.mem_size();
            let tuple = Tuple::from_row(row);
            (
                tuple,
                InQueueMetricsUpdateByCollect::Row {
                    queue_id,
                    rows_used: 1,
                    bytes_used: bytes_used as u64,
                },
            )
        })
    }

    /// # Returns
    ///
    /// None when input queue does not exist or is empty.
    fn collect_from_window_queue(
        &self,
        queue_id: WindowQueueId,
        repos: Arc<Repositories>,
    ) -> Option<(Tuple, InQueueMetricsUpdateByCollect)> {
        let window_q_repo = repos.window_queue_repository();
        let queue = window_q_repo.get(&queue_id);
        let opt_row = queue.dispatch();
        opt_row.map(|row| {
            let bytes_dispatched = row.mem_size();
            let tuple = Tuple::from_row(row);
            (
                tuple,
                InQueueMetricsUpdateByCollect::Window {
                    queue_id,
                    waiting_bytes_dispatched: bytes_dispatched as u64,
                    waiting_rows_dispatched: 1,
                },
            )
        })
    }
}
