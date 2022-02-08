// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod collect_subtask;
pub(super) mod eval_value_expr_subtask;
pub(super) mod projection_subtask;

use crate::{
    error::Result,
    stream_engine::autonomous_executor::{
        performance_metrics::metrics_update_command::metrics_update_by_task_execution::InQueueMetricsUpdateByTaskExecution,
        task::{task_context::TaskContext, tuple::Tuple},
    },
    stream_engine::command::query_plan::QueryPlan,
};

use self::{
    collect_subtask::CollectSubtask, eval_value_expr_subtask::EvalValueExprSubtask,
    projection_subtask::ProjectionSubtask,
};

/// Process input row 1-by-1.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct QuerySubtask {
    projection_subtask: ProjectionSubtask,
    collect_subtask: CollectSubtask,

    eval_value_expr_subtask: EvalValueExprSubtask, // TODO rm
}

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct QuerySubtaskOut {
    pub(in crate::stream_engine::autonomous_executor) tuples: Vec<Tuple>,
    pub(in crate::stream_engine::autonomous_executor) in_queue_metrics_update:
        InQueueMetricsUpdateByTaskExecution,
}

impl QuerySubtask {
    pub(in crate::stream_engine::autonomous_executor) fn new(plan: QueryPlan) -> Self {
        let projection_subtask =
            ProjectionSubtask::new(plan.upper_ops.projection.column_references);
        let collect_subtask = CollectSubtask::new();
        let eval_value_expr_subtask =
            EvalValueExprSubtask::new(plan.upper_ops.eval_value_expr.expressions);

        Self {
            projection_subtask,
            collect_subtask,
            eval_value_expr_subtask,
        }
    }

    /// # Returns
    ///
    /// None when input queue does not exist or is empty.
    ///
    /// # Failures
    ///
    /// TODO
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Result<Option<QuerySubtaskOut>> {
        match self.run_lower_ops(context) {
            None => Ok(None),
            Some(lower_query_subtask_out) => {
                let values_seq = self.run_upper_ops(lower_query_subtask_out.tuples)?;

                Ok(Some(QuerySubtaskOut::new(
                    values_seq,
                    lower_query_subtask_out.in_queue_metrics_update, // leaf subtask decides in queue metrics change
                )))
            }
        }
    }

    fn run_upper_ops(&self, lower_tuples: Vec<Tuple>) -> Result<Vec<Tuple>> {
        lower_tuples
            .into_iter()
            .map(|tuple| {
                let tuple = self.run_eval_value_expr_op(tuple)?;
                self.run_projection_op(tuple)
            })
            .collect()
    }

    /// # Returns
    ///
    /// None when input queue does not exist or is empty or JOIN op does not emit output yet.
    fn run_lower_ops(&self, context: &TaskContext) -> Option<QuerySubtaskOut> {
        self.run_collect_op(context)
    }

    fn run_eval_value_expr_op(&self, tuple: Tuple) -> Result<Tuple> {
        self.eval_value_expr_subtask.run(tuple)
    }

    fn run_projection_op(&self, tuple: Tuple) -> Result<Tuple> {
        self.projection_subtask.run(tuple)
    }

    fn run_collect_op(&self, context: &TaskContext) -> Option<QuerySubtaskOut> {
        self.collect_subtask.run(context)
    }
}
