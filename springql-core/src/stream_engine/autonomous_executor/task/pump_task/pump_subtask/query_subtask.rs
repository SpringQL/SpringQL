// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod aggr_projection_subtask;
pub(super) mod collect_subtask;
pub(super) mod group_aggregate_window_subtask;
pub(super) mod value_projection_subtask;

use std::sync::Arc;

use crate::{
    error::Result,
    expr_resolver::ExprResolver,
    pipeline::{name::ColumnName, stream_model::StreamModel},
    stream_engine::autonomous_executor::{
        row::{column::stream_column::StreamColumns, column_values::ColumnValues, Row},
        task::window::GroupAggrOut,
    },
    stream_engine::command::query_plan::QueryPlan,
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::InQueueMetricsUpdateByCollect,
            task::{task_context::TaskContext, tuple::Tuple},
        },
        SqlValue,
    },
};

use self::{
    aggr_projection_subtask::AggrProjectionSubtask, collect_subtask::CollectSubtask,
    group_aggregate_window_subtask::GroupAggregateWindowSubtask,
    value_projection_subtask::ValueProjectionSubtask,
};

/// Process input row 1-by-1.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct QuerySubtask {
    expr_resolver: ExprResolver,

    value_projection_subtask: Option<ValueProjectionSubtask>,

    aggr_projection_subtask: Option<AggrProjectionSubtask>,
    group_aggr_window_subtask: Option<GroupAggregateWindowSubtask>,

    collect_subtask: CollectSubtask,
}

#[derive(Clone, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct SqlValues(Vec<SqlValue>);
impl SqlValues {
    /// ```text
    /// column_order = (c2, c3, c1)
    /// stream_shape = (c1, c2, c3)
    ///
    /// |
    /// v
    ///
    /// (fields[1], fields[2], fields[0])
    /// ```
    ///
    /// # Panics
    ///
    /// - Tuple fields and column_order have different length.
    /// - Type mismatch between `self.fields` (ordered) and `stream_shape`
    /// - Duplicate column names in `column_order`
    pub(in crate::stream_engine::autonomous_executor) fn into_row(
        self,
        stream_model: Arc<StreamModel>,
        column_order: Vec<ColumnName>,
    ) -> Row {
        assert_eq!(self.0.len(), column_order.len());

        let column_values = self.mk_column_values(column_order);
        let stream_columns = StreamColumns::new(stream_model, column_values)
            .expect("type or shape mismatch? must be checked on pump creation");
        Row::new(stream_columns)
    }

    fn mk_column_values(self, column_order: Vec<ColumnName>) -> ColumnValues {
        let mut column_values = ColumnValues::default();

        for (column_name, value) in column_order.into_iter().zip(self.0.into_iter()) {
            column_values
                .insert(column_name, value)
                .expect("duplicate column name");
        }

        column_values
    }
}

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct QuerySubtaskOut {
    pub(in crate::stream_engine::autonomous_executor) values_seq: Vec<SqlValues>,
    pub(in crate::stream_engine::autonomous_executor) in_queue_metrics_update:
        InQueueMetricsUpdateByCollect,
}

impl QuerySubtask {
    pub(in crate::stream_engine::autonomous_executor) fn new(plan: QueryPlan) -> Self {
        let collect_subtask = CollectSubtask::new();

        if plan.upper_ops.projection.aggr_expr_labels.is_empty() {
            let value_projection_subtask =
                ValueProjectionSubtask::new(plan.upper_ops.projection.value_expr_labels);

            Self {
                expr_resolver: plan.expr_resolver,
                value_projection_subtask: Some(value_projection_subtask),
                aggr_projection_subtask: None,
                group_aggr_window_subtask: None,
                collect_subtask,
            }
        } else {
            assert_eq!(
                plan.upper_ops.projection.aggr_expr_labels.len(),
                1,
                "currently only 1 aggregate in select_list is supported"
            );
            assert_eq!(
                plan.upper_ops.projection.value_expr_labels.len(),
                1,
                "currently only GROUP BY expression in select_list is supported"
            );

            let aggr_projection_subtask = AggrProjectionSubtask::new(
                plan.upper_ops.projection.value_expr_labels[0],
                plan.upper_ops.projection.aggr_expr_labels[0],
            );

            let op = plan
                .upper_ops
                .group_aggr_window
                .expect("select_list includes aggregate ");
            let group_aggr_window_subtask =
                GroupAggregateWindowSubtask::new(op.window_param, op.op_param);

            Self {
                expr_resolver: plan.expr_resolver,
                value_projection_subtask: None,
                aggr_projection_subtask: Some(aggr_projection_subtask),
                group_aggr_window_subtask: Some(group_aggr_window_subtask),
                collect_subtask,
            }
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
            Some((lower_tuples, in_queue_metrics_update)) => {
                let values_seq = self.run_upper_ops(lower_tuples)?;

                Ok(Some(QuerySubtaskOut::new(
                    values_seq,
                    in_queue_metrics_update, // leaf subtask decides in queue metrics change
                )))
            }
        }
    }

    fn run_upper_ops(&self, tuples: Vec<Tuple>) -> Result<Vec<SqlValues>> {
        Ok(tuples
            .into_iter()
            .map(|tuple| self.run_upper_ops_inner(tuple))
            .collect::<Result<Vec<Vec<_>>>>()?
            .concat())
    }
    fn run_upper_ops_inner(&self, tuple: Tuple) -> Result<Vec<SqlValues>> {
        if let Some(group_aggr_window_subtask) = &self.group_aggr_window_subtask {
            let group_aggr_out = group_aggr_window_subtask.run(&self.expr_resolver, tuple);
            self.run_aggr_projection_op(group_aggr_out)
        } else {
            let values = self.run_projection_op(&tuple)?;
            Ok(vec![values])
        }
    }

    /// # Returns
    ///
    /// None when input queue does not exist or is empty or JOIN op does not emit output yet.
    fn run_lower_ops(
        &self,
        context: &TaskContext,
    ) -> Option<(Vec<Tuple>, InQueueMetricsUpdateByCollect)> {
        self.run_collect_op(context)
    }

    fn run_projection_op(&self, tuple: &Tuple) -> Result<SqlValues> {
        self.value_projection_subtask
            .as_ref()
            .unwrap()
            .run(&self.expr_resolver, tuple)
    }

    fn run_aggr_projection_op(
        &self,
        group_aggr_out_seq: Vec<GroupAggrOut>,
    ) -> Result<Vec<SqlValues>> {
        group_aggr_out_seq
            .into_iter()
            .map(|group_agg_out| {
                self.aggr_projection_subtask
                    .as_ref()
                    .unwrap()
                    .run(group_agg_out)
            })
            .collect()
    }

    fn run_collect_op(
        &self,
        context: &TaskContext,
    ) -> Option<(Vec<Tuple>, InQueueMetricsUpdateByCollect)> {
        self.collect_subtask.run(context)
    }
}
