// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(super) mod aggr_projection_subtask;
pub(super) mod collect_subtask;
pub(super) mod group_aggregate_window_subtask;
pub(super) mod join_subtask;
pub(super) mod value_projection_subtask;

use std::sync::{Arc, Mutex, MutexGuard};

use rand::{
    prelude::{SliceRandom, SmallRng},
    SeedableRng,
};

use crate::{
    error::Result,
    expr_resolver::ExprResolver,
    pipeline::{name::ColumnName, stream_model::StreamModel},
    stream_engine::{
        autonomous_executor::task::window::{
            aggregate::AggrWindow, join_window::JoinWindow, panes::pane::join_pane::JoinDir,
        },
        command::query_plan::{query_plan_operation::LowerOps, QueryPlan},
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::InQueueMetricsUpdateByCollect,
            task::{task_context::TaskContext, tuple::Tuple},
        },
        SqlValue,
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::{
                InQueueMetricsUpdateByTask, WindowInFlowByWindowTask,
            },
            row::{column::stream_column::StreamColumns, column_values::ColumnValues, Row},
            task::window::aggregate::GroupAggrOut,
        },
        command::query_plan::query_plan_operation::JoinOp,
    },
};

use self::{
    aggr_projection_subtask::AggrProjectionSubtask, collect_subtask::CollectSubtask,
    group_aggregate_window_subtask::GroupAggregateWindowSubtask, join_subtask::JoinSubtask,
    value_projection_subtask::ValueProjectionSubtask,
};

/// Process input row 1-by-1.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct QuerySubtask {
    expr_resolver: ExprResolver,

    value_projection_subtask: Option<ValueProjectionSubtask>,

    aggr_projection_subtask: Option<AggrProjectionSubtask>,
    group_aggr_window_subtask: Option<GroupAggregateWindowSubtask>,

    // TODO recursive JOIN
    join: Option<(
        JoinSubtask,
        CollectSubtask, // right stream
    )>,
    left_collect_subtask: CollectSubtask, // left stream

    rng: Mutex<SmallRng>,
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
        InQueueMetricsUpdateByTask,
}

impl QuerySubtask {
    pub(in crate::stream_engine::autonomous_executor) fn new(plan: QueryPlan) -> Self {
        let rng =
            Mutex::new(SmallRng::from_rng(rand::thread_rng()).expect("this generally won't fail"));

        let (left_collect_subtask, join) = Self::subtasks_from_lower_ops(plan.lower_ops);

        if plan.upper_ops.projection.aggr_expr_labels.is_empty() {
            let value_projection_subtask =
                ValueProjectionSubtask::new(plan.upper_ops.projection.value_expr_labels);

            Self {
                expr_resolver: plan.expr_resolver,
                value_projection_subtask: Some(value_projection_subtask),
                aggr_projection_subtask: None,
                group_aggr_window_subtask: None,
                left_collect_subtask,
                join,
                rng,
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
                left_collect_subtask,
                join,
                rng,
            }
        }
    }
    /// (left collect subtask, Option<(join subtask, right collect subtask)>)
    fn subtasks_from_lower_ops(
        lower_ops: LowerOps,
    ) -> (CollectSubtask, Option<(JoinSubtask, CollectSubtask)>) {
        match lower_ops.join {
            JoinOp::Collect(collect_op) => {
                let collect_subtask = CollectSubtask::from_collect_op(collect_op);
                (collect_subtask, None)
            }
            JoinOp::JoinWindow(join_window_op) => {
                let left_collect_subtask = CollectSubtask::from_collect_op(join_window_op.left);
                let right_collect_subtask = CollectSubtask::from_collect_op(join_window_op.right);
                let join_subtask =
                    JoinSubtask::new(join_window_op.window_param, join_window_op.join_param);
                (
                    left_collect_subtask,
                    Some((join_subtask, right_collect_subtask)),
                )
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
            Some((lower_tuples, in_queue_metrics_update_by_task)) => {
                let (values_seq, in_queue_metrics_update) =
                    self.run_upper_ops(lower_tuples, in_queue_metrics_update_by_task)?;

                Ok(Some(QuerySubtaskOut::new(
                    values_seq,
                    in_queue_metrics_update,
                )))
            }
        }
    }

    fn run_upper_ops(
        &self,
        tuples: Vec<Tuple>,
        in_queue_metrics_update_by_lower: InQueueMetricsUpdateByTask,
    ) -> Result<(Vec<SqlValues>, InQueueMetricsUpdateByTask)> {
        let (values_seq, window_in_flow_upper_total) = tuples.into_iter().fold(
            Ok((Vec::new(), WindowInFlowByWindowTask::zero())),
            |res, tuple| {
                let (mut values_seq_acc, window_in_flow_acc) = res?;

                let (mut values_seq, window_in_flow) = self.run_upper_ops_inner(tuple)?;
                values_seq_acc.append(&mut values_seq);
                Ok((values_seq_acc, window_in_flow_acc + window_in_flow))
            },
        )?;
        let in_queue_metrics_update_by_task = InQueueMetricsUpdateByTask::new(
            in_queue_metrics_update_by_lower.by_collect,
            Some(window_in_flow_upper_total + in_queue_metrics_update_by_lower.window_in_flow),
        );

        Ok((values_seq, in_queue_metrics_update_by_task))
    }
    fn run_upper_ops_inner(
        &self,
        tuple: Tuple,
    ) -> Result<(Vec<SqlValues>, WindowInFlowByWindowTask)> {
        if let Some(group_aggr_window_subtask) = &self.group_aggr_window_subtask {
            let (group_aggr_out_seq, window_in_flow) =
                group_aggr_window_subtask.run(&self.expr_resolver, tuple);

            let values = self.run_aggr_projection_op(group_aggr_out_seq)?;

            Ok((values, window_in_flow))
        } else {
            let values = self.run_projection_op(&tuple)?;
            Ok((vec![values], WindowInFlowByWindowTask::zero()))
        }
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

    /// # Returns
    ///
    /// None when input queue does not exist or is empty or JOIN op does not emit output yet.
    fn run_lower_ops(
        &self,
        context: &TaskContext,
    ) -> Option<(Vec<Tuple>, InQueueMetricsUpdateByTask)> {
        match &self.join {
            Some((join_subtask, right_collect_subtask)) => self.run_join(
                context,
                &self.left_collect_subtask,
                right_collect_subtask,
                join_subtask,
            ),
            None => self
                .run_left_collect(context)
                .map(|(tuple, metrics_collect)| {
                    (
                        vec![tuple],
                        InQueueMetricsUpdateByTask::new(
                            metrics_collect,
                            None, // single collect subtask does not use window yet
                        ),
                    )
                }),
        }
    }

    /// JOIN takes tuples from left or right at a time.
    ///
    /// Left or right is determined randomly and if first candidate does not have tuple to collect, then the other is selected.
    fn run_join(
        &self,
        context: &TaskContext,
        left_collect_subtask: &CollectSubtask,
        right_collect_subtask: &CollectSubtask,
        join_subtask: &JoinSubtask,
    ) -> Option<(Vec<Tuple>, InQueueMetricsUpdateByTask)> {
        self.join_dir_candidates().into_iter().find_map(|dir| {
            let collect_subtask = match dir {
                JoinDir::Left => left_collect_subtask,
                JoinDir::Right => right_collect_subtask,
            };
            self.run_join_core(context, collect_subtask, join_subtask, dir)
        })
    }
    fn join_dir_candidates(&self) -> [JoinDir; 2] {
        let first = [JoinDir::Left, JoinDir::Right]
            .choose(&mut *self.rng.lock().expect("rng lock poisoned"))
            .expect("not empty");
        if first == &JoinDir::Left {
            [JoinDir::Left, JoinDir::Right]
        } else {
            [JoinDir::Right, JoinDir::Left]
        }
    }
    fn run_join_core(
        &self,
        context: &TaskContext,
        collect_subtask: &CollectSubtask,
        join_subtask: &JoinSubtask,
        join_dir: JoinDir,
    ) -> Option<(Vec<Tuple>, InQueueMetricsUpdateByTask)> {
        collect_subtask
            .run(context)
            .map(|(tuple, metrics_collect)| {
                let (tuples, metrics_join) = join_subtask.run(&self.expr_resolver, tuple, join_dir);
                let metrics = InQueueMetricsUpdateByTask::new(metrics_collect, Some(metrics_join));
                (tuples, metrics)
            })
    }

    fn run_left_collect(
        &self,
        context: &TaskContext,
    ) -> Option<(Tuple, InQueueMetricsUpdateByCollect)> {
        self.left_collect_subtask.run(context)
    }

    pub(in crate::stream_engine::autonomous_executor) fn get_aggr_window_mut(
        &self,
    ) -> Option<MutexGuard<AggrWindow>> {
        self.group_aggr_window_subtask
            .as_ref()
            .map(|subtask| subtask.get_window_mut())
    }
    pub(in crate::stream_engine::autonomous_executor) fn get_join_window_mut(
        &self,
    ) -> Option<MutexGuard<JoinWindow>> {
        self.join
            .as_ref()
            .map(|(subtask, _)| subtask.get_window_mut())
    }
}
