// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod collect_subtask;
mod group_aggregate_window_subtask;
mod join_subtask;
mod projection_subtask;

use std::sync::{Arc, Mutex, MutexGuard};

use rand::{
    prelude::{SliceRandom, SmallRng},
    SeedableRng,
};

use crate::{
    api::error::Result,
    expr_resolver::ExprResolver,
    pipeline::{ColumnName, StreamModel},
    stream_engine::{
        autonomous_executor::{
            performance_metrics::{
                InQueueMetricsUpdateByCollect, InQueueMetricsUpdateByTask, WindowInFlowByWindowTask,
            },
            row::{ColumnValues, StreamColumns, StreamRow},
            task::{
                pump_task::pump_subtask::query_subtask::{
                    collect_subtask::CollectSubtask,
                    group_aggregate_window_subtask::GroupAggregateWindowSubtask,
                    join_subtask::JoinSubtask, projection_subtask::ProjectionSubtask,
                },
                task_context::TaskContext,
                tuple::Tuple,
                window::{AggrWindow, JoinDir, JoinWindow},
                ProcessedRows,
            },
        },
        command::{JoinOp, LowerOps, QueryPlan},
        SqlValue,
    },
};

/// Process input row 1-by-1.
#[derive(Debug)]
pub struct QuerySubtask {
    expr_resolver: ExprResolver,

    projection_subtask: ProjectionSubtask,

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
pub struct SqlValues(Vec<SqlValue>);
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
    pub fn into_row(
        self,
        stream_model: Arc<StreamModel>,
        column_order: Vec<ColumnName>,
    ) -> StreamRow {
        assert_eq!(self.0.len(), column_order.len());

        let column_values = self.mk_column_values(column_order);
        let stream_columns = StreamColumns::new(stream_model, column_values)
            .expect("type or shape mismatch? must be checked on pump creation");
        StreamRow::new(stream_columns)
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
pub struct QuerySubtaskOut {
    pub values_seq: Vec<SqlValues>,
    pub in_queue_metrics_update: InQueueMetricsUpdateByTask,
}
impl QuerySubtaskOut {
    pub fn processed_rows(&self) -> ProcessedRows {
        ProcessedRows::new(self.values_seq.len() as u64)
    }
}

impl QuerySubtask {
    pub fn new(plan: QueryPlan) -> Self {
        let rng =
            Mutex::new(SmallRng::from_rng(rand::thread_rng()).expect("this generally won't fail"));

        let (left_collect_subtask, join) = Self::subtasks_from_lower_ops(plan.lower_ops);

        let group_aggr_window_subtask = plan
            .upper_ops
            .group_aggr_window
            .map(|op| GroupAggregateWindowSubtask::new(op.window_param, op.op_param));

        let projection_subtask = ProjectionSubtask::new(plan.upper_ops.projection.expr_labels);

        Self {
            expr_resolver: plan.expr_resolver,
            projection_subtask,
            group_aggr_window_subtask,
            left_collect_subtask,
            join,
            rng,
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
    pub fn run(&self, context: &TaskContext) -> Result<Option<QuerySubtaskOut>> {
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
            let (aggregated_and_grouping_values_seq, window_in_flow) =
                group_aggr_window_subtask.run(&self.expr_resolver, tuple);

            let values_seq = aggregated_and_grouping_values_seq
                .into_iter()
                .map(|aggregated_and_grouping_values| {
                    self.projection_subtask
                        .run_with_aggr(aggregated_and_grouping_values)
                })
                .collect::<Result<Vec<_>>>()?;

            Ok((values_seq, window_in_flow))
        } else {
            let values = self
                .projection_subtask
                .run_without_aggr(&self.expr_resolver, &tuple)?;
            Ok((vec![values], WindowInFlowByWindowTask::zero()))
        }
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

    pub fn get_aggr_window_mut(&self) -> Option<MutexGuard<AggrWindow>> {
        self.group_aggr_window_subtask
            .as_ref()
            .map(|subtask| subtask.get_window_mut())
    }
    pub fn get_join_window_mut(&self) -> Option<MutexGuard<JoinWindow>> {
        self.join
            .as_ref()
            .map(|(subtask, _)| subtask.get_window_mut())
    }
}
