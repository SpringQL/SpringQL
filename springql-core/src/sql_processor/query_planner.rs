//! Translates `SelectSyntax` into `ExprResolver` and `QueryPlan`.
//!
//! `ExprResolver` is to resolve aliases in select_list. Each operation only has `ExprLabel` returned from `ExprResolver`.
//!
//! Query plan is represented by binary tree of operator nodes; most nodes have only a child but JOIN node has two children.
//!
//! Query plan firstly collect `Row`s from streams and they are converted into simplified `Tuple`s who have `Map<ColumnReference, SqlValue>` structure.
//! A tuple may be firstly dropped by single stream selection.
//! Then tuples may be joined.
//! And finally tuples may be dropped again by multi stream selection.
//!
//! ```text
//! (root)
//!
//!  ^
//!  | Tuple (0~)
//!  |
//! multi stream selection
//!  ^
//!  | Tuple (0~)
//!  |
//! join (window)  <--- Option<Tuple> --- ....
//!  ^
//!  | Tuple (0/1)
//!  |
//! single stream selection
//!  ^
//!  | Tuple
//!  |
//! row to tuple
//!  ^
//!  | Row
//!  |
//! collect
//!
//! (leaf)
//! ```
//!
//! Ascendant operators of multi stream selection operator do not modify tuples' structure but just reference them to resolve ColumnReference in expressions.
//!
//! ```text
//! (root)
//!
//! projection
//!  ^
//!  |
//! group aggregation (window)
//!
//! Tuple
//!
//! (leaf)
//! ```
//!
//! Projection operator emits a `SqlValues` by evaluating its expressions (via `ExprLabel`) using `Tuple` for column references.

mod select_syntax_analyzer;

use crate::{
    error::Result,
    expr_resolver::ExprResolver,
    pipeline::{
        pump_model::{
            window_operation_parameter::{
                aggregate::GroupAggregateParameter, WindowOperationParameter,
            },
            window_parameter::WindowParameter,
        },
        Pipeline,
    },
    stream_engine::command::query_plan::{
        query_plan_operation::{
            CollectOp, GroupAggregateWindowOp, JoinOp, LowerOps, ProjectionOp, UpperOps,
        },
        QueryPlan,
    },
};

use self::select_syntax_analyzer::SelectSyntaxAnalyzer;

use super::sql_parser::syntax::{GroupingElementSyntax, SelectStreamSyntax};

#[derive(Debug)]
pub(crate) struct QueryPlanner {
    analyzer: SelectSyntaxAnalyzer,
}

impl QueryPlanner {
    pub(in crate::sql_processor) fn new(select_stream_syntax: SelectStreamSyntax) -> Self {
        Self {
            analyzer: SelectSyntaxAnalyzer::new(select_stream_syntax),
        }
    }

    pub(crate) fn plan(self, _pipeline: &Pipeline) -> Result<QueryPlan> {
        let (mut expr_resolver, value_labels_select_list, aggr_labels_select_list) =
            ExprResolver::new(self.analyzer.select_list().to_vec());
        let projection = ProjectionOp {
            value_expr_labels: value_labels_select_list,
            aggr_expr_labels: aggr_labels_select_list,
        };

        let group_aggr_window =
            self.create_group_aggr_window_op(&projection, &mut expr_resolver)?;

        let upper_ops = UpperOps {
            projection,
            group_aggr_window,
        };

        let collect_ops = self.create_collect_ops()?;
        let collect = collect_ops
            .into_iter()
            .next()
            .expect("collect_ops.len() == 1");
        let lower_ops = LowerOps {
            join: JoinOp::Collect(collect),
        };

        Ok(QueryPlan::new(upper_ops, lower_ops, expr_resolver))
    }

    fn create_group_aggr_window_op(
        &self,
        projection_op: &ProjectionOp,
        expr_resolver: &mut ExprResolver,
    ) -> Result<Option<GroupAggregateWindowOp>> {
        let window_param = self.create_window_param();
        let group_aggr_param = self.create_group_aggr_param(expr_resolver, projection_op)?;

        match (window_param, group_aggr_param) {
            (Some(window_param), Some(group_aggr_param)) => Ok(Some(GroupAggregateWindowOp {
                window_param,
                op_param: WindowOperationParameter::GroupAggregation(group_aggr_param),
            })),
            _ => Ok(None),
        }
    }

    fn create_window_param(&self) -> Option<WindowParameter> {
        self.analyzer.window_parameter()
    }

    fn create_group_aggr_param(
        &self,
        expr_resolver: &mut ExprResolver,
        projection_op: &ProjectionOp,
    ) -> Result<Option<GroupAggregateParameter>> {
        let opt_grouping_elem = self.analyzer.grouping_element();
        let aggr_labels = &projection_op.aggr_expr_labels;

        match (opt_grouping_elem, aggr_labels.len()) {
            (Some(grouping_elem), 1) => {
                let aggr_label = aggr_labels.iter().next().expect("len checked");
                let aggr_func = expr_resolver.resolve_aggr_expr(*aggr_label).func;

                let group_by_label = match grouping_elem {
                    GroupingElementSyntax::ValueExpr(expr) => {
                        expr_resolver.register_value_expr(expr)
                    }
                    GroupingElementSyntax::ValueAlias(alias) => {
                        expr_resolver.resolve_value_alias(alias)?
                    }
                };

                Ok(Some(GroupAggregateParameter::new(
                    aggr_func,
                    *aggr_label,
                    group_by_label,
                )))
            }
            (None, 0) => Ok(None),
            _ => unimplemented!(),
        }
    }

    fn create_collect_ops(&self) -> Result<Vec<CollectOp>> {
        todo!("create join op")
    }
}
