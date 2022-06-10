// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

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
    api::error::Result,
    expr_resolver::{expr_label::ExprLabel, ExprResolver},
    pipeline::{
        AggregateParameter, GroupByLabels, Pipeline, WindowOperationParameter, WindowParameter,
    },
    sql_processor::{
        query_planner::select_syntax_analyzer::SelectSyntaxAnalyzer,
        sql_parser::syntax::{GroupingElementSyntax, SelectStreamSyntax},
    },
    stream_engine::command::query_plan::{
        query_plan_operation::{GroupAggregateWindowOp, JoinOp, LowerOps, ProjectionOp, UpperOps},
        QueryPlan,
    },
};

#[derive(Debug)]
pub struct QueryPlanner {
    analyzer: SelectSyntaxAnalyzer,
}

impl QueryPlanner {
    pub fn new(select_stream_syntax: SelectStreamSyntax) -> Self {
        Self {
            analyzer: SelectSyntaxAnalyzer::new(select_stream_syntax),
        }
    }

    pub fn plan(self, pipeline: &Pipeline) -> Result<QueryPlan> {
        let (mut expr_resolver, labels_select_list) =
            ExprResolver::new(self.analyzer.select_list().to_vec());
        let projection = ProjectionOp {
            expr_labels: labels_select_list,
        };

        let group_aggr_window =
            self.create_group_aggr_window_op(&projection, &mut expr_resolver)?;

        let upper_ops = UpperOps {
            projection,
            group_aggr_window,
        };

        let join = self.create_join_op(&mut expr_resolver, pipeline)?;
        let lower_ops = LowerOps { join };

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
                op_param: WindowOperationParameter::Aggregate(group_aggr_param),
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
    ) -> Result<Option<AggregateParameter>> {
        let grouping_elements = self.analyzer.grouping_elements();
        let aggr_labels = projection_op
            .expr_labels
            .iter()
            .filter_map(|label| {
                if let ExprLabel::Aggr(aggr_label) = label {
                    Some(*aggr_label)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        match aggr_labels.len() {
            1 => {
                let aggr_label = aggr_labels.get(0).expect("len checked");
                let aggr_func = expr_resolver.resolve_aggr_expr(*aggr_label).func;

                let group_by_labels = grouping_elements
                    .iter()
                    .map(|grouping_elem| match grouping_elem {
                        GroupingElementSyntax::ValueExpr(expr) => {
                            Ok(expr_resolver.register_value_expr(expr.clone()))
                        }
                        GroupingElementSyntax::ValueAlias(alias) => {
                            expr_resolver.resolve_value_alias(alias.clone())
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(Some(AggregateParameter::new(
                    aggr_func,
                    *aggr_label,
                    GroupByLabels::new(group_by_labels),
                )))
            }
            0 => Ok(None),
            _ => unimplemented!("2 or more aggregate expressions"),
        }
    }

    fn create_join_op(
        &self,
        expr_resolver: &mut ExprResolver,
        pipeline: &Pipeline,
    ) -> Result<JoinOp> {
        self.analyzer.join_op(expr_resolver, pipeline)
    }
}
