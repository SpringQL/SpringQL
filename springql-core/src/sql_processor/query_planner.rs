mod select_syntax_analyzer;

use crate::{
    error::Result,
    pipeline::{field::field_pointer::FieldPointer, Pipeline},
    stream_engine::command::query_plan::{query_plan_operation::QueryPlanOperation, QueryPlan},
};

use self::select_syntax_analyzer::SelectSyntaxAnalyzer;

use super::sql_parser::syntax::SelectStreamSyntax;

/// Translates `SelectSyntax` into `QueryPlan`.
///
/// Output tree has the following form:
///
/// ```text
/// proj
///  |
/// sort
///  |
/// aggregation
///  |
/// join
///  |
/// window
///  |
/// selection
///  |
/// eval value expressions
///  |
///  |--------+
/// collect  collect
/// ```
///
/// Nodes are created from bottom to top.
///
/// `SelectSyntax` has `ExprSyntax`s inside and query planner translates them into `Expr`s, which are evaluated into `SqlValue` with tuples.
/// To be more precise, an `Expr::ValueExpr` is evaluated into `SqlValue` with a tuple and `Expr::AggrExpr` is evaluated into `SqlValue` with set of tuples.
#[derive(Clone, Debug)]
pub(crate) struct QueryPlanner {
    plan: QueryPlan,
    analyzer: SelectSyntaxAnalyzer,
}

impl QueryPlanner {
    pub(in crate::sql_processor) fn new(select_stream_syntax: SelectStreamSyntax) -> Self {
        Self {
            plan: QueryPlan::default(),
            analyzer: SelectSyntaxAnalyzer::new(select_stream_syntax),
        }
    }

    pub(crate) fn plan(self, _pipeline: &Pipeline) -> Result<QueryPlan> {
        let collect_ops = self.create_collect_ops()?;
        let collect_op = collect_ops
            .into_iter()
            .next()
            .expect("collect_ops.len() == 1");

        let eval_value_expr_op = self.create_eval_value_expr_op();

        // self.create_window_nodes()?;
        // self.create_join_nodes()?;
        // self.create_aggregation_nodes()?;
        // self.create_sort_node()?;
        // self.create_selection_node()?;
        let projection_op = self.create_projection_op()?;

        let mut plan = self.plan;
        plan.add_root(projection_op.clone());
        plan.add_left(&projection_op, eval_value_expr_op.clone());
        plan.add_left(&eval_value_expr_op, collect_op);
        Ok(plan)
    }

    fn create_collect_ops(&self) -> Result<Vec<QueryPlanOperation>> {
        let from_item_correlations = self.analyzer.from_item_streams()?;
        assert!(
            !from_item_correlations.is_empty(),
            "at least 1 from item is expected"
        );
        assert!(
            from_item_correlations.len() == 1,
            "1 from item is currently supported"
        );

        from_item_correlations
            .into_iter()
            .map(|stream| Ok(QueryPlanOperation::Collect { stream }))
            .collect::<Result<Vec<_>>>()
    }

    fn create_projection_op(&self) -> Result<QueryPlanOperation> {
        let colrefs = self.analyzer.column_references_in_projection()?;

        let projection_op = QueryPlanOperation::Projection {
            field_pointers: colrefs.iter().map(FieldPointer::from).collect(),
        };
        Ok(projection_op)
    }

    fn create_eval_value_expr_op(&self) -> QueryPlanOperation {
        let expressions = self.analyzer.all_expressions();
        QueryPlanOperation::EvalValueExpr { expressions }
    }
}
