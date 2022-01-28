mod select_syntax_analyzer;

use crate::{
    error::Result,
    pipeline::{field::field_pointer::FieldPointer, name::StreamName, Pipeline},
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
/// selection
///  |
/// sort
///  |
/// aggregation
///  |
/// join
///  |
/// window
///  |--------+
/// collect  collect
/// ```
///
/// Nodes are created from bottom to top.
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

        let group_aggregate_window_op = self.create_group_aggregate_window_op()?; // TODO loosely coupled GROUP BY, AGGR(*), and WINDOW.

        // self.create_window_nodes()?;
        // self.create_join_nodes()?;
        // self.create_aggregation_nodes()?;
        // self.create_sort_node()?;
        // self.create_selection_node()?;
        let projection_op = self.create_projection_op()?;

        let mut plan = self.plan;
        plan.add_root(projection_op.clone());

        let parent_op = if let Some(op) = group_aggregate_window_op {
            plan.add_left(&projection_op, op.clone());
            op
        } else {
            projection_op
        };

        // TODO group_aggregate_window_op
        plan.add_left(&parent_op, collect_op);
        Ok(plan)
    }

    fn create_collect_ops(&self) -> Result<Vec<QueryPlanOperation>> {
        let from_item_correlations = self.analyzer.from_item_correlations()?;
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
            .map(|aliased_correlation_name| {
                let stream = StreamName::new(aliased_correlation_name.correlation_name.to_string());
                let aliaser = self.analyzer.aliaser()?;
                Ok(QueryPlanOperation::Collect { stream, aliaser })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn create_group_aggregate_window_op(&self) -> Result<Option<QueryPlanOperation>> {
        Ok(None) // TODO
    }

    fn create_projection_op(&self) -> Result<QueryPlanOperation> {
        let aliased_field_names = self.analyzer.aliased_field_names_in_projection()?;

        let projection_op = QueryPlanOperation::Projection {
            field_pointers: aliased_field_names.iter().map(FieldPointer::from).collect(),
        };
        Ok(projection_op)
    }
}
