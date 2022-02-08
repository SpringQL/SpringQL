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
    pipeline::Pipeline,
    stream_engine::command::query_plan::{
        query_plan_operation::{CollectOp, LowerOps, ProjectionOp, UpperOps},
        QueryPlan,
    },
};

use self::select_syntax_analyzer::SelectSyntaxAnalyzer;

use super::sql_parser::syntax::SelectStreamSyntax;

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
        let (expr_resolver, labels_select_list) =
            ExprResolver::new(self.analyzer.select_list().to_vec());
        let projection = ProjectionOp {
            expr_labels: labels_select_list,
        };

        let collect_ops = self.create_collect_ops()?;
        let collect = collect_ops
            .into_iter()
            .next()
            .expect("collect_ops.len() == 1");
        let lower_ops = LowerOps { collect };

        let upper_ops = UpperOps { projection };

        Ok(QueryPlan::new(upper_ops, lower_ops, expr_resolver))
    }

    fn create_collect_ops(&self) -> Result<Vec<CollectOp>> {
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
            .map(|stream| Ok(CollectOp { stream }))
            .collect::<Result<Vec<_>>>()
    }
}
