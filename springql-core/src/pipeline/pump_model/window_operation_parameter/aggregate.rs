use crate::expr_resolver::expr_label::ExprLabel;

/// TODO [support complex expression with aggregations](https://gh01.base.toyota-tokyo.tech/SpringQL-internal/SpringQL/issues/152)
///
/// ```sql
/// SELECT group_by, aggr_expr.func(aggr_expr.aggregated)
///   FROM s
///   GROUP BY group_by
///   SLIDING WINDOW ...;
/// ```
#[derive(Clone, PartialEq, Debug, new)]
pub(crate) struct GroupAggregateParameter {
    pub(crate) aggr_expr: ExprLabel, // TODO multiple aggr_expr
    pub(crate) group_by: ExprLabel,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AggregateFunctionParameter {
    Avg,
}
