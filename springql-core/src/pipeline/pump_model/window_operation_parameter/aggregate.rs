use crate::pipeline::{field::field_name::ColumnReference, name::FieldAlias};

/// TODO [support complex expression with aggregations](https://gh01.base.toyota-tokyo.tech/SpringQL-internal/SpringQL/issues/152)
///
/// ```sql
/// SELECT group_by, aggregate_function(aggregated) AS aggregated_alias
///   FROM s
///   GROUP BY group_by
///   SLIDING WINDOW ...;
/// ```
#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(crate) struct GroupAggregateParameter {
    pub(crate) aggregation_parameter: AggregateParameter,
    pub(crate) group_by: ColumnReference,
}

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(crate) struct AggregateParameter {
    pub(crate) aggregated: ColumnReference,
    pub(crate) aggregated_alias: FieldAlias,
    pub(crate) aggregate_function: AggregateFunctionParameter,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AggregateFunctionParameter {
    Avg,
}
