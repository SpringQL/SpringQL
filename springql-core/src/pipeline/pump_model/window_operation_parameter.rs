use crate::pipeline::{field::aliased_field_name::AliasedFieldName, name::FieldAlias};

/// Window operation parameters
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum WindowOperationParameter {
    Aggregation(AggregateParameter),
}

/// TODO [support complex expression with aggregations](https://gh01.base.toyota-tokyo.tech/SpringQL-internal/SpringQL/issues/152)
///
/// ```sql
/// SELECT group_by, aggregate_function(aggregated) AS aggregated_alias
///   FROM s
///   GROUP BY group_by
///   SLIDING WINDOW ...;
/// ```
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct AggregateParameter {
    pub(crate) group_by: AliasedFieldName,
    pub(crate) aggregated: AliasedFieldName,
    pub(crate) aggregated_alias: FieldAlias,
    pub(crate) aggregate_function: AggregateFunctionParameter,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AggregateFunctionParameter {
    Avg,
}
