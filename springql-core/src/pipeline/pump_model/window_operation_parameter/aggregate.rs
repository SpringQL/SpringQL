use crate::pipeline::{
    field::{aliased_field_name::AliasedFieldName, field_pointer::FieldPointer},
    name::FieldAlias,
};

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
    pub(crate) group_by: FieldPointer,
}

impl GroupAggregateParameter {
    pub(crate) fn group_by_aliased_field_name(&self) -> AliasedFieldName {
        AliasedFieldName::from_only_alias(FieldAlias::new(self.group_by.attr().to_string()))
    }
}

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(crate) struct AggregateParameter {
    pub(crate) aggregated: FieldPointer,
    pub(crate) aggregated_alias: FieldAlias,
    pub(crate) aggregate_function: AggregateFunctionParameter,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AggregateFunctionParameter {
    Avg,
}

impl AggregateParameter {
    pub(crate) fn aggregated_aliased_field_name(&self) -> AliasedFieldName {
        AliasedFieldName::from_only_alias(self.aggregated_alias.clone())
    }
}
