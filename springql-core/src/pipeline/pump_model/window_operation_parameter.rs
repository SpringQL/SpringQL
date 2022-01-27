use crate::pipeline::{
    correlation::aliased_correlation_name::AliasedCorrelationName,
    field::{
        aliased_field_name::AliasedFieldName, field_name::FieldName, field_pointer::FieldPointer,
    },
    name::{AttributeName, CorrelationName, FieldAlias},
};

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
#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(crate) struct AggregateParameter {
    pub(crate) group_by: FieldPointer,       // TODO exclude
    pub(crate) aggregated: AliasedFieldName, // TODO FieldPointer
    pub(crate) aggregated_alias: FieldAlias,
    pub(crate) aggregate_function: AggregateFunctionParameter,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AggregateFunctionParameter {
    Avg,
}

impl AggregateParameter {
    pub(crate) fn aggregated_aliased_field_name(&self) -> AliasedFieldName {
        // FIXME field name for expression
        let field_name = FieldName::new(
            AliasedCorrelationName::new(CorrelationName::new("_".to_string()), None),
            AttributeName::new("_".to_string()),
        );
        AliasedFieldName::new(field_name, Some(self.aggregated_alias.clone()))
    }

    pub(crate) fn group_by_aliased_field_name(&self) -> AliasedFieldName {
        let field_name = FieldName::new(
            AliasedCorrelationName::new(CorrelationName::new("_".to_string()), None),
            AttributeName::new("_".to_string()),
        );
        AliasedFieldName::new(
            field_name,
            Some(FieldAlias::new(self.group_by.attr().to_string())),
        )
    }
}
