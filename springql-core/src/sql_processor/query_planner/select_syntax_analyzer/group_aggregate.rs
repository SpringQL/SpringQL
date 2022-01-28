use super::SelectSyntaxAnalyzer;
use crate::error::Result;
use crate::pipeline::pump_model::window_operation_parameter::aggregate::{
    AggregateParameter, GroupAggregateParameter,
};
use crate::sql_processor::sql_parser::syntax::SelectFieldSyntax;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn group_aggregate_parameter(
        &self,
    ) -> Result<Option<GroupAggregateParameter>> {
        let opt_group_by = self.select_syntax.grouping_element;
        let mut aggregate_parameters = self.aggregate_parameters();

        match (opt_group_by, aggregate_parameters.len()) {
            (Some(group_by), 1) => Ok(Some(GroupAggregateParameter::new(
                aggregate_parameters.next().expect("len checked"),
                group_by,
            ))),
            (None, 0) => Ok(None),
            _ => unimplemented!(),
        }
    }

    pub(in super::super) fn aggregate_parameters(&self) -> Vec<AggregateParameter> {
        self.select_syntax
            .fields
            .iter()
            .filter_map(|field| match field {
                SelectFieldSyntax::Expression { .. } => None,
                SelectFieldSyntax::Aggregate(aggregate_parameter) => {
                    Some(aggregate_parameter.clone())
                }
            })
            .collect()
    }
}
