use super::SelectSyntaxAnalyzer;
use crate::{
    error::Result,
    expression::{function_call::FunctionCall, ValueExpr},
    pipeline::{
        field::field_name::ColumnReference,
        name::{ColumnName, StreamName},
    },
    sql_processor::sql_parser::syntax::SelectFieldSyntax,
};

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn field_expressions(&self) -> Vec<ValueExpr> {
        let select_fields = &self.select_syntax.fields;
        select_fields
            .iter()
            .map(|field| &field.value_expr)
            .cloned()
            .collect()
    }

    pub(in super::super) fn column_references_in_projection(&self) -> Result<Vec<ColumnReference>> {
        let from_item_correlations = self.from_item_streams()?;
        let select_fields = &self.select_syntax.fields;

        select_fields
            .iter()
            .map(|select_field| {
                Self::select_field_into_colref(select_field, &from_item_correlations)
            })
            .collect::<Result<_>>()
    }

    fn select_field_into_colref(
        select_field: &SelectFieldSyntax,
        from_item_streams: &[StreamName],
    ) -> Result<ColumnReference> {
        match &select_field.value_expr {
            ValueExpr::Constant(_) => {
                unimplemented!("constant in select field is not supported currently",)
            }
            ValueExpr::UnaryOperator(_, _) => {
                // TODO Better to shrink expression in this layer.
                unimplemented!("unary operation in select field is not supported currently",)
            }
            ValueExpr::BooleanExpr(_) => {
                // TODO will use label for projection
                Ok(ColumnReference::new(
                    StreamName::new("_".to_string()),
                    ColumnName::new("_".to_string()),
                ))
            }
            ValueExpr::ColumnReference(colref) => Ok(colref.clone()),
            ValueExpr::FunctionCall(fun_call) => match fun_call {
                FunctionCall::FloorTime { target, .. } => {
                    // TODO will use label for projection
                    match target.as_ref() {
                        ValueExpr::ColumnReference(colref) => Ok(colref.clone()),
                        _ => unimplemented!(),
                    }
                }
                FunctionCall::DurationSecs { .. } => {
                    unreachable!("DURATION_SECS() cannot appear in field list")
                }
            },
        }
    }
}
