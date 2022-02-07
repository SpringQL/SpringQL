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
}
