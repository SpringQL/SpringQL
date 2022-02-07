use super::SelectSyntaxAnalyzer;
use crate::{
    error::{Result, SpringError},
    expression::{function_call::FunctionCall, ValueExprPh1},
    pipeline::{
        field::{field_name::ColumnReference, field_pointer::FieldPointer},
        name::{AttributeName, ColumnName, StreamName},
    },
    sql_processor::sql_parser::syntax::SelectFieldSyntax,
};
use anyhow::anyhow;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn field_expressions(&self) -> Vec<ValueExprPh1> {
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
            ValueExprPh1::Constant(_) => {
                unimplemented!("constant in select field is not supported currently",)
            }
            ValueExprPh1::UnaryOperator(_, _) => {
                // TODO Better to shrink expression in this layer.
                unimplemented!("unary operation in select field is not supported currently",)
            }
            ValueExprPh1::BooleanExpr(_) => {
                // TODO will use label for projection
                Ok(ColumnReference::new(
                    StreamName::new("_".to_string()),
                    ColumnName::new("_".to_string()),
                ))
            }
            ValueExprPh1::ColumnReference(colref) => Ok(colref.clone()),
            ValueExprPh1::FunctionCall(fun_call) => match fun_call {
                FunctionCall::FloorTime { target, .. } => {
                    // TODO will use label for projection
                    match target.as_ref() {
                        ValueExprPh1::ColumnReference(colref) => Ok(colref.clone()),
                        _ => unimplemented!(),
                    }
                }
                FunctionCall::DurationSecs { .. } => {
                    unreachable!("DURATION_SECS() cannot appear in field list")
                }
            },
        }
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - `prefix` does not match any of `from_item_correlations`.
    fn field_name_with_prefix(
        prefix: &str,
        attr: &str,
        from_item_streams: &[StreamName],
    ) -> Result<ColumnReference> {
        assert!(!from_item_streams.is_empty());

        let attr = AttributeName::new(attr.to_string());
        let pointer = FieldPointer::from(format!("{}.{}", prefix, attr).as_str());

        // SELECT T.C FROM ...;
        from_item_streams
            .iter()
            .find_map(|stream_name| {
                // creates ColumnReference to use .matches()
                let colref_candidate =
                    ColumnReference::new(stream_name.clone(), ColumnName::new(attr.to_string()));
                colref_candidate.matches(&pointer).then(|| colref_candidate)
            })
            .ok_or_else(|| {
                SpringError::Sql(anyhow!(
                    "`{}` does not match any of FROM items: {:?}",
                    pointer,
                    from_item_streams
                ))
            })
    }

    fn field_name_without_prefix(
        attr: &str,
        from_item_streams: &[StreamName],
    ) -> Result<ColumnReference> {
        assert!(!from_item_streams.is_empty());
        if from_item_streams.len() > 1 {
            return Err(SpringError::Sql(anyhow!(
                "needs pipeline info to detect which stream has the column `{:?}`",
                attr
            )));
        }

        // SELECT C FROM T (AS a)?;
        // -> C is from T
        let from_item_stream = from_item_streams[0].clone();
        let attr = AttributeName::new(attr.to_string());
        Ok(ColumnReference::new(
            from_item_stream,
            ColumnName::new(attr.to_string()),
        ))
    }
}
