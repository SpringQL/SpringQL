// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::{Result, SpringError};
use crate::expression::Expression;
use crate::pipeline::correlation::aliased_correlation_name::AliasedCorrelationName;
use crate::pipeline::expression_to_field::ExpressionToField;
use crate::pipeline::field::aliased_field_name::AliasedFieldName;
use crate::pipeline::field::field_name::FieldName;
use crate::pipeline::field::field_pointer::FieldPointer;
use crate::pipeline::field::Field;
use crate::pipeline::name::{AttributeName, CorrelationName};
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;
use anyhow::anyhow;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct EvalExpressionSubtask {
    expr_to_fields: Vec<ExpressionToField>,
}

impl EvalExpressionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(&self, tuple: Tuple) -> Result<Tuple> {
        let new_fields = self
            .expr_to_fields
            .iter()
            .map(|expr_to_field| {
                let sql_value = tuple.eval_expression(expr_to_field.expression)?;

                let aliased_field_name = match &expr_to_field.expression {
                    Expression::FieldPointer(field_pointer) => Ok(AliasedFieldName::new(
                        Self::field_name_from_field_pointer(field_pointer),
                        expr_to_field.alias.clone(),
                    )),
                    _ => {
                        let alias = expr_to_field.alias.ok_or_else(|| {
                            SpringError::Sql(anyhow!(
                                "expression (not a field pointer) must have alias"
                            ))
                        })?;
                        Ok(AliasedFieldName::from_only_alias(alias))
                    }
                }?;

                Ok(Field::new(aliased_field_name, sql_value))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Tuple::new(*tuple.rowtime(), new_fields))
    }

    fn field_name_from_field_pointer(field_pointer: &FieldPointer) -> FieldName {
        // FIXME super ugly...

        FieldName::new(
            AliasedCorrelationName::new(
                CorrelationName::new(
                    field_pointer
                        .prefix()
                        .expect("field pointer in select field must have prefix")
                        .to_string(),
                ),
                None,
            ),
            AttributeName::new(field_pointer.attr().to_string()),
        )
    }
}
