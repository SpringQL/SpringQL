// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::expression::function_call::FunctionCall;
use crate::expression::ValueExpr;
use crate::pipeline::field::field_name::ColumnReference;
use crate::pipeline::field::Field;
use crate::pipeline::name::{ColumnName, StreamName};
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct EvalValueExprSubtask {
    expressions: Vec<ValueExpr>, // TODO include both ValueExpr and AggrExpr (enum?)
}

impl EvalValueExprSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(&self, tuple: Tuple) -> Result<Tuple> {
        let rowtime = *tuple.rowtime();

        let new_fields = self
            .expressions
            .iter()
            .map(|expr_ph1| {
                let colref = match expr_ph1 {
                    ValueExpr::ColumnReference(colref) => colref.clone(),
                    ValueExpr::FunctionCall(fun_call) => match fun_call {
                        FunctionCall::FloorTime { target, .. } => {
                            // TODO will use label for projection
                            match target.as_ref() {
                                ValueExpr::ColumnReference(colref) => colref.clone(),
                                _ => unimplemented!(),
                            }
                        }
                        FunctionCall::DurationSecs { .. } => {
                            unreachable!("DURATION_SECS() cannot appear in field list")
                        }
                    },
                    _ => ColumnReference::new(
                        StreamName::new("_".to_string()),
                        ColumnName::new("_".to_string()),
                    ),
                };

                let expr_ph2 = expr_ph1.clone().resolve_colref(&tuple)?;
                let value = expr_ph2.eval()?;

                Ok(Field::new(colref, value))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Tuple::new(rowtime, new_fields))
    }
}
