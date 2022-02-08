// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::expr_resolver::expr_label::ExprLabel;
use crate::expr_resolver::ExprResolver;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;

use super::SqlValues;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct ProjectionSubtask(Vec<ExprLabel>);

impl ProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        expr_resolver: &mut ExprResolver,
        tuple: &Tuple,
    ) -> Result<SqlValues> {
        let values = self
            .0
            .iter()
            .map(|label| expr_resolver.eval(*label, tuple))
            .collect::<Result<Vec<_>>>()?;
        Ok(SqlValues::new(values))
    }
}
