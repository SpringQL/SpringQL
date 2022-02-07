// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::pipeline::field::field_name::ColumnReference;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct ProjectionSubtask(Vec<ColumnReference>);

impl ProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run(&self, tuple: Tuple) -> Result<Tuple> {
        tuple.projection(&self.0)
    }
}
