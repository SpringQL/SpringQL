// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::pipeline::name::ColumnName;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::dependency_injection::DependencyInjection;

#[derive(Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct ProjectionSubtask(Vec<ColumnName>);

impl ProjectionSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn run<DI>(&self, row: Row) -> Result<Row>
    where
        DI: DependencyInjection,
    {
        row.projection::<DI>(&self.0)
    }
}
