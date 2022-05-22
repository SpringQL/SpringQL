// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#![doc = include_str!("pipeline.md")]

pub(crate) mod field;
pub(crate) mod name;
pub(crate) mod option;
pub(crate) mod pipeline_graph;
pub(crate) mod pipeline_version;
pub(crate) mod pump_model;
pub(crate) mod relation;
pub(crate) mod sink_writer_model;
pub(crate) mod source_reader_model;
pub(crate) mod stream_model;

#[cfg(test)]
pub(crate) mod test_support;

use anyhow::anyhow;
use std::{collections::HashSet, sync::Arc};

use crate::error::{Result, SpringError};
use crate::stream_engine::{SinkRow, SqlValue};
use crate::SpringValue;

use self::{
    name::StreamName, pipeline_graph::PipelineGraph, pipeline_version::PipelineVersion,
    pump_model::PumpModel, sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel, stream_model::StreamModel,
};

pub use crate::api::low_level_rs::{
    spring_command, spring_open, spring_pop, spring_pop_non_blocking, SpringPipeline,
};

/// Get an integer column.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - `i_col` already fetched.
///   - `i_col` out of range.
/// - [SpringError::Null](crate::error::SpringError::Null) when:
///   - Column value is NULL
pub fn spring_column_i32(row: &SpringRow, i_col: usize) -> Result<i32> {
    spring_column_not_null(row, i_col)
}

/// Get a 2-byte integer column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_i16(row: &SpringRow, i_col: usize) -> Result<i16> {
    spring_column_not_null(row, i_col)
}

/// Get a 8-byte integer column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_i64(row: &SpringRow, i_col: usize) -> Result<i64> {
    spring_column_not_null(row, i_col)
}

/// Get a float column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_f32(row: &SpringRow, i_col: usize) -> Result<f32> {
    spring_column_not_null(row, i_col)
}

/// Get a boolean column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_bool(row: &SpringRow, i_col: usize) -> Result<bool> {
    spring_column_not_null(row, i_col)
}

/// Get a text column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_text(row: &SpringRow, i_col: usize) -> Result<String> {
    spring_column_not_null(row, i_col)
}

fn spring_column_not_null<T: SpringValue>(row: &SpringRow, i_col: usize) -> Result<T> {
    let v = row.0.get_by_index(i_col)?;
    if let SqlValue::NotNull(v) = v {
        v.unpack()
    } else {
        Err(SpringError::Null {
            stream_name: row.0.stream_name().clone(),
            i_col,
        })
    }
}

/// Row object from an in memory queue.
#[derive(Debug)]
pub struct SpringRow(pub(crate) SinkRow);

impl From<SinkRow> for SpringRow {
    fn from(sink_row: SinkRow) -> Self {
        Self(sink_row)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Pipeline {
    version: PipelineVersion,
    object_names: HashSet<String>,
    graph: PipelineGraph,
}

impl Pipeline {
    pub(super) fn new(version: PipelineVersion) -> Self {
        Self {
            version,
            object_names: HashSet::default(),
            graph: PipelineGraph::default(),
        }
    }

    pub(super) fn version(&self) -> PipelineVersion {
        self.version
    }

    pub(super) fn as_graph(&self) -> &PipelineGraph {
        &self.graph
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Stream is not registered in pipeline
    pub(super) fn get_stream(&self, stream: &StreamName) -> Result<Arc<StreamModel>> {
        self.graph.get_stream(stream)
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of pump is already used in the same pipeline
    ///   - Name of upstream stream is not found in pipeline
    ///   - Name of downstream stream is not found in pipeline
    pub(super) fn add_pump(&mut self, pump: PumpModel) -> Result<()> {
        self.update_version();
        self.register_name(pump.name().as_ref())?;
        self.graph.add_pump(pump)
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name of stream is already used in the same pipeline
    pub(super) fn add_stream(&mut self, stream: Arc<StreamModel>) -> Result<()> {
        self.update_version();
        self.register_name(stream.name().as_ref())?;
        self.graph.add_stream(stream)
    }

    /// # Failure
    ///
    /// TODO
    pub(super) fn add_source_reader(&mut self, source_reader: SourceReaderModel) -> Result<()> {
        self.update_version();
        self.graph.add_source_reader(source_reader)
    }
    /// # Failure
    ///
    /// TODO
    pub(super) fn add_sink_writer(&mut self, sink_writer: SinkWriterModel) -> Result<()> {
        self.update_version();
        self.graph.add_sink_writer(sink_writer)
    }

    pub(super) fn all_sources(&self) -> Vec<&SourceReaderModel> {
        self.graph.all_sources()
    }
    pub(super) fn all_sinks(&self) -> Vec<&SinkWriterModel> {
        self.graph.all_sinks()
    }

    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Name is already used in the same pipeline
    fn register_name(&mut self, name: &str) -> Result<()> {
        if !self.object_names.insert(name.to_string()) {
            Err(SpringError::Sql(anyhow!(
                r#"name "{}" already exists in pipeline"#,
                name
            )))
        } else {
            Ok(())
        }
    }

    fn update_version(&mut self) {
        self.version.up();
    }
}
