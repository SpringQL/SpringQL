// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod source_reader_state;
pub(crate) mod source_reader_type;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::{source_reader_state::SourceReaderState, source_reader_type::SourceReaderType};

use super::{
    foreign_stream_model::ForeignStreamModel, name::SourceReaderName, option::Options,
    pipeline_graph::PipelineGraph,
};

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(crate) struct SourceReaderModel {
    name: SourceReaderName,
    source_reader_type: SourceReaderType,
    dest_foreign_stream: Arc<ForeignStreamModel>,
    options: Options,
}

impl SourceReaderModel {
    pub(crate) fn new(
        source_reader_type: SourceReaderType,
        dest_foreign_stream: Arc<ForeignStreamModel>,
        options: Options,
    ) -> Self {
        Self {
            name: SourceReaderName::from(&source_reader_type),
            source_reader_type,
            dest_foreign_stream,
            options,
        }
    }

    pub(crate) fn name(&self) -> &SourceReaderName {
        &self.name
    }

    pub(crate) fn state(&self, pipeline_graph: &PipelineGraph) -> SourceReaderState {
        pipeline_graph.source_reader_state(self.dest_foreign_stream.name())
    }

    pub(crate) fn source_reader_type(&self) -> &SourceReaderType {
        &self.source_reader_type
    }

    pub(crate) fn dest_foreign_stream(&self) -> Arc<ForeignStreamModel> {
        self.dest_foreign_stream.clone()
    }

    pub(crate) fn options(&self) -> &Options {
        &self.options
    }
}
