// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod source_reader_type;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::source_reader_type::SourceReaderType;

use super::{
    foreign_stream_model::ForeignStreamModel,
    name::{SourceReaderName, StreamName},
    option::Options,
};

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, new)]
pub(crate) struct SourceReaderModel {
    name: SourceReaderName,
    source_reader_type: SourceReaderType,
    dest_source_stream: StreamName,
    options: Options,
}

impl SourceReaderModel {
    pub(crate) fn name(&self) -> &SourceReaderName {
        &self.name
    }

    pub(crate) fn source_reader_type(&self) -> &SourceReaderType {
        &self.source_reader_type
    }

    pub(crate) fn dest_source_stream(&self) -> &StreamName {
        &self.dest_source_stream
    }

    pub(crate) fn options(&self) -> &Options {
        &self.options
    }
}
