// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod source_reader_type;

use crate::pipeline::{
    name::{SourceReaderName, StreamName},
    option::Options,
    source_reader_model::source_reader_type::SourceReaderType,
};

#[derive(Clone, PartialEq, Eq, Debug, new)]
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
