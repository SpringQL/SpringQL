// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod source_reader_type;
pub use source_reader_type::SourceReaderType;

use crate::pipeline::{
    name::{SourceReaderName, StreamName},
    option::Options,
};

#[derive(Clone, PartialEq, Eq, Debug, new)]
pub struct SourceReaderModel {
    name: SourceReaderName,
    source_reader_type: SourceReaderType,
    dest_source_stream: StreamName,
    options: Options,
}

impl SourceReaderModel {
    pub fn name(&self) -> &SourceReaderName {
        &self.name
    }

    pub fn source_reader_type(&self) -> &SourceReaderType {
        &self.source_reader_type
    }

    pub fn dest_source_stream(&self) -> &StreamName {
        &self.dest_source_stream
    }

    pub fn options(&self) -> &Options {
        &self.options
    }
}
