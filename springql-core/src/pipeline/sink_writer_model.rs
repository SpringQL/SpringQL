// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod sink_writer_type;
use crate::pipeline::{
    name::{SinkWriterName, StreamName},
    option::Options,
};
pub use sink_writer_type::SinkWriterType;

#[derive(Clone, PartialEq, Eq, Debug, new)]
pub struct SinkWriterModel {
    name: SinkWriterName,
    sink_writer_type: SinkWriterType,
    sink_upstream: StreamName,
    options: Options,
}

impl SinkWriterModel {
    pub fn name(&self) -> &SinkWriterName {
        &self.name
    }

    pub fn sink_writer_type(&self) -> &SinkWriterType {
        &self.sink_writer_type
    }

    pub fn sink_upstream(&self) -> &StreamName {
        &self.sink_upstream
    }

    pub fn options(&self) -> &Options {
        &self.options
    }
}
