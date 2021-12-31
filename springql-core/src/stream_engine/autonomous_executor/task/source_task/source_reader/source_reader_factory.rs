// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::pipeline::option::Options;
use crate::pipeline::source_reader_model::source_reader_type::SourceReaderType;
use crate::stream_engine::autonomous_executor::task::source_task::source_reader::SourceReader;

use super::net::NetSourceReader;

pub(in crate::stream_engine::autonomous_executor) struct SourceReaderFactory;

impl SourceReaderFactory {
    pub(in crate::stream_engine::autonomous_executor) fn source(
        source_reader_type: &SourceReaderType,
        options: &Options,
    ) -> Result<Box<dyn SourceReader>> {
        let source = match source_reader_type {
            SourceReaderType::Net => NetSourceReader::start(options),
        }?;
        Ok(Box::new(source))
    }
}