// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::error::Result;
use crate::low_level_rs::SpringSourceReaderConfig;
use crate::pipeline::option::Options;
use crate::pipeline::source_reader_model::source_reader_type::SourceReaderType;
use crate::stream_engine::autonomous_executor::task::source_task::source_reader::SourceReader;

use super::net_client::NetClientSourceReader;

pub(in crate::stream_engine::autonomous_executor) struct SourceReaderFactory;

impl SourceReaderFactory {
    pub(in crate::stream_engine::autonomous_executor) fn source(
        source_reader_type: &SourceReaderType,
        options: &Options,
        config: &SpringSourceReaderConfig,
    ) -> Result<Box<dyn SourceReader>> {
        let source = match source_reader_type {
            SourceReaderType::NetClient => NetClientSourceReader::start(options, config),
        }?;
        Ok(Box::new(source))
    }
}
