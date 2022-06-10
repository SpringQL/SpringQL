// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    api::{error::Result, SpringSourceReaderConfig},
    pipeline::{option::Options, source_reader_model::source_reader_type::SourceReaderType},
    stream_engine::autonomous_executor::task::source_task::source_reader::{
        net_client::NetClientSourceReader, net_server::NetServerSourceReader, SourceReader,
    },
};

pub(in crate::stream_engine::autonomous_executor) struct SourceReaderFactory;

impl SourceReaderFactory {
    pub(in crate::stream_engine::autonomous_executor) fn source(
        source_reader_type: &SourceReaderType,
        options: &Options,
        config: &SpringSourceReaderConfig,
    ) -> Result<Box<dyn SourceReader>> {
        match source_reader_type {
            SourceReaderType::NetClient => {
                Ok(Box::new(NetClientSourceReader::start(options, config)?))
            }
            SourceReaderType::NetServer => {
                Ok(Box::new(NetServerSourceReader::start(options, config)?))
            }
        }
    }
}
