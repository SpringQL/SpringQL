// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::pipeline::{
    pump_model::PumpModel, sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel, stream_model::StreamModel,
};

#[allow(clippy::enum_variant_names)]
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateSourceStream(StreamModel),
    CreateSourceReader(SourceReaderModel),
    CreateStream(StreamModel),
    CreateSinkStream(StreamModel),
    CreateSinkWriter(SinkWriterModel),
    CreatePump(Box<PumpModel>),
}
