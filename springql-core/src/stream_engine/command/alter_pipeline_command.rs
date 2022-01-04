// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::pipeline::{
    pump_model::PumpModel, sink_stream_model::SinkStreamModel, sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel, source_stream_model::SourceStreamModel,
};

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateSourceStream(SourceStreamModel),
    CreateSourceReader(SourceReaderModel),
    CreateSinkStream(SinkStreamModel),
    CreateSinkWriter(SinkWriterModel),
    CreatePump(PumpModel),
}
