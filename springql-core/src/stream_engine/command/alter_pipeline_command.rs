// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::pipeline::{
    foreign_stream_model::ForeignStreamModel, pump_model::PumpModel,
    sink_writer_model::SinkWriterModel, source_reader_model::SourceReaderModel,
};

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateSourceStream(ForeignStreamModel),
    CreateSourceReader(SourceReaderModel),
    CreateSinkStream(ForeignStreamModel),
    CreateSinkWriter(SinkWriterModel),
    CreatePump(PumpModel),
}
