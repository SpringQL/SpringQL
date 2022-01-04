// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::pipeline::{
    foreign_stream_model::ForeignStreamModel,
    name::PumpName,
    pump_model::{pump_state::PumpState, PumpModel},
    sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel,
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateSourceStream(ForeignStreamModel),
    CreateSourceReader(SourceReaderModel),
    CreateForeignSinkStream(SinkWriterModel),
    CreatePump(PumpModel),
    AlterPump { name: PumpName, state: PumpState },
}
