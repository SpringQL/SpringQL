// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    pipeline::{
        name::PumpName, sink_stream_model::SinkStreamModel, sink_writer_model::SinkWriterModel,
        source_reader_model::SourceReaderModel, source_stream_model::SourceStreamModel,
    },
    stream_engine::command::insert_plan::InsertPlan,
};

use super::syntax::SelectStreamSyntax;

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) enum ParseSuccess {
    CreateSourceStream(SourceStreamModel),
    CreateSourceReader(SourceReaderModel),
    CreateSinkStream(SinkStreamModel),
    CreateSinkWriter(SinkWriterModel),
    CreatePump {
        pump_name: PumpName,
        select_stream_syntax: SelectStreamSyntax,
        insert_plan: InsertPlan,
    },
}
