// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    pipeline::{
        name::PumpName, sink_writer_model::SinkWriterModel, source_reader_model::SourceReaderModel,
        stream_model::StreamModel,
    },
    stream_engine::command::insert_plan::InsertPlan,
};

use super::syntax::SelectStreamSyntax;

#[allow(clippy::enum_variant_names)]
#[derive(Clone, PartialEq, Debug)]
pub(in crate::sql_processor) enum ParseSuccess {
    CreateSourceStream(StreamModel),
    CreateSourceReader(SourceReaderModel),
    CreateStream(StreamModel),
    CreateSinkStream(StreamModel),
    CreateSinkWriter(SinkWriterModel),
    CreatePump(Box<CreatePump>),
}

#[derive(Clone, PartialEq, Debug)]
pub(in crate::sql_processor) struct CreatePump {
    pub(in crate::sql_processor) pump_name: PumpName,
    pub(in crate::sql_processor) select_stream_syntax: SelectStreamSyntax,
    pub(in crate::sql_processor) insert_plan: InsertPlan,
}
