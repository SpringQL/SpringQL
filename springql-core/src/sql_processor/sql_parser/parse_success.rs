// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    pipeline::{stream_model::StreamModel, PumpName, SinkWriterModel, SourceReaderModel},
    sql_processor::sql_parser::syntax::SelectStreamSyntax,
    stream_engine::command::insert_plan::InsertPlan,
};

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
