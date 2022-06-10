// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    pipeline::{PumpName, SinkWriterModel, SourceReaderModel, StreamModel},
    sql_processor::sql_parser::syntax::SelectStreamSyntax,
    stream_engine::command::insert_plan::InsertPlan,
};

#[allow(clippy::enum_variant_names)]
#[derive(Clone, PartialEq, Debug)]
pub enum ParseSuccess {
    CreateSourceStream(StreamModel),
    CreateSourceReader(SourceReaderModel),
    CreateStream(StreamModel),
    CreateSinkStream(StreamModel),
    CreateSinkWriter(SinkWriterModel),
    CreatePump(Box<CreatePump>),
}

#[derive(Clone, PartialEq, Debug)]
pub struct CreatePump {
    pub pump_name: PumpName,
    pub select_stream_syntax: SelectStreamSyntax,
    pub insert_plan: InsertPlan,
}
