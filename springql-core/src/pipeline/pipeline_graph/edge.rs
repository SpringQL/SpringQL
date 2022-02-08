// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::pipeline::{
    pump_model::PumpModel, sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel,
};

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Edge {
    /// A pump can have 2 or more upstreams (on JOIN, for example). Then, graph edges share the same PumpModel.
    Pump(Arc<PumpModel>),
    Source(SourceReaderModel),
    Sink(SinkWriterModel),
}
