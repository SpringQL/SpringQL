// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use crate::pipeline::{
    name::StreamName, pump_model::PumpModel, sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel,
};

#[derive(Clone, Debug)]
pub(crate) enum Edge {
    Pump {
        /// A pump can have 2 or more upstreams (on JOIN, for example). Then, graph edges share the same PumpModel.
        pump_model: Arc<PumpModel>,
        upstream: StreamName,
    },
    Source(SourceReaderModel),
    Sink(SinkWriterModel),
}
