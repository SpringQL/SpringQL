// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::pipeline::{
    name::StreamName, pump_model::PumpModel, sink_writer_model::SinkWriterModel,
    source_reader_model::SourceReaderModel,
};

#[derive(Clone, Debug)]
pub enum Edge {
    Pump {
        /// A pump can have 2 or more upstreams (on JOIN, for example). Then, graph edges share the same PumpModel.
        pump_model: Arc<PumpModel>,
        upstream: StreamName,
    },
    Source(SourceReaderModel),
    Sink(SinkWriterModel),
}
