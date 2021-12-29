// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod sink_writer_type;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::sink_writer_type::SinkWriterType;

use super::{foreign_stream_model::ForeignStreamModel, name::SinkWriterName, option::Options};

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(crate) struct SinkWriter {
    name: SinkWriterName,
    sink_writer_type: SinkWriterType,
    from_foreign_stream: Arc<ForeignStreamModel>,
    options: Options,
}

impl SinkWriter {
    pub(crate) fn new(
        sink_writer_type: SinkWriterType,
        from_foreign_stream: Arc<ForeignStreamModel>,
        options: Options,
    ) -> Self {
        Self {
            name: SinkWriterName::from(&sink_writer_type),
            sink_writer_type,
            from_foreign_stream,
            options,
        }
    }

    pub(crate) fn name(&self) -> &SinkWriterName {
        &self.name
    }

    pub(crate) fn sink_writer_type(&self) -> &SinkWriterType {
        &self.sink_writer_type
    }

    pub(crate) fn from_foreign_stream(&self) -> Arc<ForeignStreamModel> {
        self.from_foreign_stream.clone()
    }

    pub(crate) fn options(&self) -> &Options {
        &self.options
    }
}
