// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

pub(crate) mod sink_writer_type;

use std::sync::Arc;

use serde::{Deserialize, Serialize};



use self::sink_writer_type::SinkWriterType;

use super::{
    foreign_stream_model::ForeignStreamModel, name::SinkWriterName, option::Options,
    
};

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(crate) struct SinkWriter {
    name: SinkWriterName,
    server_type: SinkWriterType,
    from_foreign_stream: Arc<ForeignStreamModel>,
    options: Options,
}

impl SinkWriter {
    pub(crate) fn new(
        server_type: SinkWriterType,
        from_foreign_stream: Arc<ForeignStreamModel>,
        options: Options,
    ) -> Self {
        Self {
            name: SinkWriterName::from(&server_type),
            server_type,
            from_foreign_stream,
            options,
        }
    }

    pub(crate) fn name(&self) -> &SinkWriterName {
        &self.name
    }

    pub(crate) fn server_type(&self) -> &SinkWriterType {
        &self.server_type
    }

    pub(crate) fn from_foreign_stream(&self) -> Arc<ForeignStreamModel> {
        self.from_foreign_stream.clone()
    }

    pub(crate) fn options(&self) -> &Options {
        &self.options
    }
}
