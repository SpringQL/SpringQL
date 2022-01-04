// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::pipeline::{
    name::StreamName, sink_stream_model::SinkStreamModel, source_stream_model::SourceStreamModel,
    stream_model::StreamModel,
};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum StreamNode {
    Native(Arc<StreamModel>),
    Source(Arc<SourceStreamModel>),
    Sink(Arc<SinkStreamModel>),
    VirtualRoot,
    VirtualLeaf { parent_sink_stream: StreamName },
}

impl StreamNode {
    pub(crate) fn name(&self) -> StreamName {
        match self {
            StreamNode::Native(stream) => stream.name().clone(),
            StreamNode::Source(stream) => stream.name().clone(),
            StreamNode::Sink(stream) => stream.name().clone(),
            StreamNode::VirtualRoot => StreamName::virtual_root(),
            StreamNode::VirtualLeaf { parent_sink_stream } => {
                StreamName::virtual_leaf(parent_sink_stream.clone())
            }
        }
    }
}
