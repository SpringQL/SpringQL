// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::pipeline::{
    foreign_stream_model::ForeignStreamModel, name::StreamName, stream_model::StreamModel,
};

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum StreamNode {
    Native(Arc<StreamModel>),
    Foreign(Arc<ForeignStreamModel>),
    VirtualRoot,
    VirtualLeaf { parent_foreign_stream: StreamName },
}

impl StreamNode {
    pub(crate) fn name(&self) -> StreamName {
        match self {
            StreamNode::Native(stream) => stream.name().clone(),
            StreamNode::Foreign(stream) => stream.name().clone(),
            StreamNode::VirtualRoot => StreamName::virtual_root(),
            StreamNode::VirtualLeaf {
                parent_foreign_stream,
            } => StreamName::virtual_leaf(parent_foreign_stream.clone()),
        }
    }
}
