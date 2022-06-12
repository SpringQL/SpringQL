// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use crate::pipeline::{name::StreamName, stream_model::StreamModel};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum StreamNode {
    Stream(Arc<StreamModel>),
    VirtualRoot,
    VirtualLeaf { parent_sink_stream: StreamName },
}
