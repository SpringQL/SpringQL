//! # Events diagram
//!
//! ![Communication between entities](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/stream-engine-architecture-communication.svg)

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::pipeline_derivatives::PipelineDerivatives;

#[derive(Clone, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum Event {
    UpdatePipeline {
        pipeline_derivatives: Arc<PipelineDerivatives>,
    },
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum EventTag {
    UpdatePipeline,
}

impl From<&Event> for EventTag {
    fn from(event: &Event) -> Self {
        match event {
            Event::UpdatePipeline { .. } => EventTag::UpdatePipeline,
        }
    }
}
