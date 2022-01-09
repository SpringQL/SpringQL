//! # Events diagram
//!
//! ![Communication between entities](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/stream-engine-architecture-communication.svg)

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecution,
        PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
};

#[derive(Clone, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum Event {
    UpdatePipeline {
        pipeline_derivatives: Arc<PipelineDerivatives>,
    },
    UpdatePerformanceMetrics {
        metrics: Arc<PerformanceMetrics>,
    },
    IncrementalUpdateMetrics {
        metrics_update_by_task_execution: Arc<MetricsUpdateByTaskExecution>,
    },
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum EventTag {
    UpdatePipeline,
    UpdatePerformanceMetrics,
    IncrementalUpdateMetrics,
}

impl From<&Event> for EventTag {
    fn from(event: &Event) -> Self {
        match event {
            Event::UpdatePipeline { .. } => Self::UpdatePipeline,
            Event::UpdatePerformanceMetrics { .. } => Self::UpdatePerformanceMetrics,
            Event::IncrementalUpdateMetrics { .. } => Self::IncrementalUpdateMetrics,
        }
    }
}
