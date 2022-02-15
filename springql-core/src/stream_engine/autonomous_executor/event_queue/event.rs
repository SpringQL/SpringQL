//! # Events diagram
//!
//! ![Communication between entities](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/stream-engine-architecture-communication.svg)

use std::sync::Arc;

use crate::stream_engine::autonomous_executor::{
    memory_state_machine::MemoryStateTransition,
    performance_metrics::{
        metrics_update_command::metrics_update_by_task_execution::MetricsUpdateByTaskExecutionOrPurge,
        performance_metrics_summary::PerformanceMetricsSummary, PerformanceMetrics,
    },
    pipeline_derivatives::PipelineDerivatives,
};

#[derive(Clone, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum Event {
    UpdatePipeline {
        pipeline_derivatives: Arc<PipelineDerivatives>,
    },
    ReplacePerformanceMetrics {
        metrics: Arc<PerformanceMetrics>,
    },
    IncrementalUpdateMetrics {
        metrics_update_by_task_execution_or_purge: Arc<MetricsUpdateByTaskExecutionOrPurge>,
    },
    ReportMetricsSummary {
        metrics_summary: Arc<PerformanceMetricsSummary>,
    },
    TransitMemoryState {
        memory_state_transition: Arc<MemoryStateTransition>,
    },
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum EventTag {
    UpdatePipeline,
    ReplacePerformanceMetrics,
    IncrementalUpdateMetrics,
    ReportMetricsSummary,
    TransitMemoryState,
}

impl From<&Event> for EventTag {
    fn from(event: &Event) -> Self {
        match event {
            Event::UpdatePipeline { .. } => Self::UpdatePipeline,
            Event::ReplacePerformanceMetrics { .. } => Self::ReplacePerformanceMetrics,
            Event::IncrementalUpdateMetrics { .. } => Self::IncrementalUpdateMetrics,
            Event::ReportMetricsSummary { .. } => Self::ReportMetricsSummary,
            Event::TransitMemoryState { .. } => Self::TransitMemoryState,
        }
    }
}
