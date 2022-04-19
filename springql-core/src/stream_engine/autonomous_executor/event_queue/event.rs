// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

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
    Blocking(BlockingEventTag),
    NonBlocking(NonBlockingEventTag),
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum BlockingEventTag {
    UpdatePipeline,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum NonBlockingEventTag {
    ReplacePerformanceMetrics,
    IncrementalUpdateMetrics,
    ReportMetricsSummary,
    TransitMemoryState,
}

impl From<&Event> for EventTag {
    fn from(event: &Event) -> Self {
        match event {
            Event::UpdatePipeline { .. } => Self::Blocking(BlockingEventTag::UpdatePipeline),
            Event::ReplacePerformanceMetrics { .. } => {
                Self::NonBlocking(NonBlockingEventTag::ReplacePerformanceMetrics)
            }
            Event::IncrementalUpdateMetrics { .. } => {
                Self::NonBlocking(NonBlockingEventTag::IncrementalUpdateMetrics)
            }
            Event::ReportMetricsSummary { .. } => {
                Self::NonBlocking(NonBlockingEventTag::ReportMetricsSummary)
            }
            Event::TransitMemoryState { .. } => {
                Self::NonBlocking(NonBlockingEventTag::TransitMemoryState)
            }
        }
    }
}
