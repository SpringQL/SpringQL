use std::sync::mpsc;

use crate::stream_engine::autonomous_executor::performance_metrics::{
    performance_metrics_summary::PerformanceMetricsSummary, PerformanceMetrics,
};

/// Reports performance summary to [AutonomousExecutor](crate::stream_processor::autonomous_executor::AutonomousExecutor).
#[derive(Debug, new)]
pub(super) struct AutonomousExecutorReporter {
    autonomous_executor: mpsc::Sender<PerformanceMetricsSummary>,
}

impl AutonomousExecutorReporter {
    pub(super) fn report(&self, metrics: &PerformanceMetrics) {
        let summary = PerformanceMetricsSummary::from(metrics);
        self.autonomous_executor
            .send(summary)
            .expect("Receiver in AutonomousExecutor must not be deallocated here");
    }
}
