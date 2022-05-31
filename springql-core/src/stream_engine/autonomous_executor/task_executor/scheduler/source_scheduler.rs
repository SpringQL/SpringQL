// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Source Scheduler dedicating to schedule source tasks eagerly at Moderate and Severe state.

use std::collections::HashSet;

use crate::stream_engine::autonomous_executor::{
    performance_metrics::PerformanceMetrics,
    task_executor::scheduler::Scheduler,
    task_graph::{task_id::TaskId, TaskGraph},
};

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct SourceTask {
    task_id: TaskId,
}

#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct SourceScheduler {}

impl Scheduler for SourceScheduler {
    /// TODO [prioritize source with lower source-miss rate](https://gh01.base.toyota-tokyo.tech/SpringQL-internal/SpringQL/issues/122)
    fn next_task_series(&self, graph: &TaskGraph, _metrics: &PerformanceMetrics) -> Vec<TaskId> {
        self.source_tasks(graph)
            .into_iter()
            .map(|s| s.task_id)
            .collect()
    }
}

impl SourceScheduler {
    fn source_tasks(&self, graph: &TaskGraph) -> HashSet<SourceTask> {
        graph
            .source_tasks()
            .into_iter()
            .map(|task_id| SourceTask { task_id })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use springql_test_logger::setup_test_logger;

    use super::*;

    #[ignore]
    #[test]
    fn test_source_scheduler() {
        setup_test_logger();

        let sched = SourceScheduler::default();
        let series = sched.next_task_series(
            &TaskGraph::fx_split_join(),
            &PerformanceMetrics::fx_split_join(),
        );
        log::error!(
            "[SourceScheduler] {}",
            series
                .iter()
                .map(|task_id| format!("{}", task_id))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}
