//! Flow-Efficient Scheduler, intended to minimize the size of working memory.
//!
//! # Basic idea
//!
//! 1. Collect a row from a "row generator" (source task or window task).
//! 2. Repeatedly pass the row to downstream tasks until "flow stopper" (sink or window task).
//! 3. Goto 1.
//!
//! # Deeper dive
//!
//! To minimize the number of alive rows in memory, Flow-Efficient Scheduler has the following rules.
//!
//! - **Rule1: one-by-one**
//!
//!   A task inputs only 1 row at a time without making "mini-batch" of rows. This rule
//!   eliminates requisition to extra buffer and decrease latency between source to sink,
//!   while it may badly affects throughput.
//!
//! - **Rule2: a collector to stoppers**
//!
//!   ![Generators, collectors, and stoppers in Flow-Efficient Scheduler](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/flow-efficient-scheduler.svg)
//!
//!   Flow-Efficient Scheduler produces series of tasks from collector (downstream task of a generator) to reachable flow stoppers.
//!   If path from a collector to flow stoppers have split, tasks are traversed in DFS order.
//!   A worker then executes the series consecutively in order not to remain intermediate rows.
//!
//! - **Rule3: fair**
//!
//! Each collector has fair chance to be scheduled. Fairness is defined as such here:
//!
//! Each row created by the generator waits, on average, the same number of times until it is collected by the collector.

use std::{cell::RefCell, collections::HashSet};

use rand::{
    distributions::{Uniform, WeightedIndex},
    prelude::{Distribution, ThreadRng},
};

use crate::stream_engine::autonomous_executor::{
    performance_metrics::PerformanceMetrics,
    task_graph::{task_id::TaskId, TaskGraph},
};

use super::Scheduler;

/// A generator task is one of:
///
/// - source tasks
/// - window tasks
///
/// Source tasks and window tasks are not simple stream tasks but generator of rows.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct Generator {
    task_id: TaskId,
}

/// Collector tasks are downstream tasks of a generator.
///
/// Rows' start flowing from collectors.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct Collector {
    task_id: TaskId,
}

impl Generator {
    fn collectors(&self, graph: &TaskGraph) -> HashSet<Collector> {
        graph
            .downstream_tasks(&self.task_id)
            .into_iter()
            .map(|collector_task_id| Collector {
                task_id: collector_task_id,
            })
            .collect()
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
struct Stopper {
    task_id: TaskId,
}

#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct FlowEfficientScheduler {
    rng: RefCell<ThreadRng>,
}

impl Scheduler for FlowEfficientScheduler {
    fn next_task_series(&self, graph: &TaskGraph, metrics: &PerformanceMetrics) -> Vec<TaskId> {
        self.decide_collector(graph, metrics)
            .map(|collector| self.collector_to_stoppers_dfs(&collector, graph))
            .unwrap_or_else(Vec::new)
    }
}

impl FlowEfficientScheduler {
    /// A collector task is one of:
    ///
    /// - downstream tasks source tasks
    /// - downstream tasks of window tasks
    ///
    /// Source tasks and window tasks are not simple stream tasks but generator of rows.
    /// Rows' flow start from downstream of generator tasks.
    ///
    /// This function picks a collector probabilistically to meet the **Rule3: fair**.
    ///
    /// # Returns
    ///
    /// `None` if no collector task exists in `graph`.
    fn decide_collector(
        &self,
        graph: &TaskGraph,
        metrics: &PerformanceMetrics,
    ) -> Option<Collector> {
        let collectors = self.collectors(graph).into_iter().collect::<Vec<_>>();
        if collectors.is_empty() {
            None
        } else {
            let in_rows = collectors
                .iter()
                .map(|c| self.incoming_rows(c, graph, metrics))
                .collect::<Vec<_>>();

            let idx = if in_rows.iter().all(|nr| *nr == 0) {
                // No collector has input row. Pick a collector randomly.
                let distribution = Uniform::from(0..collectors.len());
                distribution.sample(&mut *self.rng.borrow_mut())
            } else {
                let distribution = WeightedIndex::new(in_rows).expect("at least 1 collector");
                distribution.sample(&mut *self.rng.borrow_mut())
            };
            let picked_collector = collectors.get(idx).expect("safe index");

            log::trace!(
                "[FlowEfficientScheduler::decide_collector()] {}",
                picked_collector.task_id,
            );
            Some(picked_collector.clone())
        }
    }
    fn incoming_rows(
        &self,
        collector: &Collector,
        graph: &TaskGraph,
        metrics: &PerformanceMetrics,
    ) -> i64 {
        graph
            .input_queues(&collector.task_id)
            .iter()
            .map(|q| metrics.rows_for_task_input(q))
            // FIXME: sum is not completely fair if the collector has multiple input queues
            .sum()
    }

    fn collector_to_stoppers_dfs(&self, collector: &Collector, graph: &TaskGraph) -> Vec<TaskId> {
        fn to_stoppers_dfs(current_task: &TaskId, graph: &TaskGraph) -> Vec<TaskId> {
            if current_task.is_window_task() {
                // window task is a stopper
                vec![current_task.clone()]
            } else {
                let mut downstream_path = graph.downstream_tasks(current_task).iter().fold(
                    vec![],
                    |mut head, next_task| {
                        let mut tail = to_stoppers_dfs(next_task, graph);
                        head.append(&mut tail);
                        head
                    },
                );

                let mut me = vec![current_task.clone()];
                me.append(&mut downstream_path);
                me
            }
        }
        to_stoppers_dfs(&collector.task_id, graph)
    }

    fn generators(&self, graph: &TaskGraph) -> HashSet<Generator> {
        graph
            .source_tasks()
            .iter()
            .cloned()
            .chain(graph.window_tasks())
            .map(|task_id| Generator { task_id })
            .collect()
    }
    fn collectors(&self, graph: &TaskGraph) -> HashSet<Collector> {
        // TODO cache
        self.generators(graph)
            .iter()
            .flat_map(|generator| generator.collectors(graph))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use springql_test_logger::setup_test_logger;

    use super::*;

    #[ignore]
    #[test]
    fn test_flow_efficient_scheduler() {
        setup_test_logger();

        let sched = FlowEfficientScheduler::default();
        let series = sched.next_task_series(
            &TaskGraph::fx_split_join(),
            &PerformanceMetrics::fx_split_join(),
        );
        log::info!(
            "[FlowEfficientScheduler] {}",
            series
                .iter()
                .map(|task_id| format!("{}", task_id))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}
