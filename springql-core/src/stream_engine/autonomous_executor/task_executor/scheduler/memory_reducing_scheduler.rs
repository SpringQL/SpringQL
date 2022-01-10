//! Memory-Reducing Scheduler, intended to reducing working memory in task queues at the cost of fairness.
//!
//! It schedules tasks to minimize loss function below:
//!
//! ```text
//! L(task) = avg_gain_bytes_per_sec(task) (input_rows(task) > 0)
//! L(task) = infinite (input_rows(task) == 0)
//! ```
//!
//! Large negative `avg_gain_bytes_per_sec(task)` means that this task will quickly reduce working memory in task graph.
//!
//! Tasks with `L(task) > 0`, which increase working memory by execution, have chance to be scheduled because
//! they may lead to large memory reducing tasks. See example below:
//!
//! ```text
//! [source task]
//!  |
//!  v
//! [task1] (avg_gain_bytes_per_sec = 100)
//!  |
//!  v
//! [task2] (avg_gain_bytes_per_sec = -1000)
//! ```
//!
//! Task1 should be scheduled for task2 to get input rows.
//!
//! Unlike Flow-Efficient Scheduler, Memory-Reducing Scheduler does not have fairness.
//! Some rows may get large delay until they get to sink, or even lose chance to participate in time-based window.

use std::{cmp::min, iter};

use crate::stream_engine::autonomous_executor::{
    performance_metrics::PerformanceMetrics,
    task_graph::{task_id::TaskId, TaskGraph},
};

use super::{Scheduler, MAX_TASK_SERIES};

#[derive(Clone, PartialEq, Debug)]
struct TaskProfile {
    task_id: TaskId,
    loss: f32,
    input_rows: u64,
}

#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct MemoryReducingScheduler;

impl Scheduler for MemoryReducingScheduler {
    fn next_task_series(&self, graph: &TaskGraph, metrics: &PerformanceMetrics) -> Vec<TaskId> {
        let profiles = self.task_profiles_ordered_by_loss(graph, metrics);

        let mut series = vec![];
        for profile in profiles {
            if profile.input_rows == 0 {
                // profile.loss must be infinite and remaining profiles must also have no input rows
                break;
            } else {
                let rest_len = MAX_TASK_SERIES - series.len() as u16;
                let n_tasks = min(profile.input_rows, rest_len as u64);
                let mut tail = iter::repeat(profile.task_id)
                    .take(n_tasks as usize)
                    .collect::<Vec<_>>();
                series.append(&mut tail);
            }
        }
        series
    }
}

impl MemoryReducingScheduler {
    fn task_profiles_ordered_by_loss(
        &self,
        graph: &TaskGraph,
        metrics: &PerformanceMetrics,
    ) -> Vec<TaskProfile> {
        let tasks = graph.tasks();
        let mut profiles = tasks
            .iter()
            .map(|task| self.task_profile(task, graph, metrics))
            .collect::<Vec<_>>();
        profiles.sort_by(|a, b| a.loss.partial_cmp(&b.loss).expect("loss cannot be NaN"));
        profiles
    }

    fn task_profile(
        &self,
        task: &TaskId,
        graph: &TaskGraph,
        metrics: &PerformanceMetrics,
    ) -> TaskProfile {
        let input_rows = self.incoming_rows(task, graph, metrics);
        let loss = if input_rows == 0 {
            f32::MAX
        } else {
            metrics.avg_gain_bytes_per_sec(task)
        };
        TaskProfile {
            task_id: task.clone(),
            loss,
            input_rows,
        }
    }

    fn incoming_rows(&self, task: &TaskId, graph: &TaskGraph, metrics: &PerformanceMetrics) -> u64 {
        graph
            .input_queues(task)
            .iter()
            .map(|q| metrics.rows_for_task_input(q))
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use springql_test_logger::setup_test_logger;

    use super::*;

    #[ignore]
    #[test]
    fn test_memory_reducing_scheduler() {
        setup_test_logger();

        let sched = MemoryReducingScheduler::default();
        let series = sched.next_task_series(
            &TaskGraph::fx_split_join(),
            &PerformanceMetrics::fx_split_join(),
        );
        log::debug!(
            "[MemoryReducingScheduler] {}",
            series
                .iter()
                .map(|task_id| format!("{}", task_id))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}
