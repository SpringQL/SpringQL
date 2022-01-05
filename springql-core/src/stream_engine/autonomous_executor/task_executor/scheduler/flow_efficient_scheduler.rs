// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Flow-Efficient Scheduler, intended to minimize the size of working memory.
//!
//! # Basic idea
//!
//! 1. Collect a row from a source.
//! 2. Repeatedly pass the row to downstream streams until sink without collecting another row from the source.
//! 3. Goto 1.
//!
//! # Deeper dive
//!
//! ## Scheduling model
//!
//! A pipeline is a DAG where nodes are (source, sink, or native) streams and edges are pumps, source readers, or sink writers.
//!
//! A node has 1 or more incoming edges and 1 or more outgoing edges.
//!
//! ```text
//! (0)--a-->[1]--c-->[3]--f-->[4]--g-->[5]--h-->[6]--j-->[8]--l-->
//!  |                          ^       ^ |
//!  |                          |       | |
//!  +---b-->[2]-------d--------+       | +--i-->[7]--k-->[9]--m-->
//!           |                         |
//!           +--------------e----------+
//! ```
//!
//! This is a sample pipeline DAG.
//!
//! - `[1]` - `[9]`: A stream.
//!   - `[1]` and `[2]` are source stream.
//!   - `[8]` and `[9]` are sink stream.
//!   - `[3]` - `[7]` are native stream.
//! - `(0)`: A virtual root stream, which is introduced to make nodes traversal algorithm simpler.
//! - `a` and `b`: Source readers.
//! - `l` and `m`: Sink writers.
//! - `c` - `k`: Pumps.
//!
//! A scheduler regards edges (`a` - `m`) as tasks and executes them in some order.
//! Each task may have dependent tasks because a task need input row given from upstream. Single exception is source reader, which can generate row.
//! The core concept of scheduling is to order all the tasks in a pipeline so that dependency is resolved.
//! Since task dependency is mapped to node dependency in pipeline DAG, topological sort to pipeline produces a valid task schedule.
//!
//! ## Flow-Efficient Scheduler
//!
//! To minimize the number of alive rows in memory, Flow-Efficient Scheduler has the following rules.
//!
//! - **Rule1: one-by-one**
//!
//!   A worker does not make buffering. In other words, a worker hols 0 or 1 input row for a task (pump, source reader, or sink writer) at a time.
//!
//!   Example: A worker request only 1 row from `a`.
//!
//! - **Rule2: complete sequence**
//!
//!   A worker completes sequential task group (edges between a multi-out stream and multi-in stream) without jumping to another sequential task group.
//!
//!   Example: Sequential task groups: `acf`, `b(d|e)`, `g`, `hjl`, `lkm`. Worker must execute `f` after `c`.
//!
//! - **Rule3: request other dependencies**
//!
//!   For a stream having multiple incoming edges, a task mapped to an incoming edge request other tasks mapped to the remaining incoming edges to complete first.
//!
//!   Example: Request `d` after `f` has finished.
//!
//! - **Rule4: flow joined immediately**
//!
//!   For a stream having multiple incoming edges, a worker executes a task mapped to one of outgoing edges of the stream soon after all tasks mapped to the incoming edges finished.
//!
//!   Example: Execute `g` after `d` if `f` is already finished.
//!
//!   Note that Rule4 includes Rule2.
//!
//! Therefore, Flow-Efficient Scheduler produces either of these schedules:
//!
//! 1. `acfbdgehjlikm`
//!
//!   (imm **`a`** or `b`) `[acf]` (req `d`) `[bd]` (imm `g`) [g] (req `e`) `[e]` (imm **`h`** or `i`) `[hjl][ikm]`
//! 2. `acfbdgeikmhjl`
//!
//!   (imm **`a`** or `b`) `[acf]` (req `d`) `[bd]` (imm `g`) [g] (req `e`) `[e]` (imm `h` or **`i`**) `[ikm][hjl]`
//! 3. `bdacfgehjlikm`
//!
//!   (imm `a` or **`b`**) `[b]` (imm **`d`** or `e`) [d] (req `f`) `[acf]` (imm `g`) `[g]` (req `e`) `[e]` (imm **`h`** or `i`) `[hjl][ikm]`
//! 4. `bdacfgeikmhjl`
//!
//!   (imm `a` or **`b`**) `[b]` (imm **`d`** or `e`) [d] (req `f`) `[acf]` (imm `g`) `[g]` (req `e`) `[e]` (imm **`h`** or `i`) `[hjl][ikm]`
//! 5. `beacfdghjlikm`
//!   (imm `a` or **`b`**) `[b]` (imm `d` or **`e`**) [e] (req `g`) (req **`f`** and `d`) `[acf]` (req `d`) `[d]` (imm `g`) `[g]` (imm **`h`** or `i`) `[hjl][ikm]`
//! 6. `beacfdgikmhjl`
//!
//!   (imm `a` or **`b`**) `[b]` (imm `d` or **`e`**) [e] (req `g`) (req **`f`** and `d`) `[acf]` (req `d`) `[d]` (imm `g`) `[g]` (imm `h` or **`i`**) `[ikm][hjl]`
//! 7. `bedacfghjlikm`
//!
//!   (imm `a` or **`b`**) `[b]` (imm `d` or **`e`**) [e] (req `g`) (req `f` and **`d`**) `[d]` (req `f`) `[acf]` (imm `g`) `[g]` (imm **`h`** or `i`) `[hjl][ikm]`
//! 8. `bedacfgikmhjl`
//!
//!   (imm `a` or **`b`**) `[b]` (imm `d` or **`e`**) [e] (req `g`) (req `f` and **`d`**) `[d]` (req `f`) `[acf]` (imm `g`) `[g]` (imm `h` or **`i`**) `[ikm][hjl]`
//!
//! Selecting 1 from these schedule intelligently should lead to more memory reduction but current implementation always select first one (eagerly select leftmost outgoing edge).

use crate::{
    error::Result,
    pipeline::{name::StreamName, pipeline_version::PipelineVersion},
    stream_engine::autonomous_executor::task_graph::TaskGraph,
};
use petgraph::{
    graph::{DiGraph, NodeIndex},
    visit::EdgeRef,
};
use std::{collections::HashSet, sync::Arc};

use crate::stream_engine::autonomous_executor::task::{task_id::TaskId, Task};

use super::{Scheduler, WorkerState};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub(crate) struct CurrentTaskIdx {
    pipeline_version: PipelineVersion,
    idx: usize,
}
impl WorkerState for CurrentTaskIdx {}

#[derive(Debug, Default)]
pub(crate) struct FlowEfficientScheduler {
    pipeline_version: PipelineVersion,
    seq_task_schedule: Vec<Arc<Task>>,
}

impl Scheduler for FlowEfficientScheduler {
    type W = CurrentTaskIdx;

    fn _notify_pipeline_version(&mut self, v: PipelineVersion) {
        self.pipeline_version = v;
    }

    fn next_task(&self, worker_state: CurrentTaskIdx) -> Option<(Arc<Task>, CurrentTaskIdx)> {
        if self.seq_task_schedule.is_empty() {
            None
        } else if worker_state.pipeline_version != self.pipeline_version {
            let current_task = self
                .seq_task_schedule
                .get(0)
                .expect("index is managed in this function")
                .clone();
            let next_worker_state = CurrentTaskIdx {
                pipeline_version: self.pipeline_version,
                idx: 1 % self.seq_task_schedule.len(),
            };
            Some((current_task, next_worker_state))
        } else {
            let current_task_idx = worker_state.idx;
            let current_task = self
                .seq_task_schedule
                .get(current_task_idx)
                .expect("index is managed in this function")
                .clone();
            let next_worker_state = CurrentTaskIdx {
                idx: (worker_state.idx + 1) % self.seq_task_schedule.len(),
                ..worker_state
            };
            Some((current_task, next_worker_state))
        }
    }

    /// # Failure
    ///
    /// - [SpringError::ForeignIo](crate::error::SpringError::ForeignIo) when:
    ///   - failed to start a source reader.
    fn _notify_task_graph_update(&mut self, task_graph: &TaskGraph) -> Result<()> {
        let graph = task_graph.as_petgraph();

        let mut unvisited = graph
            .edge_weights()
            .into_iter()
            .map(|task| task.id())
            .collect::<HashSet<TaskId>>();

        let mut seq_task_schedule = Vec::<Arc<Task>>::new();

        for leaf_node in Self::_leaf_nodes(graph) {
            Self::_leaf_to_root_dfs_post_order(
                leaf_node,
                graph,
                &mut seq_task_schedule,
                &mut unvisited,
            )?;
        }

        self.seq_task_schedule = seq_task_schedule;

        log::debug!(
            "[FlowEfficientScheduler] new schedule [{}]; from task graph {:?}",
            self.seq_task_schedule
                .iter()
                .map(|task| task.id().to_string())
                .collect::<Vec<_>>()
                .join(", "),
            task_graph
        );

        Ok(())
    }
}

impl FlowEfficientScheduler {
    fn _leaf_nodes(graph: &DiGraph<StreamName, Arc<Task>>) -> Vec<NodeIndex> {
        graph
            .node_indices()
            .filter(|idx| graph.neighbors(*idx).next().is_none())
            .collect()
    }

    /// Push tasks to seq_task_schedule in DFS post-order.
    fn _leaf_to_root_dfs_post_order(
        cur_node: NodeIndex,
        graph: &DiGraph<StreamName, Arc<Task>>,
        seq_task_schedule: &mut Vec<Arc<Task>>,
        unvisited: &mut HashSet<TaskId>,
    ) -> Result<()> {
        let incoming_edges = graph.edges_directed(cur_node, petgraph::Direction::Incoming);
        for incoming_edge in incoming_edges {
            let task = incoming_edge.weight();

            if !unvisited.remove(&task.id()) {
                // already visited
                break;
            } else {
                let parent_node = incoming_edge.source();
                Self::_leaf_to_root_dfs_post_order(
                    parent_node,
                    graph,
                    seq_task_schedule,
                    unvisited,
                )?;
                seq_task_schedule.push(task.clone());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use springql_foreign_service::{
        sink::ForeignSink,
        source::{source_input::ForeignSourceInput, ForeignSource},
    };

    use crate::{
        pipeline::{name::PumpName, Pipeline},
        stream_engine::autonomous_executor::{
            current_pipeline::CurrentPipeline, task::task_id::TaskId,
        },
    };

    use super::*;

    fn t(pipeline: Pipeline, expected: Vec<TaskId>) {
        let mut expected = expected.into_iter().collect::<VecDeque<_>>();

        let mut cur_task_idx = CurrentTaskIdx::default();

        let mut scheduler = FlowEfficientScheduler::default();

        let current_pipeline = CurrentPipeline::new(pipeline);
        scheduler.notify_pipeline_update(&current_pipeline).unwrap();

        if let Some((first_task, next_task_idx)) = scheduler.next_task(cur_task_idx) {
            assert_eq!(first_task.id(), expected.pop_front().unwrap());
            cur_task_idx = next_task_idx;

            loop {
                let (next_task, next_task_idx) = scheduler
                    .next_task(cur_task_idx)
                    .expect("task must be infinitely provided");
                if next_task == first_task {
                    return;
                }

                let expected_next = expected.pop_front().unwrap();
                assert_eq!(next_task.id(), expected_next);

                cur_task_idx = next_task_idx;
            }
        } else {
            assert!(expected.is_empty())
        }
    }

    #[test]
    fn test_source_only_pipeline() {
        t(Pipeline::fx_source_only(), vec![])
    }

    #[test]
    fn test_linear_pipeline() {
        let test_source = ForeignSource::start(ForeignSourceInput::new_fifo_batch(vec![])).unwrap();
        let test_sink = ForeignSink::start().unwrap();
        t(
            Pipeline::fx_linear(
                test_source.host_ip(),
                test_source.port(),
                test_sink.host_ip(),
                test_sink.port(),
            ),
            vec![
                TaskId::from_source_reader(StreamName::factory("st_1")),
                TaskId::from_pump(PumpName::factory("pu_b")),
                TaskId::from_sink_writer(StreamName::factory("st_2")),
            ],
        )
    }

    #[test]
    fn test_pipeline_with_split() {
        let test_source = ForeignSource::start(ForeignSourceInput::new_fifo_batch(vec![])).unwrap();
        let test_sink1 = ForeignSink::start().unwrap();
        let test_sink2 = ForeignSink::start().unwrap();

        t(
            Pipeline::fx_split(
                test_source.host_ip(),
                test_source.port(),
                test_sink1.host_ip(),
                test_sink1.port(),
                test_sink2.host_ip(),
                test_sink2.port(),
            ),
            vec![
                TaskId::from_source_reader(StreamName::factory("st_1")),
                TaskId::from_pump(PumpName::factory("pu_c")),
                TaskId::from_sink_writer(StreamName::factory("st_3")),
                TaskId::from_source_reader(StreamName::factory("st_2")),
                TaskId::from_pump(PumpName::factory("pu_d")),
                TaskId::from_sink_writer(StreamName::factory("st_4")),
            ],
        )
    }

    #[test]
    fn test_pipeline_with_merge() {
        let test_source = ForeignSource::start(ForeignSourceInput::new_fifo_batch(vec![])).unwrap();
        let test_sink = ForeignSink::start().unwrap();
        t(
            Pipeline::fx_split_merge(
                test_source.host_ip(),
                test_source.port(),
                test_sink.host_ip(),
                test_sink.port(),
            ),
            vec![
                TaskId::from_source_reader(StreamName::factory("st_2")),
                TaskId::from_pump(PumpName::factory("pu_d")),
                TaskId::from_source_reader(StreamName::factory("st_1")),
                TaskId::from_pump(PumpName::factory("pu_c")),
                TaskId::from_sink_writer(StreamName::factory("st_3")),
            ],
        )
    }

    #[test]
    fn test_complex_pipeline() {
        let test_source = ForeignSource::start(ForeignSourceInput::new_fifo_batch(vec![])).unwrap();
        let test_sink1 = ForeignSink::start().unwrap();
        let test_sink2 = ForeignSink::start().unwrap();
        t(
            Pipeline::fx_complex(
                test_source.host_ip(),
                test_source.port(),
                test_sink1.host_ip(),
                test_sink1.port(),
                test_sink2.host_ip(),
                test_sink2.port(),
            ),
            vec![
                TaskId::from_source_reader(StreamName::factory("st_1")),
                TaskId::from_pump(PumpName::factory("pu_c")),
                TaskId::from_pump(PumpName::factory("pu_f")),
                TaskId::from_source_reader(StreamName::factory("st_2")),
                TaskId::from_pump(PumpName::factory("pu_d")),
                TaskId::from_pump(PumpName::factory("pu_g")),
                TaskId::from_pump(PumpName::factory("pu_e")),
                TaskId::from_pump(PumpName::factory("pu_h")),
                TaskId::from_pump(PumpName::factory("pu_j")),
                TaskId::from_sink_writer(StreamName::factory("st_8")),
                TaskId::from_pump(PumpName::factory("pu_i")),
                TaskId::from_pump(PumpName::factory("pu_k")),
                TaskId::from_sink_writer(StreamName::factory("st_9")),
            ],
        )
    }
}
