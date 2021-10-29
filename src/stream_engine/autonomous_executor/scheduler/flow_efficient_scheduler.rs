//! Flow-Efficient Scheduler, intended to minimize the size of working memory.
//!
//! # Basic idea
//!
//! 1. Collect a foreign row from a source.
//! 2. Repeatedly pass the row to downstream streams until sink without collecting another row from the source.
//! 3. Goto 1.
//!
//! # Deeper dive
//!
//! ## Scheduling model
//!
//! A pipeline is a DAG where nodes are (foreign or native) streams and edges are pumps or (source | sink) servers.
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
//!   - `[1]` and `[2]` are source foreign stream.
//!   - `[8]` and `[9]` are sink foreign stream.
//!   - `[3]` - `[7]` are native stream.
//! - `(0)`: A virtual root stream, which is introduced to make nodes traversal algorithm simpler.
//! - `a` and `b`: Source servers.
//! - `l` and `m`: Sink servers.
//! - `c` - `k`: Pumps.
//!
//! A scheduler regards edges (`a` - `m`) as tasks and executes them in some order.
//! Each task may have dependent tasks because a task need input row given from upstream. Single exception is source server, which can generate row.
//! The core concept of scheduling is to order all the tasks in a pipeline so that dependency is resolved.
//! Since task dependency is mapped to node dependency in pipeline DAG, topological sort to pipeline produces a valid task schedule.
//!
//! ## Flow-Efficient Scheduler
//!
//! To minimize the number of alive rows in memory, Flow-Efficient Scheduler has the following rules.
//!
//! - **Rule1: one-by-one**
//!
//!   A worker does not make buffering. In other words, a worker hols 0 or 1 input row for a task (pump, source server, or sink server) at a time.
//!
//!   Example: A worker request only 1 row from `a`.
//!
//! - **Rule2 :complete sequence**
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

use crate::stream_engine::autonomous_executor::task::Task;

use super::Scheduler;

pub(crate) struct FlowEfficientScheduler;

impl Scheduler for FlowEfficientScheduler {
    fn new(pipeline: PipelineRead) -> Self {
        todo!()
    }

    fn next_task(&mut self) -> Option<Task> {
        todo!()
    }
}
