use std::ops::Add;

use crate::stream_engine::{
    autonomous_executor::task_graph::{
        queue_id::{row_queue_id::RowQueueId, window_queue_id::WindowQueueId, QueueId},
        task_id::TaskId,
    },
    time::duration::{wall_clock_duration::WallClockDuration, SpringDuration},
};

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) enum MetricsUpdateByTaskExecutionOrPurge {
    TaskExecution(MetricsUpdateByTaskExecution),
    Purge,
}

/// Metrics update per task execution.
///
/// Commands only include flow metrics (no stock metrics).
#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct MetricsUpdateByTaskExecution {
    task: TaskMetricsUpdateByTask,
    in_queues: Vec<InQueueMetricsUpdateByTask>,
    out_queues: Vec<OutQueueMetricsUpdateByTask>,
}

impl MetricsUpdateByTaskExecution {
    pub(in crate::stream_engine::autonomous_executor) fn updated_task(&self) -> &TaskId {
        &self.task.task_id
    }
    pub(in crate::stream_engine::autonomous_executor) fn updated_queues(&self) -> Vec<QueueId> {
        self.in_queues
            .iter()
            .map(|q| q.queue_id())
            .chain(self.out_queues.iter().map(|q| q.queue_id.clone()))
            .collect()
    }

    /// Memory gain speed of this row task.
    ///
    /// Note that this is the expected gain on the task execution (not a normal throughput).
    /// Memory-Reducing Schedulers may prioritize tasks with lower result value of this function
    /// because such tasks quickly reduces memory consumption.
    pub(in crate::stream_engine::autonomous_executor) fn task_gain_bytes_per_sec(&self) -> f32 {
        self.task_gain_bytes() as f32 / self.task_execution_time().as_secs_f32()
    }

    pub(in crate::stream_engine::autonomous_executor) fn row_queue_gain_rows(
        &self,
        id: &RowQueueId,
    ) -> i64 {
        self.queue_put_rows(&id.clone().into()) as i64 - self.row_queue_used_rows(id) as i64
    }
    pub(in crate::stream_engine::autonomous_executor) fn row_queue_gain_bytes(
        &self,
        id: &RowQueueId,
    ) -> i64 {
        self.queue_put_bytes(&id.clone().into()) as i64 - self.row_queue_used_bytes(id) as i64
    }

    pub(in crate::stream_engine::autonomous_executor) fn window_queue_waiting_gain_rows(
        &self,
        id: &WindowQueueId,
    ) -> i64 {
        self.queue_put_rows(&id.clone().into()) as i64
            - self.window_queue_waiting_dispatched_rows(id) as i64
    }
    pub(in crate::stream_engine::autonomous_executor) fn window_queue_gain_bytes(
        &self,
        id: &WindowQueueId,
    ) -> i64 {
        self.window_queue_waiting_gain_bytes(id) + self.window_queue_window_gain_bytes(id)
    }

    fn task_execution_time(&self) -> WallClockDuration {
        self.task.execution_time
    }

    fn task_gain_bytes(&self) -> i64 {
        self.task_put_bytes() as i64
            - self.row_task_used_bytes() as i64
            - self.window_task_dispatched_bytes() as i64
            + self.window_task_window_gain_bytes()
    }
    fn task_put_bytes(&self) -> u64 {
        self.out_queues
            .iter()
            .fold(0, |acc, out_q| acc + out_q.bytes_put)
    }
    fn row_task_used_bytes(&self) -> u64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row { bytes_used, .. } => Some(bytes_used),
                InQueueMetricsUpdateByCollect::Window { .. } => None,
            })
            .sum()
    }
    fn window_task_dispatched_bytes(&self) -> u64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row { .. } => None,
                InQueueMetricsUpdateByCollect::Window {
                    waiting_bytes_dispatched,
                    ..
                } => Some(waiting_bytes_dispatched),
            })
            .sum()
    }
    fn window_task_window_gain_bytes(&self) -> i64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row { .. } => None,
                InQueueMetricsUpdateByCollect::Window { .. } => Some(
                    in_q.window_in_flow.window_gain_bytes_states
                        + in_q.window_in_flow.window_gain_bytes_rows,
                ),
            })
            .sum()
    }

    fn queue_put_rows(&self, id: &QueueId) -> u64 {
        self.out_queues
            .iter()
            .filter(|out_q| &out_q.queue_id == id)
            .fold(0, |acc, out_q| acc + out_q.rows_put)
    }
    fn queue_put_bytes(&self, id: &QueueId) -> u64 {
        self.out_queues
            .iter()
            .filter(|out_q| &out_q.queue_id == id)
            .fold(0, |acc, out_q| acc + out_q.bytes_put)
    }
    fn row_queue_used_rows(&self, id: &RowQueueId) -> u64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match &in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row {
                    queue_id,
                    rows_used,
                    ..
                } => (queue_id == id).then(|| rows_used),
                InQueueMetricsUpdateByCollect::Window { .. } => None,
            })
            .sum()
    }
    fn row_queue_used_bytes(&self, id: &RowQueueId) -> u64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match &in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row {
                    queue_id,
                    bytes_used,
                    ..
                } => (queue_id == id).then(|| bytes_used),
                InQueueMetricsUpdateByCollect::Window { .. } => None,
            })
            .sum()
    }
    fn window_queue_waiting_dispatched_rows(&self, id: &WindowQueueId) -> u64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match &in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row { .. } => None,
                InQueueMetricsUpdateByCollect::Window {
                    queue_id,
                    waiting_rows_dispatched,
                    ..
                } => (queue_id == id).then(|| waiting_rows_dispatched),
            })
            .sum()
    }
    fn window_queue_waiting_dispatched_bytes(&self, id: &WindowQueueId) -> u64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match &in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row { .. } => None,
                InQueueMetricsUpdateByCollect::Window {
                    queue_id,
                    waiting_rows_dispatched: waiting_bytes_dispatched,
                    ..
                } => (queue_id == id).then(|| waiting_bytes_dispatched),
            })
            .sum()
    }
    fn window_queue_waiting_gain_bytes(&self, id: &WindowQueueId) -> i64 {
        self.queue_put_bytes(&id.clone().into()) as i64
            - self.window_queue_waiting_dispatched_bytes(id) as i64
    }
    fn window_queue_window_gain_bytes(&self, id: &WindowQueueId) -> i64 {
        self.window_queue_window_states_gain_bytes(id)
            + self.window_queue_window_rows_gain_bytes(id)
    }
    fn window_queue_window_states_gain_bytes(&self, id: &WindowQueueId) -> i64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match &in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row { .. } => None,
                InQueueMetricsUpdateByCollect::Window { queue_id, .. } => {
                    (queue_id == id).then(|| in_q.window_in_flow.window_gain_bytes_states)
                }
            })
            .sum()
    }
    fn window_queue_window_rows_gain_bytes(&self, id: &WindowQueueId) -> i64 {
        self.in_queues
            .iter()
            .filter_map(|in_q| match &in_q.by_collect {
                InQueueMetricsUpdateByCollect::Row { .. } => None,
                InQueueMetricsUpdateByCollect::Window { queue_id, .. } => {
                    (queue_id == id).then(|| in_q.window_in_flow.window_gain_bytes_rows)
                }
            })
            .sum()
    }
}

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct TaskMetricsUpdateByTask {
    task_id: TaskId,
    execution_time: WallClockDuration,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::stream_engine::autonomous_executor) struct InQueueMetricsUpdateByTask {
    pub(in crate::stream_engine::autonomous_executor) by_collect: InQueueMetricsUpdateByCollect,

    /// WindowInFlowByWindowTask::zero() for window task
    pub(in crate::stream_engine::autonomous_executor) window_in_flow: WindowInFlowByWindowTask,
}

impl InQueueMetricsUpdateByTask {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        by_collect: InQueueMetricsUpdateByCollect,
        window_in_flow: Option<WindowInFlowByWindowTask>,
    ) -> Self {
        Self {
            by_collect,
            window_in_flow: window_in_flow.unwrap_or_else(WindowInFlowByWindowTask::zero),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn queue_id(&self) -> QueueId {
        self.by_collect.queue_id()
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum InQueueMetricsUpdateByCollect {
    Row {
        queue_id: RowQueueId,
        rows_used: u64,
        bytes_used: u64,
    },
    Window {
        queue_id: WindowQueueId,
        waiting_bytes_dispatched: u64,
        waiting_rows_dispatched: u64,
    },
}

impl InQueueMetricsUpdateByCollect {
    pub(in crate::stream_engine::autonomous_executor) fn queue_id(&self) -> QueueId {
        match self {
            Self::Row { queue_id, .. } => queue_id.clone().into(),
            Self::Window { queue_id, .. } => queue_id.clone().into(),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct WindowInFlowByWindowTask {
    window_gain_bytes_states: i64,
    window_gain_bytes_rows: i64,
}
impl WindowInFlowByWindowTask {
    pub(in crate::stream_engine::autonomous_executor) fn zero() -> Self {
        Self {
            window_gain_bytes_states: 0,
            window_gain_bytes_rows: 0,
        }
    }
}

impl Add for WindowInFlowByWindowTask {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            window_gain_bytes_states: self.window_gain_bytes_states + rhs.window_gain_bytes_states,
            window_gain_bytes_rows: self.window_gain_bytes_rows + rhs.window_gain_bytes_rows,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct OutQueueMetricsUpdateByTask {
    queue_id: QueueId,
    rows_put: u64,
    bytes_put: u64,
}
