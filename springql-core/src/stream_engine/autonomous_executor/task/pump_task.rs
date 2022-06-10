// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod pump_subtask;

use std::sync::MutexGuard;
use std::thread;
use std::time::Duration;

use crate::{
    api::error::Result,
    pipeline::{PipelineGraph, PumpModel},
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::{
                InQueueMetricsUpdateByTask, MetricsUpdateByTaskExecution,
                OutQueueMetricsUpdateByTask, TaskMetricsUpdateByTask,
            },
            task::{
                pump_task::pump_subtask::{
                    insert_subtask::InsertSubtask, query_subtask::QuerySubtask,
                },
                task_context::TaskContext,
                window::{aggregate::AggrWindow, join_window::JoinWindow},
            },
            task_graph::task_id::TaskId,
        },
        time::duration::wall_clock_duration::wall_clock_stopwatch::WallClockStopwatch,
    },
};

/// Source tasks may require I/O but pump tasks are not.
/// Pump tasks should yield CPU time when it cannot get input data, expecting source tasks get enough CPU time to feed source data.
const WAIT_ON_NO_INPUT: Duration = Duration::from_micros(100);

#[derive(Debug)]
pub(crate) struct PumpTask {
    id: TaskId,
    query_subtask: QuerySubtask,
    insert_subtask: InsertSubtask,
}

impl PumpTask {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        pump: &PumpModel,
        pipeline_graph: &PipelineGraph,
    ) -> Self {
        let id = TaskId::from_pump(pump);
        let query_subtask = QuerySubtask::new(pump.query_plan().clone());
        let insert_subtask = InsertSubtask::new(pump.insert_plan(), pipeline_graph);
        Self {
            id,
            query_subtask,
            insert_subtask,
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        context: &TaskContext,
    ) -> Result<MetricsUpdateByTaskExecution> {
        let stopwatch = WallClockStopwatch::start();
        let (in_queue_metrics, out_queues_metrics) = self.run_query_insert(context)?;
        let execution_time = stopwatch.stop();

        let task_metrics = TaskMetricsUpdateByTask::new(context.task(), execution_time);
        let metrics = MetricsUpdateByTaskExecution::new(
            task_metrics,
            in_queue_metrics.map_or_else(Vec::new, |m| vec![m]),
            out_queues_metrics,
        );

        Ok(metrics)
    }

    fn run_query_insert(
        &self,
        context: &TaskContext,
    ) -> Result<(
        Option<InQueueMetricsUpdateByTask>,
        Vec<OutQueueMetricsUpdateByTask>,
    )> {
        if let Some(query_subtask_out) = self.query_subtask.run(context)? {
            let insert_subtask_out = self
                .insert_subtask
                .run(query_subtask_out.values_seq, context);
            Ok((
                Some(query_subtask_out.in_queue_metrics_update),
                insert_subtask_out.out_queues_metrics_update,
            ))
        } else {
            thread::sleep(WAIT_ON_NO_INPUT);
            Ok((None, vec![]))
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn get_aggr_window_mut(
        &self,
    ) -> Option<MutexGuard<AggrWindow>> {
        self.query_subtask.get_aggr_window_mut()
    }
    pub(in crate::stream_engine::autonomous_executor) fn get_join_window_mut(
        &self,
    ) -> Option<MutexGuard<JoinWindow>> {
        self.query_subtask.get_join_window_mut()
    }
}
