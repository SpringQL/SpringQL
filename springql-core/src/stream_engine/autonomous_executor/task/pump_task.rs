// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod pump_subtask;

use super::task_context::TaskContext;
use crate::error::Result;
use crate::pipeline::pipeline_graph::PipelineGraph;
use crate::pipeline::pump_model::PumpModel;
use crate::stream_engine::autonomous_executor::performance_metrics::metrics_update_command::metrics_update_by_task_execution::{MetricsUpdateByTaskExecution, TaskMetricsUpdateByTaskExecution, InQueueMetricsUpdateByTaskExecution, OutQueueMetricsUpdateByTaskExecution};
use crate::stream_engine::autonomous_executor::task_graph::task_id::TaskId;
use crate::stream_engine::time::duration::wall_clock_duration::wall_clock_stopwatch::WallClockStopwatch;
use pump_subtask::insert_subtask::InsertSubtask;
use pump_subtask::query_subtask::QuerySubtask;

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

        let task_metrics = TaskMetricsUpdateByTaskExecution::new(context.task(), execution_time);
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
        Option<InQueueMetricsUpdateByTaskExecution>,
        Vec<OutQueueMetricsUpdateByTaskExecution>,
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
            Ok((None, vec![]))
        }
    }
}
