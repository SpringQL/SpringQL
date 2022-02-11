use crate::{
    expr_resolver::ExprResolver,
    pipeline::pump_model::window_operation_parameter::WindowOperationParameter,
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::WindowInFlowByWindowTask,
            task::window::watermark::Watermark,
        },
        time::timestamp::Timestamp,
        Tuple,
    },
};

pub(in crate::stream_engine::autonomous_executor) mod aggregate_pane;
pub(in crate::stream_engine::autonomous_executor) mod join_pane;

pub(in crate::stream_engine::autonomous_executor) trait Pane {
    type CloseOut;

    fn new(open_at: Timestamp, close_at: Timestamp, param: WindowOperationParameter) -> Self;

    fn open_at(&self) -> Timestamp;
    fn close_at(&self) -> Timestamp;

    fn is_acceptable(&self, rowtime: &Timestamp) -> bool {
        &self.open_at() <= rowtime && rowtime < &self.close_at()
    }

    fn should_close(&self, watermark: &Watermark) -> bool {
        self.close_at() <= watermark.as_timestamp()
    }

    fn dispatch(&mut self, expr_resolver: &ExprResolver, tuple: &Tuple)
        -> WindowInFlowByWindowTask;

    fn close(self, expr_resolver: &ExprResolver)
        -> (Vec<Self::CloseOut>, WindowInFlowByWindowTask);
}
