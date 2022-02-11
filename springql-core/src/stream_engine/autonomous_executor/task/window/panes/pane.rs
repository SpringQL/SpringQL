use crate::{
    expr_resolver::ExprResolver,
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

pub(in super::super) trait Pane {
    type CloseOut;

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

    fn close(self) -> (Self::CloseOut, WindowInFlowByWindowTask);
}
