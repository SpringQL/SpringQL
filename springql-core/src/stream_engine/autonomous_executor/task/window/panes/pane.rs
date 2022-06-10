// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub mod aggregate_pane;
pub mod join_pane;

use crate::{
    expr_resolver::ExprResolver,
    pipeline::WindowOperationParameter,
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::WindowInFlowByWindowTask,
            task::window::watermark::Watermark,
        },
        time::timestamp::SpringTimestamp,
        Tuple,
    },
};

pub trait Pane {
    type CloseOut;
    type DispatchArg: Clone;

    fn new(
        open_at: SpringTimestamp,
        close_at: SpringTimestamp,
        param: WindowOperationParameter,
    ) -> Self;

    fn open_at(&self) -> SpringTimestamp;
    fn close_at(&self) -> SpringTimestamp;

    fn is_acceptable(&self, rowtime: &SpringTimestamp) -> bool {
        &self.open_at() <= rowtime && rowtime < &self.close_at()
    }

    fn should_close(&self, watermark: &Watermark) -> bool {
        self.close_at() <= watermark.as_timestamp()
    }

    fn dispatch(
        &mut self,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
        arg: Self::DispatchArg,
    ) -> WindowInFlowByWindowTask;

    fn close(self, expr_resolver: &ExprResolver)
        -> (Vec<Self::CloseOut>, WindowInFlowByWindowTask);
}
