use crate::{expr_resolver::ExprResolver, stream_engine::{Tuple, autonomous_executor::performance_metrics::metrics_update_command::metrics_update_by_task_execution::WindowInFlowByWindowTask}};

use self::{
    panes::{pane::Pane, Panes},
    watermark::Watermark,
};

pub(in crate::stream_engine::autonomous_executor) mod aggregate;
pub(in crate::stream_engine::autonomous_executor) mod join_window;
pub(in crate::stream_engine::autonomous_executor) mod panes;

mod watermark;

pub(in crate::stream_engine::autonomous_executor) trait Window {
    type Pane: Pane;

    fn watermark(&self) -> &Watermark;
    fn watermark_mut(&mut self) -> &mut Watermark;

    fn panes(&self) -> &Panes<Self::Pane>;
    fn panes_mut(&mut self) -> &mut Panes<Self::Pane>;

    fn purge(&mut self);

    /// A task dispatches a tuple from waiting queue.
    fn dispatch(
        &mut self,
        expr_resolver: &ExprResolver,
        tuple: Tuple,
        arg: <<Self as Window>::Pane as Pane>::DispatchArg,
    ) -> (
        Vec<<<Self as Window>::Pane as Pane>::CloseOut>,
        WindowInFlowByWindowTask,
    ) {
        let rowtime = *tuple.rowtime();

        if rowtime < self.watermark().as_timestamp() {
            // too late tuple does not have any chance to be dispatched nor to close a pane.
            (Vec::new(), WindowInFlowByWindowTask::zero())
        } else {
            self.watermark_mut().update(rowtime);
            let wm = *self.watermark();

            let window_in_flow_dispatch = self
                .panes_mut()
                .panes_to_dispatch(rowtime)
                .map(|pane| pane.dispatch(expr_resolver, &tuple, arg.clone()))
                .fold(WindowInFlowByWindowTask::zero(), |acc, window_in_flow| {
                    acc + window_in_flow
                });

            let (out, window_in_flow_close) = self
                .panes_mut()
                .remove_panes_to_close(&wm)
                .into_iter()
                .fold(
                    (Vec::new(), WindowInFlowByWindowTask::zero()),
                    |(mut out_acc, window_in_flow_acc), pane| {
                        let (mut out_seq, window_in_flow) = pane.close(expr_resolver);
                        out_acc.append(&mut out_seq);
                        (out_acc, window_in_flow_acc + window_in_flow)
                    },
                );

            (out, window_in_flow_dispatch + window_in_flow_close)
        }
    }
}
