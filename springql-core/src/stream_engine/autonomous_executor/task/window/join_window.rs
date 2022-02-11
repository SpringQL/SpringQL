use crate::pipeline::pump_model::{
    window_operation_parameter::WindowOperationParameter, window_parameter::WindowParameter,
};

use super::{
    panes::{pane::join_pane::JoinPane, Panes},
    watermark::Watermark,
    Window,
};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct JoinWindow {
    watermark: Watermark,
    panes: Panes<JoinPane>,
}

impl Window for JoinWindow {
    type Pane = JoinPane;

    fn watermark(&self) -> &Watermark {
        &self.watermark
    }

    fn watermark_mut(&mut self) -> &mut Watermark {
        &mut self.watermark
    }

    fn panes(&self) -> &Panes<Self::Pane> {
        &self.panes
    }

    fn panes_mut(&mut self) -> &mut Panes<Self::Pane> {
        &mut self.panes
    }
}

impl JoinWindow {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        window_param: WindowParameter,
        op_param: WindowOperationParameter,
    ) -> Self {
        let watermark = Watermark::new(window_param.allowed_delay());
        Self {
            watermark,
            panes: Panes::new(window_param, op_param),
        }
    }
}
