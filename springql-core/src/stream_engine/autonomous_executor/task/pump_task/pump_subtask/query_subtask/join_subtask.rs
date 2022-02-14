// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Mutex;

use crate::expr_resolver::ExprResolver;
use crate::pipeline::pump_model::window_operation_parameter::WindowOperationParameter;
use crate::pipeline::pump_model::window_parameter::WindowParameter;
use crate::stream_engine::autonomous_executor::performance_metrics::metrics_update_command::metrics_update_by_task_execution::WindowInFlowByWindowTask;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;
use crate::stream_engine::autonomous_executor::task::window::Window;
use crate::stream_engine::autonomous_executor::task::window::join_window::JoinWindow;
use crate::stream_engine::autonomous_executor::task::window::panes::pane::join_pane::JoinDir;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct JoinSubtask(Mutex<JoinWindow>);

impl JoinSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        window_param: WindowParameter,
        op_param: WindowOperationParameter,
    ) -> Self {
        let window = JoinWindow::new(window_param, op_param);
        Self(Mutex::new(window))
    }

    pub(in crate::stream_engine::autonomous_executor) fn run(
        &self,
        tuple: Tuple,
        dir: JoinDir,
    ) -> (Vec<Tuple>, WindowInFlowByWindowTask) {
        self.0
            .lock()
            .expect("another thread accessing to window gets poisoned")
            .dispatch(expr_resolver, tuple, dir)
    }
}
