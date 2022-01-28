// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::sync::Mutex;

use crate::pipeline::pump_model::window_operation_parameter::WindowOperationParameter;
use crate::pipeline::pump_model::window_parameter::WindowParameter;
use crate::stream_engine::autonomous_executor::task::tuple::Tuple;
use crate::stream_engine::autonomous_executor::task::window::Window;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct GroupAggregateWindowSubtask(Mutex<Window>);

impl GroupAggregateWindowSubtask {
    pub(in crate::stream_engine::autonomous_executor) fn new(
        window_param: WindowParameter,
        op_param: WindowOperationParameter,
    ) -> Self {
        let window = Window::new(window_param, op_param);
        Self(Mutex::new(window))
    }

    pub(in crate::stream_engine::autonomous_executor) fn run(&self, tuple: Tuple) -> Vec<Tuple> {
        self.0
            .lock()
            .expect("another thread accessing to window gets poisoned")
            .dispatch(tuple)
    }
}
