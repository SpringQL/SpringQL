// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::{Mutex, MutexGuard};

use crate::{
    expr_resolver::ExprResolver,
    pipeline::{JoinParameter, WindowParameter},
    stream_engine::autonomous_executor::{
        performance_metrics::WindowInFlowByWindowTask,
        task::{
            tuple::Tuple,
            window::{JoinDir, JoinWindow, Window},
        },
    },
};

#[derive(Debug)]
pub struct JoinSubtask(Mutex<JoinWindow>);

impl JoinSubtask {
    pub fn new(window_param: WindowParameter, join_param: JoinParameter) -> Self {
        let window = JoinWindow::new(window_param, join_param);
        Self(Mutex::new(window))
    }

    pub fn run(
        &self,
        expr_resolver: &ExprResolver,
        tuple: Tuple,
        dir: JoinDir,
    ) -> (Vec<Tuple>, WindowInFlowByWindowTask) {
        self.0
            .lock()
            .expect("another thread accessing to window gets poisoned")
            .dispatch(expr_resolver, tuple, dir)
    }

    pub fn get_window_mut(&self) -> MutexGuard<JoinWindow> {
        self.0
            .lock()
            .expect("another thread accessing to window gets poisoned")
    }
}
