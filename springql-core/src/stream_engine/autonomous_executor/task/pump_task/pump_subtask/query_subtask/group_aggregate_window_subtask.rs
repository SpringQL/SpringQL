// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::{Mutex, MutexGuard};

use crate::{
    expr_resolver::ExprResolver,
    pipeline::{WindowOperationParameter, WindowParameter},
    stream_engine::autonomous_executor::{
        performance_metrics::WindowInFlowByWindowTask,
        task::{
            tuple::Tuple,
            window::{AggrWindow, AggregatedAndGroupingValues, Window},
        },
    },
};

#[derive(Debug)]
pub struct GroupAggregateWindowSubtask(Mutex<AggrWindow>);

impl GroupAggregateWindowSubtask {
    pub fn new(window_param: WindowParameter, op_param: WindowOperationParameter) -> Self {
        let window = AggrWindow::new(window_param, op_param);
        Self(Mutex::new(window))
    }

    pub fn run(
        &self,
        expr_resolver: &ExprResolver,
        tuple: Tuple,
    ) -> (Vec<AggregatedAndGroupingValues>, WindowInFlowByWindowTask) {
        self.0
            .lock()
            .expect("another thread accessing to window gets poisoned")
            .dispatch(expr_resolver, tuple, ())
            .expect("dispatch failed")
    }

    pub fn get_window_mut(&self) -> MutexGuard<AggrWindow> {
        self.0
            .lock()
            .expect("another thread accessing to window gets poisoned")
    }
}
