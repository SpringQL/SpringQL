// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::str::FromStr;

use crate::{
    expr_resolver::ExprResolver,
    mem_size::MemSize,
    pipeline::{Field, JoinParameter, JoinType, WindowOperationParameter},
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::WindowInFlowByWindowTask,
            task::{tuple::Tuple, window::panes::pane::Pane},
        },
        time::timestamp::SpringTimestamp,
        SqlValue,
    },
};

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum JoinDir {
    Left,
    Right,
}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct JoinPane {
    open_at: SpringTimestamp,
    close_at: SpringTimestamp,

    join_parameter: JoinParameter,

    left_tuples: Vec<Tuple>,
    right_tuples: Vec<Tuple>,
}

impl Pane for JoinPane {
    type CloseOut = Tuple;
    type DispatchArg = JoinDir;

    /// # Panics
    ///
    /// if `op_param` is not `JoinParameter`
    fn new(
        open_at: SpringTimestamp,
        close_at: SpringTimestamp,
        op_param: WindowOperationParameter,
    ) -> Self {
        let join_parameter = if let WindowOperationParameter::Join(p) = op_param {
            p
        } else {
            panic!("op_param {:?} is not JoinParameter", op_param)
        };

        Self {
            open_at,
            close_at,
            left_tuples: Vec::new(),
            right_tuples: Vec::new(),
            join_parameter,
        }
    }

    fn open_at(&self) -> SpringTimestamp {
        self.open_at
    }

    fn close_at(&self) -> SpringTimestamp {
        self.close_at
    }

    /// Dispatch to left_tuples
    fn dispatch(
        &mut self,
        _expr_resolver: &ExprResolver,
        tuple: &Tuple,
        dir: JoinDir,
    ) -> WindowInFlowByWindowTask {
        match dir {
            JoinDir::Left => self.left_tuples.push(tuple.clone()),
            JoinDir::Right => self.right_tuples.push(tuple.clone()),
        }
        WindowInFlowByWindowTask::new(0, tuple.mem_size() as i64)
    }

    fn close(
        self,
        expr_resolver: &ExprResolver,
    ) -> (Vec<Self::CloseOut>, WindowInFlowByWindowTask) {
        match self.join_parameter.join_type {
            JoinType::LeftOuter => self.left_outer_join(expr_resolver),
        }
    }
}

impl JoinPane {
    fn left_outer_join(
        self,
        expr_resolver: &ExprResolver,
    ) -> (Vec<Tuple>, WindowInFlowByWindowTask) {
        let window_in_flow = self.calc_window_in_flow_on_close();

        let null_right = self.null_right_tuple();

        // using Nested Loop Join.
        let mut res_tuples = Vec::new();
        for left_tuple in self.left_tuples {
            let mut joined_to_the_left = vec![];

            for right_tuple in &self.right_tuples {
                // TODO less clone. ExprResolver takes two tuples to resolve ColumnReference?
                let joined_tuple = left_tuple.clone().join(right_tuple.clone());

                let on_bool = expr_resolver
                    .eval_value_expr(self.join_parameter.on_expr, &joined_tuple)
                    .expect("TODO Result")
                    .to_bool()
                    .expect("TODO Result");

                if on_bool {
                    joined_to_the_left.push(joined_tuple);
                }
            }

            if joined_to_the_left.is_empty() {
                let joined_tuple = left_tuple.join(null_right.clone());
                joined_to_the_left.push(joined_tuple);
            }

            res_tuples.extend(joined_to_the_left);
        }

        (res_tuples, window_in_flow)
    }

    fn calc_window_in_flow_on_close(&self) -> WindowInFlowByWindowTask {
        let left_size = self.left_tuples.iter().map(|t| t.mem_size()).sum::<usize>();
        let right_size = self
            .right_tuples
            .iter()
            .map(|t| t.mem_size())
            .sum::<usize>();

        WindowInFlowByWindowTask::new(0, -((left_size + right_size) as i64))
    }

    fn null_right_tuple(&self) -> Tuple {
        // unused
        let rowtime = SpringTimestamp::from_str("1970-01-01 00:00:00.0000000000").unwrap();

        let fields = self
            .join_parameter
            .right_colrefs
            .iter()
            .map(|colref| Field::new(colref.clone(), SqlValue::Null))
            .collect();

        Tuple::new(rowtime, fields)
    }
}
