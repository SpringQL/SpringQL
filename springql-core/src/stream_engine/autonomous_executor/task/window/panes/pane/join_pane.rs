use std::{collections::HashMap, str::FromStr, sync::Arc};

use crate::{
    expr_resolver::ExprResolver,
    mem_size::MemSize,
    pipeline::{
        field::Field,
        pump_model::window_operation_parameter::{
            join_parameter::{JoinParameter, JoinType},
            WindowOperationParameter,
        },
    },
    stream_engine::{
        autonomous_executor::{
            performance_metrics::metrics_update_command::metrics_update_by_task_execution::WindowInFlowByWindowTask,
            task::{tuple::Tuple, window::aggregate::GroupAggrOut},
        },
        time::timestamp::Timestamp,
        NnSqlValue, SqlValue,
    },
};

use super::Pane;

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct JoinPane {
    open_at: Timestamp,
    close_at: Timestamp,

    join_parameter: JoinParameter,

    left_tuples: Vec<Tuple>,
    right_tuples: Vec<Tuple>,
}

impl Pane for JoinPane {
    type CloseOut = Tuple;

    /// # Panics
    ///
    /// if `op_param` is not `JoinParameter`
    fn new(open_at: Timestamp, close_at: Timestamp, op_param: WindowOperationParameter) -> Self {
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

    fn open_at(&self) -> Timestamp {
        self.open_at
    }

    fn close_at(&self) -> Timestamp {
        self.close_at
    }

    /// Dispatch to left_tuples
    fn dispatch(
        &mut self,
        expr_resolver: &ExprResolver,
        tuple: &Tuple,
    ) -> WindowInFlowByWindowTask {
        self.left_tuples.push(tuple.clone()); // TODO not use trait

        WindowInFlowByWindowTask::new(tuple.mem_size() as i64, 1)
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

            for right_tuple in self.right_tuples.clone() {
                // TODO less clone. ExprResolver takes two tuples to resolve ColumnReference?
                let joined_tuple = left_tuple.clone().join(right_tuple);

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
        let left_rows = self.left_tuples.len();
        let right_rows = self.right_tuples.len();

        let left_size = self.left_tuples.iter().map(|t| t.mem_size()).sum::<usize>();
        let right_size = self
            .right_tuples
            .iter()
            .map(|t| t.mem_size())
            .sum::<usize>();

        WindowInFlowByWindowTask::new(
            -((left_size + right_size) as i64),
            -((left_rows + right_rows) as i64),
        )
    }

    fn null_right_tuple(&self) -> Tuple {
        // unused
        let rowtime = Timestamp::from_str("1970-01-01 00:00:00.0000000000").unwrap();

        let fields = self
            .join_parameter
            .right_colrefs
            .iter()
            .map(|colref| Field::new(colref.clone(), SqlValue::Null))
            .collect();

        Tuple::new(rowtime, fields)
    }
}
