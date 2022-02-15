use super::SelectSyntaxAnalyzer;
use crate::{
    error::Result,
    expr_resolver::ExprResolver,
    pipeline::{pump_model::window_operation_parameter::join_parameter::JoinParameter, Pipeline},
    sql_processor::sql_parser::syntax::{FromItemSyntax, SubFromItemSyntax},
    stream_engine::command::query_plan::query_plan_operation::{CollectOp, JoinOp, JoinWindowOp},
};

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn join_op(
        &self,
        expr_resolver: &mut ExprResolver,
        pipeline: &Pipeline,
    ) -> Result<JoinOp> {
        match self.select_syntax.from_item.clone() {
            FromItemSyntax::StreamVariant(sub_from_item) => {
                let collect_op = Self::sub_from_item_to_collect_op(sub_from_item);
                Ok(JoinOp::Collect(collect_op))
            }
            FromItemSyntax::JoinVariant {
                left: left_sub,
                right,
                join_type,
                on_expr,
            } => {
                let right_sub = match right.as_ref() {
                    FromItemSyntax::StreamVariant(sub_from_item) => sub_from_item,
                    FromItemSyntax::JoinVariant { .. } => unimplemented!("recursive join"),
                };

                let left_collect_op = Self::sub_from_item_to_collect_op(left_sub.clone());
                let right_collect_op = Self::sub_from_item_to_collect_op(right_sub.clone());

                let left_colrefs = pipeline
                    .get_stream(&left_sub.stream_name)?
                    .column_references();
                let right_colrefs = pipeline
                    .get_stream(&right_sub.stream_name)?
                    .column_references();

                let on_expr_label = expr_resolver.register_value_expr(on_expr);

                let join_param =
                    JoinParameter::new(join_type, left_colrefs, right_colrefs, on_expr_label);

                let window_param = self
                    .window_parameter()
                    .expect("JOIN must take window clause");

                Ok(JoinOp::JoinWindow(JoinWindowOp {
                    left: left_collect_op,
                    right: right_collect_op,
                    window_param,
                    join_param,
                }))
            }
        }
    }

    fn sub_from_item_to_collect_op(sub_from_item: SubFromItemSyntax) -> CollectOp {
        CollectOp {
            stream: sub_from_item.stream_name,
        }
    }
}
