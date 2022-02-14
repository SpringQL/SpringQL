use super::SelectSyntaxAnalyzer;
use crate::{
    sql_processor::sql_parser::syntax::{FromItemSyntax, SubFromItemSyntax},
    stream_engine::command::query_plan::query_plan_operation::{CollectOp, JoinOp},
};

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn join_op(&self) -> JoinOp {
        match self.select_syntax.from_item.clone() {
            FromItemSyntax::StreamVariant(sub_from_item) => {
                let collect_op = Self::sub_from_item_to_collect_op(sub_from_item);
                JoinOp::Collect(collect_op)
            }
            FromItemSyntax::JoinVariant {
                left,
                right,
                join_type,
                on_expr,
            } => {
                let left = Self::sub_from_item_to_collect_op(left);

                let right = match right.as_ref() {
                    FromItemSyntax::StreamVariant(sub_from_item) => {
                        Self::sub_from_item_to_collect_op(sub_from_item.clone())
                    }
                    FromItemSyntax::JoinVariant { .. } => unimplemented!("recursive join"),
                };

                JoinOp::Join {
                    left,
                    right,
                    join_type,
                    on_expr,
                }
            }
        }
    }

    fn sub_from_item_to_collect_op(sub_from_item: SubFromItemSyntax) -> CollectOp {
        CollectOp {
            stream: sub_from_item.stream_name,
        }
    }
}
