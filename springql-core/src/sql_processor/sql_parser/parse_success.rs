use crate::{
    pipeline::name::PumpName,
    stream_engine::command::{insert_plan::InsertPlan, Command},
};

use super::pest_parser_impl::syntax::SelectStreamSyntax;

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) enum ParseSuccess {
    CreatePump {
        pump_name: PumpName,
        select_stream_syntax: SelectStreamSyntax,
        insert_plan: InsertPlan,
    },
    CommandWithoutQuery(Command),
}
