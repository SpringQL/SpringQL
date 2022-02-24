// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::sql_processor::sql_parser::syntax::SelectStreamSyntax;

mod field;
mod from_item;
mod group_aggregate;
mod window;

#[derive(Clone, Debug, new)]
pub(in crate::sql_processor) struct SelectSyntaxAnalyzer {
    select_syntax: SelectStreamSyntax,
}
