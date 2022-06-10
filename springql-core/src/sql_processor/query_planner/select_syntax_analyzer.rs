// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod field;
mod from_item;
mod group_aggregate;
mod window;

use crate::sql_processor::sql_parser::SelectStreamSyntax;

#[derive(Clone, Debug, new)]
pub struct SelectSyntaxAnalyzer {
    select_syntax: SelectStreamSyntax,
}
