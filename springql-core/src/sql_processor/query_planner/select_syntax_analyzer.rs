use crate::sql_processor::sql_parser::syntax::SelectStreamSyntax;

mod field;
mod from_item;
mod group_aggregate;
mod window;

use crate::error::Result;
use crate::stream_engine::command::query_plan::aliaser::Aliaser;

#[derive(Clone, Debug, new)]
pub(in crate::sql_processor) struct SelectSyntaxAnalyzer {
    select_syntax: SelectStreamSyntax,
}

impl SelectSyntaxAnalyzer {
    pub(super) fn aliaser(&self) -> Result<Aliaser> {
        let afns = self.aliased_field_names_in_projection()?;
        Ok(Aliaser::from(afns))
    }
}
