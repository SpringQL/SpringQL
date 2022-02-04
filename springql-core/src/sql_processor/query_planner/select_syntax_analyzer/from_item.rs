use super::SelectSyntaxAnalyzer;
use crate::error::Result;
use crate::expression::ValueExprPh1;
use crate::pipeline::name::StreamName;
use crate::sql_processor::sql_parser::syntax::FromItemSyntax;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn from_item_expressions(&self) -> Vec<ValueExprPh1> {
        vec![]
    }

    pub(in super::super) fn from_item_streams(&self) -> Result<Vec<StreamName>> {
        let from_item = self.from_item();
        Self::from_item_into_stream_names(from_item)
    }
    fn from_item(&self) -> &FromItemSyntax {
        &self.select_syntax.from_item
    }
    fn from_item_into_stream_names(from_item: &FromItemSyntax) -> Result<Vec<StreamName>> {
        match from_item {
            FromItemSyntax::StreamVariant { stream_name, .. } => Ok(vec![stream_name.clone()]),
        }
    }
}
