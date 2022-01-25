use super::SelectSyntaxAnalyzer;
use crate::error::Result;
use crate::pipeline::correlation::aliased_correlation_name::AliasedCorrelationName;
use crate::pipeline::name::CorrelationName;
use crate::sql_processor::sql_parser::syntax::FromItemSyntax;

impl SelectSyntaxAnalyzer {
    pub(in super::super) fn from_item_correlations(&self) -> Result<Vec<AliasedCorrelationName>> {
        let from_item = self.from_item();
        Self::from_item_into_correlation_references(from_item)
    }
    fn from_item(&self) -> &FromItemSyntax {
        &self.select_syntax.from_item
    }
    fn from_item_into_correlation_references(
        from_item: &FromItemSyntax,
    ) -> Result<Vec<AliasedCorrelationName>> {
        match from_item {
            FromItemSyntax::StreamVariant { stream_name, alias } => {
                let correlation_name = CorrelationName::new(stream_name.to_string());
                let aliased_correlation_name =
                    AliasedCorrelationName::new(correlation_name, alias.clone());
                Ok(vec![aliased_correlation_name])
            }
        }
    }
}
