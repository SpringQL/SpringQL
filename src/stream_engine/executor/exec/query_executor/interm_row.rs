use std::rc::Rc;

use crate::stream_engine::executor::data::row::Row;

/// Intermediate row appearing only in a QueryPlan.
///
/// This row is an output from a QueryPlan's operation, which means the operation does not change the input row.
#[derive(Clone, PartialEq, Debug, new)]
pub(super) struct PreservedRow(Rc<Row>);

/// Intermediate output row appearing only in a QueryPlan.
///
/// In order to realize zero-copy stream, whether or not a QueryPlan can preserve child node's row is important (if possible, a row is not copied but just transferred from input to output stream).
#[derive(PartialEq, Debug)]
pub(super) enum IntermOutputRow {
    Preserved(PreservedRow),
    NewlyCreated(Row),
}

impl AsRef<Row> for PreservedRow {
    fn as_ref(&self) -> &Row {
        &self.0
    }
}

impl AsRef<Row> for IntermOutputRow {
    fn as_ref(&self) -> &Row {
        match self {
            IntermOutputRow::Preserved(row) => row.as_ref(),
            IntermOutputRow::NewlyCreated(row) => row,
        }
    }
}
