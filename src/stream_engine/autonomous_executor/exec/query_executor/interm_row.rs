use std::sync::Arc;

use crate::stream_engine::autonomous_executor::data::row::Row;

/// Intermediate row appearing only in a QueryPlan.
///
/// This row is an output from a QueryPlan's operation and is **not changed** by the operation.
#[derive(Clone, PartialEq, Debug, new)]
pub(super) struct PreservedRow(Arc<Row>);

impl AsRef<Row> for PreservedRow {
    fn as_ref(&self) -> &Row {
        &self.0
    }
}

/// Intermediate row appearing only in a QueryPlan.
///
/// This row is an output from a QueryPlan's operation and is **newly created** by the operation.
#[derive(PartialEq, Debug, new)]
pub(super) struct NewRow(Row);

impl From<NewRow> for Row {
    fn from(n: NewRow) -> Self {
        n.0
    }
}
