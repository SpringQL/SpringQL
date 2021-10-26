use std::rc::Rc;

use crate::stream_engine::executor::data::row::Row;

/// Intermediate row appearing only in a QueryPlan.
///
/// This row is an output from a QueryPlan's operation and is **not changed** by the operation.
#[derive(Clone, PartialEq, Debug, new)]
pub(super) struct PreservedRow(Rc<Row>);

/// Intermediate row appearing only in a QueryPlan.
///
/// This row is an output from a QueryPlan's operation and is **newly created** by the operation.
#[derive(PartialEq, Debug, new)]
pub(super) struct NewRow(Row);

impl AsRef<Row> for PreservedRow {
    fn as_ref(&self) -> &Row {
        &self.0
    }
}
