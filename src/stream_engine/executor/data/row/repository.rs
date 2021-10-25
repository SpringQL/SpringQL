#[cfg(test)]
pub(crate) mod naive_row_repository;

#[cfg(test)]
pub(crate) use naive_row_repository::NaiveRowRepository;

use crate::{
    error::Result,
    model::name::{PumpName, StreamName},
};

use super::Row;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) struct RowRef;

impl RowRef {
    pub(in crate::stream_engine::executor) fn get(&self) -> &Row {
        todo!()
    }
}

/// # Concept diagram
///
/// ```text
/// ---> (Stream "s1") -+- (Pump "s1_p1") ---> (Stream "s2")
///                     |   in buf: []
///                     |
///                     +- (Pump "s1_p2") ---> (Stream "s3")
///                         in buf: []
/// ```
///
/// ```text
/// emit(r1, "s1");
///
/// ---> (Stream "s1") -+- (Pump "s1_p1") ---> (Stream "s2")
///                     |   in buf: [r1]
///                     |
///                     +- (Pump "s1_p2") ---> (Stream "s3")
///                         in buf: [r1]
/// ```
///
/// ```text
/// collect_next("s1_p1");  // -> r1
///
/// ---> (Stream "s1") -+- (Pump "s1_p1") ---> (Stream "s2")
///                     |   in buf: []
///                     |
///                     +- (Pump "s1_p2") ---> (Stream "s3")
///                         in buf: [r1]
/// ```
///
/// ```text
/// emit(r2, "s1");
///
/// ---> (Stream "s1") -+- (Pump "s1_p1") ---> (Stream "s2")
///                     |   in buf: [r2]
///                     |
///                     +- (Pump "s1_p2") ---> (Stream "s3")
///                         in buf: [r2, r1]
/// ```
///
/// ```text
/// collect_next("s1_p2");  // -> r1
///
/// ---> (Stream "s1") -+- (Pump "s1_p1") ---> (Stream "s2")
///                     |   in buf: [r2]
///                     |
///                     +- (Pump "s1_p2") ---> (Stream "s3")
///                         in buf: [r2]
/// ```
pub(crate) trait RowRepository {
    /// Get ref to Row from RowRef.
    fn get(&self, row_ref: &RowRef) -> &Row;

    /// Get the next RowRef from `pump`.
    fn collect_next(&self, pump: &PumpName) -> Result<RowRef>;

    /// Move `row` to `dest_stream`.
    /// The row is distributed to all pumps going out from `dest_stream`.
    fn emit(&self, row: Row, dest_stream: &StreamName) -> Result<()>;
}

#[derive(Debug, Default)]
pub(crate) struct RefCntGcRowRepository;

impl RowRepository for RefCntGcRowRepository {
    fn get(&self, row_ref: &RowRef) -> &Row {
        todo!()
    }

    fn collect_next(&self, pump: &PumpName) -> Result<RowRef> {
        todo!()
    }

    fn emit(&self, row: Row, dest_stream: &StreamName) -> Result<()> {
        todo!()
    }
}
