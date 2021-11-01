#[cfg(test)]
pub(crate) mod test_row_repository;

use std::rc::Rc;

#[cfg(test)]
pub(crate) use test_row_repository::TestRowRepository;

use crate::{error::Result, model::name::PumpName};

use super::Row;

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
/// emit(r1, vec!["s1_p1", "s1_p2"]);
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
/// emit(r2, vec!["s1_p1", "s1_p2"]);
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
pub(crate) trait RowRepository: Default {
    /// Get the next RowRef from `pump`.
    ///
    /// # Failure
    ///
    /// - [SpringError::InputTimeout](crate::error::SpringError::InputTimeout) when:
    ///   - next row is not available within `timeout`
    fn collect_next(&self, pump: &PumpName) -> Result<Rc<Row>>;

    /// Gives `row_ref` to `dest_pumps`.
    fn emit(&self, row_ref: Rc<Row>, downstream_pumps: &[PumpName]) -> Result<()>;

    /// Move newly created `row` to `dest_pumps`.
    fn emit_owned(&self, row: Row, downstream_pumps: &[PumpName]) -> Result<()>;
}
