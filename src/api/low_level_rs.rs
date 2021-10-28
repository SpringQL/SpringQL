//! Low-level API functions to execute / register SpringQL from Rust.
//!
//! Has the similar interface like [SQLite](https://www.sqlite.org/c3ref/intro.html).
//!
//! C API and high-level Rust API are provided separately.

use crate::error::Result;

/// Connection object.
///
/// 1 stream pipeline has only 1 connection.
/// In other words, the lifecycle of SpringConnection and internal stream pipeline are the same.
#[derive(Debug)]
pub struct SpringPipeline;

/// Prepared statement.
#[derive(Debug)]
pub struct SpringStatement;

/// Successful response from `spring_step()`.
#[derive(Debug)]
pub enum SpringStepSuccess {
    /// No more rows available.
    ///
    /// You must not call `spring_step()` again with the same prepared statement object
    /// after you get this response.
    ///
    /// Returned on:
    /// - DDL statements
    /// - SELECT statements to table after reading final row.
    Done,

    /// Your prepared statement has gotten a new row.
    /// You can call `spring_column_*()` to get column values.
    ///
    /// Returned on:
    /// - SELECT statements to stream or table.
    Row,
}

/// Creates and open an in-process stream pipeline.
///
/// # Failure
///
/// (will be added)
pub fn spring_open() -> Result<SpringPipeline> {
    todo!()
}

/// Creates a prepared statement.
///
/// # Failure
///
/// - [SpringError::Sql](crate::error::SpringError::Sql) when:
///   - Invalid SQL syntax.
///   - Refers to undefined objects (streams, pumps, etc)
/// - [SpringError::InvalidOption](crate::error::SpringError::Sql) when:
///   - `OPTIONS` in `CREATE` statement includes invalid key or value.
pub fn spring_prepare(sql: &str) -> Result<SpringStatement> {
    todo!()
}

/// - DDL: Executes a prepared statement.
///   - [SpringStepSuccess::Done](crate::api::low_level_rs::SpringStepSuccess::Done) is returned on success;
/// - SELECT: Get a next row from sink.
///   - [SpringStepSuccess::Row](crate::api::low_level_rs::SpringStepSuccess::Row) is returned when next row is available. Call `spring_step()` again after `spring_column_*()` if you need more rows.
///   - [SpringStepSuccess::Done](crate::api::low_level_rs::SpringStepSuccess::Done) is returned when no more row is available.
///
/// This function is non-blocking.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - `stmt` is already `SpringStepSuccess::Done`.
///   - `stmt` is already `spring_finalize()`-ed.
pub fn spring_step(stmt: &mut SpringStatement) -> Result<SpringStepSuccess> {
    todo!()
}

/// Get an integer column.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - `i_col` already fetched.
///   - `i_col` out of range.
///   - `stmt` is not in `SpringStepSuccess::Row` state.
pub fn spring_column_i32(stmt: &mut SpringStatement, i_col: usize) -> Result<i32> {
    todo!()
}

/// Get an text column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_text(stmt: &mut SpringStatement, i_col: usize) -> Result<String> {
    todo!()
}

/// Destroys the prepared statement.
///
/// You don't have to call this function but just dropping (moving out) the connection object is enough.
pub fn spring_finalize(stmt: SpringStatement) {
    // just drop stmt
}

/// Destroys the in-process stream pipeline.
///
/// You don't have to call this function but just dropping (moving out) the connection object is enough.
pub fn spring_close(conn: SpringPipeline) {
    // just drop conn
}
