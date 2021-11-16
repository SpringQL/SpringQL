//! Low-level API functions to execute / register SpringQL from Rust.
//!
//! Has the similar interface like [SQLite](https://www.sqlite.org/c3ref/intro.html).
//!
//! C API and high-level Rust API are provided separately.

mod engine_mutex;

use crate::{error::Result, sql_processor::SqlProcessor, stream_engine::command::Command};

use self::engine_mutex::EngineMutex;

// TODO config
const N_WORKER_THREADS: usize = 2;

/// Connection object.
///
/// 1 stream pipeline has only 1 connection.
/// In other words, the lifecycle of SpringConnection and internal stream pipeline are the same.
#[derive(Debug)]
pub struct SpringPipeline {
    engine: EngineMutex,
    sql_processor: SqlProcessor,
}

/// Row object from an in memory queue.
#[derive(Debug)]
pub struct SpringRow;

/// Successful response from `spring_step()`.
#[derive(Eq, PartialEq, Debug)]
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
pub fn spring_open() -> Result<SpringPipeline> {
    let engine = EngineMutex::new(N_WORKER_THREADS);
    let sql_processor = SqlProcessor::default();

    Ok(SpringPipeline {
        engine,
        sql_processor,
    })
}

/// Execute commands (DDL).
///
/// # Failure
///
/// - [SpringError::Sql](crate::error::SpringError::Sql) when:
///   - Invalid SQL syntax.
///   - Refers to undefined objects (streams, pumps, etc)
/// - [SpringError::InvalidOption](crate::error::SpringError::Sql) when:
///   - `OPTIONS` in `CREATE` statement includes invalid key or value.
pub fn spring_command(pipeline: &SpringPipeline, sql: &str) -> Result<()> {
    let command = pipeline.sql_processor.compile(sql)?;
    let mut engine = pipeline.engine.get()?;

    match command {
        Command::AlterPipeline(c) => engine.alter_pipeline(c),
    }
}

/// Pop a row from an in memory queue.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - queue named `queue` does not exist.
pub fn spring_pop(queue: &str) -> Result<SpringRow> {
    todo!()
}

/// Get an integer column.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - `i_col` already fetched.
///   - `i_col` out of range.
pub fn spring_column_i32(row: &SpringRow, i_col: usize) -> Result<i32> {
    todo!()
}

/// Get an text column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_text(row: &SpringRow, i_col: usize) -> Result<String> {
    todo!()
}
