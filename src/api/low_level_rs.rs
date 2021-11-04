//! Low-level API functions to execute / register SpringQL from Rust.
//!
//! Has the similar interface like [SQLite](https://www.sqlite.org/c3ref/intro.html).
//!
//! C API and high-level Rust API are provided separately.

mod engine_mutex;

use std::sync::atomic::Ordering;

use anyhow::anyhow;

use crate::{
    error::{Result, SpringError},
    stream_engine::command::Command,
    PIPELINE_CREATED,
};

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
}

/// Prepared statement.
#[derive(Debug)]
pub struct SpringStatement {
    engine: EngineMutex,
    command: Command,
}

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
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - Pipeline is already open.
pub fn spring_open() -> Result<SpringPipeline> {
    let created = PIPELINE_CREATED.load(Ordering::SeqCst);

    if created {
        return Err(SpringError::Unavailable {
            source: anyhow!("pipeline already open"),
            resource: "pipeline".to_string(),
        });
    }
    PIPELINE_CREATED.store(true, Ordering::SeqCst);

    let engine = EngineMutex::new(N_WORKER_THREADS);
    Ok(SpringPipeline { engine })
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
pub fn spring_prepare(pipeline: &SpringPipeline, sql: &str) -> Result<SpringStatement> {
    let command = todo!();
    Ok(SpringStatement {
        engine: pipeline.engine.clone(),
        command,
    })
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
pub fn spring_step(stmt: &SpringStatement) -> Result<SpringStepSuccess> {
    let mut engine = stmt.engine.get()?;

    match &stmt.command {
        Command::AlterPipeline(c) => engine
            .alter_pipeline(c.clone())
            .map(|_| SpringStepSuccess::Done),
    }
}

/// Get an integer column.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - `i_col` already fetched.
///   - `i_col` out of range.
///   - `stmt` is not in `SpringStepSuccess::Row` state.
pub fn spring_column_i32(stmt: &SpringStatement, i_col: usize) -> Result<i32> {
    todo!()
}

/// Get an text column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_text(stmt: &SpringStatement, i_col: usize) -> Result<String> {
    todo!()
}

/// Destroys the prepared statement.
///
/// You don't have to call this function but just dropping (moving out) the prepared statement object is enough.
pub fn spring_finalize(stmt: SpringStatement) {
    // just drop stmt
}

/// Destroys the in-process stream pipeline.
///
/// You don't have to call this function but just dropping (moving out) the connection object is enough.
pub fn spring_close(pipeline: SpringPipeline) {
    // just drop pipeline
}
