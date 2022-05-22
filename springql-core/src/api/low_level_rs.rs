// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Low-level API functions to execute / register SpringQL from Rust.
//!
//! C API and high-level Rust API are provided separately.

mod engine_mutex;
mod spring_config;

pub use spring_config::*;

use std::{sync::Once, thread, time::Duration};

use crate::{
    error::{Result, SpringError},
    pipeline::name::QueueName,
    sql_processor::SqlProcessor,
    stream_engine::{command::Command, SpringValue, SqlValue},
};

use self::engine_mutex::EngineMutex;
use crate::pipeline::SpringRow;

fn setup_logger() {
    static INIT: Once = Once::new();

    INIT.call_once(|| {
        let _ = env_logger::builder()
            .is_test(false) // To enable color. Logs are not captured by test framework.
            .try_init();
        log_panics::init();
    });

    log::info!("setup_logger(): done");
}
/// Connection object.
///
/// 1 stream pipeline has only 1 connection.
/// In other words, the lifecycle of SpringConnection and internal stream pipeline are the same.
#[derive(Debug)]
pub struct SpringPipeline {
    engine: EngineMutex,
    sql_processor: SqlProcessor,
}

/// Creates and open an in-process stream pipeline.
pub fn spring_open(config: &SpringConfig) -> Result<SpringPipeline> {
    setup_logger();

    let engine = EngineMutex::new(config);
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
///   - Other semantic errors.
/// - [SpringError::InvalidOption](crate::error::SpringError::Sql) when:
///   - `OPTIONS` in `CREATE` statement includes invalid key or value.
pub fn spring_command(pipeline: &SpringPipeline, sql: &str) -> Result<()> {
    let mut engine = pipeline.engine.get()?;

    let command = pipeline
        .sql_processor
        .compile(sql, engine.current_pipeline())?;

    match command {
        Command::AlterPipeline(c) => engine.alter_pipeline(c),
    }
}

/// Pop a row from an in memory queue. This is a blocking function.
///
/// **Do not call this function from threads.**
/// If you need to pop from multiple in-memory queues using threads, use `spring_pop_non_blocking()`.
/// See: <https://github.com/SpringQL/SpringQL/issues/125>
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - queue named `queue` does not exist.
pub fn spring_pop(pipeline: &SpringPipeline, queue: &str) -> Result<SpringRow> {
    const SLEEP_MSECS: u64 = 10;

    let mut engine = pipeline.engine.get()?;

    loop {
        if let Some(sink_row) =
            engine.pop_in_memory_queue_non_blocking(QueueName::new(queue.to_string()))?
        {
            return Ok(SpringRow::from(sink_row));
        } else {
            thread::sleep(Duration::from_millis(SLEEP_MSECS));
        }
    }
}

/// Pop a row from an in memory queue. This is a non-blocking function.
///
/// # Returns
///
/// - `Ok(Some)` when at least a row is in the queue.
/// - `None` when no row is in the queue.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - queue named `queue` does not exist.
pub fn spring_pop_non_blocking(
    pipeline: &SpringPipeline,
    queue: &str,
) -> Result<Option<SpringRow>> {
    let mut engine = pipeline.engine.get()?;
    let sink_row = engine.pop_in_memory_queue_non_blocking(QueueName::new(queue.to_string()))?;
    Ok(sink_row.map(SpringRow::from))
}

/// Get an integer column.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - `i_col` already fetched.
///   - `i_col` out of range.
/// - [SpringError::Null](crate::error::SpringError::Null) when:
///   - Column value is NULL
pub fn spring_column_i32(row: &SpringRow, i_col: usize) -> Result<i32> {
    spring_column_not_null(row, i_col)
}

/// Get a text column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_text(row: &SpringRow, i_col: usize) -> Result<String> {
    spring_column_not_null(row, i_col)
}

/// Get a boolean column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_bool(row: &SpringRow, i_col: usize) -> Result<bool> {
    spring_column_not_null(row, i_col)
}

/// Get a float column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_f32(row: &SpringRow, i_col: usize) -> Result<f32> {
    spring_column_not_null(row, i_col)
}

/// Get a 2-byte integer column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_i16(row: &SpringRow, i_col: usize) -> Result<i16> {
    spring_column_not_null(row, i_col)
}

/// Get a 8-byte integer column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_i64(row: &SpringRow, i_col: usize) -> Result<i64> {
    spring_column_not_null(row, i_col)
}

fn spring_column_not_null<T: SpringValue>(row: &SpringRow, i_col: usize) -> Result<T> {
    let v = row.0.get_by_index(i_col)?;
    if let SqlValue::NotNull(v) = v {
        v.unpack()
    } else {
        Err(SpringError::Null {
            stream_name: row.0.stream_name().clone(),
            i_col,
        })
    }
}
