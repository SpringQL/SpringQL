// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Low-level API functions to execute / register SpringQL from Rust.
//!
//! C API and high-level Rust API are provided separately.

mod engine_mutex;
mod spring_config;

pub use spring_config::*;

use std::sync::Once;

use crate::{
    error::Result,
    pipeline::name::QueueName,
    sql_processor::SqlProcessor,
    stream_engine::{command::Command, SinkRow, SqlConvertible, SqlValue},
};

use self::engine_mutex::EngineMutex;

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

/// Row object from an in memory queue.
#[derive(Debug)]
pub struct SpringRow(SinkRow);

impl From<SinkRow> for SpringRow {
    fn from(sink_row: SinkRow) -> Self {
        Self(sink_row)
    }
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
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - queue named `queue` does not exist.
pub fn spring_pop(pipeline: &SpringPipeline, queue: &str) -> Result<SpringRow> {
    let mut engine = pipeline.engine.get()?;
    let sink_row = engine.pop_in_memory_queue(QueueName::new(queue.to_string()))?;
    Ok(SpringRow::from(sink_row))
}

/// Get an integer column.
///
/// # Failure
///
/// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
///   - `i_col` already fetched.
///   - `i_col` out of range.
pub fn spring_column_i32(row: &SpringRow, i_col: usize) -> Result<i32> {
    spring_column(row, i_col)
}

/// Get an text column.
///
/// # Failure
///
/// Same as [spring_column_i32()](spring_column_i32)
pub fn spring_column_text(row: &SpringRow, i_col: usize) -> Result<String> {
    spring_column(row, i_col)
}

fn spring_column<T: SqlConvertible>(row: &SpringRow, i_col: usize) -> Result<T> {
    let v = row.0.get_by_index(i_col)?;
    if let SqlValue::NotNull(v) = v {
        v.unpack()
    } else {
        todo!("support nullable value")
    }
}
