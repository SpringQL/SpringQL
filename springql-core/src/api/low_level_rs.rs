// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! Low-level API functions to execute / register SpringQL from Rust.
//!
//! C API and high-level Rust API are provided separately.

mod config;
mod engine_mutex;

use std::sync::Once;

use crate::{
    error::Result,
    pipeline::name::QueueName,
    sql_processor::SqlProcessor,
    stream_engine::{command::Command, SinkRow, SqlConvertible, SqlValue},
};

use self::{config::SpringConfig, engine_mutex::EngineMutex};

// TODO config
const N_WORKER_THREADS: usize = 2;

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
///
/// # Parameters
///
/// - `overwrite_config_toml`: TOML format configuration to overwrite default. See `SPRING_CONFIG_DEFAULT` in [config.rs](https://github.com/SpringQL/SpringQL/tree/main/springql-core/src/api/low_level_rs/config.rs) for full-set default configuration.
///
/// # Failures
///
/// - [SpringError::InvalidConfig](crate::error::SpringError::InvalidConfig) when:
///   - `overwrite_config_toml` includes invalid key and/or value.
/// - [SpringError::InvalidFormat](crate::error::SpringError::InvalidFormat) when:
///   - `overwrite_config_toml` is not valid as TOML.
pub fn spring_open(overwrite_config_toml: &str) -> Result<SpringPipeline> {
    let config = SpringConfig::new(overwrite_config_toml)?;

    setup_logger();

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
