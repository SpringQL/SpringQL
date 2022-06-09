// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{sync::Once, thread, time::Duration};

use crate::{
    api::{error::Result, SpringConfig},
    pipeline::name::QueueName,
    sql_processor::SqlProcessor,
    stream_engine::{command::Command, EngineMutex, SinkRow},
};

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
/// In other words, the lifecycle of Connection and internal stream pipeline are the same.
#[derive(Debug)]
pub struct Connection {
    engine: EngineMutex,
    sql_processor: SqlProcessor,
}

impl Connection {
    pub fn new(config: &SpringConfig) -> Self {
        setup_logger();

        let engine = EngineMutex::new(config);
        let sql_processor = SqlProcessor::default();

        Self {
            engine,
            sql_processor,
        }
    }
}

/// Execute commands (DDL).
///
/// # Failure
///
/// - `SpringError::Sql` when:
///   - Invalid SQL syntax.
///   - Refers to undefined objects (streams, pumps, etc)
///   - Other semantic errors.
/// - `SpringError::InvalidOption` when:
///   - `OPTIONS` in `CREATE` statement includes invalid key or value.
pub fn spring_command(pipeline: &Connection, sql: &str) -> Result<()> {
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
/// - `SpringError::Unavailable` when:
///   - queue named `queue` does not exist.
pub fn spring_pop(pipeline: &Connection, queue: &str) -> Result<SinkRow> {
    const SLEEP_MSECS: u64 = 10;

    let mut engine = pipeline.engine.get()?;

    loop {
        if let Some(sink_row) =
            engine.pop_in_memory_queue_non_blocking(QueueName::new(queue.to_string()))?
        {
            return Ok(sink_row);
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
/// - `SpringError::Unavailable` when:
///   - queue named `queue` does not exist.
pub fn spring_pop_non_blocking(pipeline: &Connection, queue: &str) -> Result<Option<SinkRow>> {
    let mut engine = pipeline.engine.get()?;
    let sink_row = engine.pop_in_memory_queue_non_blocking(QueueName::new(queue.to_string()))?;
    Ok(sink_row)
}
