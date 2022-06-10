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

    pub fn command(&self, sql: &str) -> Result<()> {
        let mut engine = self.engine.get()?;

        let command = self.sql_processor.compile(sql, engine.current_pipeline())?;

        match command {
            Command::AlterPipeline(c) => engine.alter_pipeline(c),
        }
    }

    pub fn pop(&self, queue: &str) -> Result<SinkRow> {
        const SLEEP_MSECS: u64 = 10;

        let mut engine = self.engine.get()?;

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

    pub fn pop_non_blocking(&self, queue: &str) -> Result<Option<SinkRow>> {
        let mut engine = self.engine.get()?;
        let sink_row =
            engine.pop_in_memory_queue_non_blocking(QueueName::new(queue.to_string()))?;
        Ok(sink_row)
    }
}
