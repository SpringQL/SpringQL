// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use anyhow::anyhow;

use crate::{
    api::error::{foreign_info::ForeignInfo, Result, SpringError},
    api::SpringSourceReaderConfig,
    pipeline::{InMemoryQueueOptions, Options, QueueName},
    stream_engine::{
        autonomous_executor::{row::SourceRow, task::source_task::source_reader::SourceReader},
        in_memory_queue_repository::InMemoryQueueRepository,
    },
};

#[derive(Debug)]
pub struct InMemoryQueueSourceReader(QueueName);

impl SourceReader for InMemoryQueueSourceReader {
    /// # Failure
    ///
    /// - `SpringError::Unavailable` when:
    ///   - queue name provided from the option is invalid
    /// - `SpringError::InvalidOption`
    fn start(options: &Options, _config: &SpringSourceReaderConfig) -> Result<Self> {
        let options = InMemoryQueueOptions::try_from(options)?;
        let queue_name = options.queue_name;
        InMemoryQueueRepository::instance().create(queue_name.clone())?;
        Ok(Self(queue_name))
    }

    /// # Failure
    ///
    /// - `SpringError::ForeignSourceTimeout` when:
    ///   - queue does not have any row (does not wait a bit)
    fn next_row(&mut self) -> Result<SourceRow> {
        let q = InMemoryQueueRepository::instance().get(&self.0)?;

        if let Some(row) = q.pop_non_blocking() {
            Ok(SourceRow::Raw(row))
        } else {
            Err(SpringError::ForeignSourceTimeout {
                source: anyhow!("queue is empty"),
                foreign_info: ForeignInfo::InMemoryQueue(self.0.clone()),
            })
        }
    }
}
