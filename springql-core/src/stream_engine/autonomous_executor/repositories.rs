// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::api::low_level_rs::SpringConfig;

use super::{
    queue::{
        row_queue_repository::RowQueueRepository, window_queue_repository::WindowQueueRepository,
    },
    task::{
        sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
        source_task::source_reader::source_reader_repository::SourceReaderRepository,
    },
};

/// Collection of repository instances.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct Repositories {
    row_queue_repository: RowQueueRepository,
    window_queue_repository: WindowQueueRepository,
    source_reader_repository: SourceReaderRepository,
    sink_writer_repository: SinkWriterRepository,
}

impl Repositories {
    pub(in crate::stream_engine::autonomous_executor) fn new(config: &SpringConfig) -> Self {
        Self {
            row_queue_repository: RowQueueRepository::default(),
            window_queue_repository: WindowQueueRepository::default(),
            source_reader_repository: SourceReaderRepository::new(config.source_reader),
            sink_writer_repository: SinkWriterRepository::new(config.sink_writer),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn row_queue_repository(
        &self,
    ) -> &RowQueueRepository {
        &self.row_queue_repository
    }

    pub(in crate::stream_engine::autonomous_executor) fn window_queue_repository(
        &self,
    ) -> &WindowQueueRepository {
        &self.window_queue_repository
    }

    pub(in crate::stream_engine::autonomous_executor) fn source_reader_repository(
        &self,
    ) -> &SourceReaderRepository {
        &self.source_reader_repository
    }

    pub(in crate::stream_engine::autonomous_executor) fn sink_writer_repository(
        &self,
    ) -> &SinkWriterRepository {
        &self.sink_writer_repository
    }
}
