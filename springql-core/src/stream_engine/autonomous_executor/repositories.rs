use crate::low_level_rs::SpringConfig;

use super::{
    queue::row_queue_repository::RowQueueRepository,
    task::{
        sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
        source_task::source_reader::source_reader_repository::SourceReaderRepository,
    },
};

/// Collection of repository instances.
#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor) struct Repositories {
    row_queue_repository: RowQueueRepository,
    source_reader_repository: SourceReaderRepository,
    sink_writer_repository: SinkWriterRepository,
}

impl Repositories {
    pub(in crate::stream_engine::autonomous_executor) fn new(config: &SpringConfig) -> Self {
        Self {
            row_queue_repository: RowQueueRepository::default(),
            source_reader_repository: SourceReaderRepository::default(),
            sink_writer_repository: SinkWriterRepository::new(config.sink_writer),
        }
    }

    pub(in crate::stream_engine::autonomous_executor) fn row_queue_repository(
        &self,
    ) -> &RowQueueRepository {
        &self.row_queue_repository
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
