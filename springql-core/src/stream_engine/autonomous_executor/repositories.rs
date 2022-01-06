use super::{
    row::row_repository::RowRepository,
    task::{
        sink_task::sink_writer::sink_writer_repository::SinkWriterRepository,
        source_task::source_reader::source_reader_repository::SourceReaderRepository,
    },
};

/// Collection of repository instances.
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct Repositories {
    row_repository: RowRepository,
    source_reader_repository: SourceReaderRepository,
    sink_writer_repository: SinkWriterRepository,
}

impl Repositories {
    pub(in crate::stream_engine::autonomous_executor) fn row_repository(&self) -> &RowRepository {
        &self.row_repository
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
