use super::Row;

/// Chunk of rows.
///
/// Ordered by timestamp (smaller comes first).
#[derive(Debug)]
pub(in crate::stream_engine::executor) struct RowChunk(Vec<Row>);
