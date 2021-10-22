use super::ForeignInputRow;

/// Chunk of ForeignInputRow's.
///
/// Ordered by timestamp (smaller comes first).
#[derive(Debug)]
pub(in crate::stream_engine::executor) struct ForeignInputRowChunk(Vec<ForeignInputRow>);
