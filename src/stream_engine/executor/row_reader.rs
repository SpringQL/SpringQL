use crate::error::Result;

use super::row::row_chunk::RowChunk;

trait RowReader {
    fn next() -> Result<RowChunk>;
}
