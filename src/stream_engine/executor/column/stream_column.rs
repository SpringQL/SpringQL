use crate::timestamp::Timestamp;

/// Column values in a stream.
#[derive(Eq, PartialEq, Debug, Default)]
pub(in crate::stream_engine::executor) struct StreamColumn;

impl StreamColumn {
    pub(in crate::stream_engine::executor) fn promoted_rowtime(&self) -> Option<&Timestamp> {
        todo!()
    }
}
