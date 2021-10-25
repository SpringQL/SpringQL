use crate::stream_engine::{CurrentTimestamp, RefCntGcRowRepository, Timestamp};

use super::DependencyInjection;

#[derive(Debug, Default)]
pub(crate) struct TestDI {
    row_repo: RefCntGcRowRepository,
}

impl DependencyInjection for TestDI {
    type CurrentTimestampType = TestCurrentTimestamp;
    type RowRepositoryType = RefCntGcRowRepository;

    fn row_repository(&self) -> &Self::RowRepositoryType {
        &self.row_repo
    }
}

#[derive(Debug)]
pub(crate) struct TestCurrentTimestamp;

impl CurrentTimestamp for TestCurrentTimestamp {
    fn now() -> Timestamp {
        Timestamp::fx_now()
    }
}
