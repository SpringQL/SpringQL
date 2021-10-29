use crate::stream_engine::{
    CurrentTimestamp, FlowEfficientScheduler, TestRowRepository, Timestamp,
};

use super::DependencyInjection;

#[derive(Debug, Default)]
pub(crate) struct TestDI {
    row_repo: TestRowRepository,
}

impl DependencyInjection for TestDI {
    type CurrentTimestampType = TestCurrentTimestamp;
    type RowRepositoryType = TestRowRepository;
    type SchedulerType = FlowEfficientScheduler;

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
