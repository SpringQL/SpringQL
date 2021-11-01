use crate::stream_engine::{
    autonomous_executor::{FlowEfficientScheduler, TestRowRepository, Timestamp},
    CurrentTimestamp,
};

use super::DependencyInjection;

#[derive(Debug)]
pub(crate) struct TestDI {
    row_repo: TestRowRepository,
}

impl DependencyInjection for TestDI {
    type CurrentTimestampType = TestCurrentTimestamp;
    type RowRepositoryType = TestRowRepository;
    type SchedulerType = FlowEfficientScheduler;
}

#[derive(Debug)]
pub(crate) struct TestCurrentTimestamp;

impl CurrentTimestamp for TestCurrentTimestamp {
    fn now() -> Timestamp {
        Timestamp::fx_now()
    }
}
