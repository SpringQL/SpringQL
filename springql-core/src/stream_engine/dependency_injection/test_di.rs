use crate::stream_engine::{
    autonomous_executor::{FlowEfficientScheduler, NaiveRowRepository, Timestamp},
    CurrentTimestamp,
};

use super::DependencyInjection;

#[derive(Debug)]
pub(crate) struct TestDI {
    row_repo: NaiveRowRepository,
}

impl DependencyInjection for TestDI {
    type CurrentTimestampType = TestCurrentTimestamp;
    type RowRepositoryType = NaiveRowRepository;
    type SchedulerType = FlowEfficientScheduler;
}

#[derive(Debug)]
pub(crate) struct TestCurrentTimestamp;

impl CurrentTimestamp for TestCurrentTimestamp {
    fn now() -> Timestamp {
        Timestamp::fx_now()
    }
}
