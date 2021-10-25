use crate::stream_engine::{CurrentTimestamp, Timestamp};

use super::DependencyInjection;

#[derive(Debug)]
pub(crate) struct TestDI;

impl DependencyInjection for TestDI {
    type CurrentTimestampType = TestCurrentTimestamp;
}

#[derive(Debug)]
pub(crate) struct TestCurrentTimestamp;

impl CurrentTimestamp for TestCurrentTimestamp {
    fn now() -> Timestamp {
        Timestamp::fx_now()
    }
}
