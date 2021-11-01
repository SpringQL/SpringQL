use crate::stream_engine::{
    autonomous_executor::{FlowEfficientScheduler, NaiveRowRepository, Timestamp},
    CurrentTimestamp,
};

use super::DependencyInjection;

#[derive(Debug)]
pub(crate) struct ProdDI {
    row_repo: NaiveRowRepository,
}

impl DependencyInjection for ProdDI {
    type CurrentTimestampType = SystemTimestamp;
    type RowRepositoryType = NaiveRowRepository;
    type SchedulerType = FlowEfficientScheduler;
}

#[derive(Debug)]
pub(crate) struct SystemTimestamp;

impl CurrentTimestamp for SystemTimestamp {
    fn now() -> Timestamp {
        let t = chrono::offset::Utc::now().naive_utc();
        Timestamp::new(t)
    }
}
