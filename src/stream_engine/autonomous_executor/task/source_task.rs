use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::stream_engine::autonomous_executor::data::row::Row;
use crate::stream_engine::autonomous_executor::server::source::SourceServerActive;
use crate::stream_engine::dependency_injection::DependencyInjection;
use crate::stream_engine::pipeline::foreign_stream_model::ForeignStreamModel;

#[derive(Debug, new)]
pub(in crate::stream_engine) struct SourceTask {
    /// 1 server can be shared to 2 or more foreign streams.
    upstream_server: Arc<Mutex<Box<dyn SourceServerActive>>>,

    downstream: Arc<ForeignStreamModel>,
}

impl SourceTask {
    fn collect_next<DI: DependencyInjection>(&self) -> Result<Row> {
        let foreign_row = self
            .upstream_server
            .lock()
            .expect("other worker threads sharing the same server must not get panic")
            .next_row()?;
        foreign_row.into_row::<DI>(self.downstream.shape())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        error::{Result, SpringError},
        stream_engine::{
            autonomous_executor::{
                data::foreign_row::format::json::JsonObject,
                server::source::net::NetSourceServerActive,
            },
            dependency_injection::test_di::TestDI,
        },
    };

    use super::*;

    #[test]
    fn test_foreign_input_pump() -> Result<()> {
        let j1 = JsonObject::fx_city_temperature_tokyo();
        let j2 = JsonObject::fx_city_temperature_osaka();
        let j3 = JsonObject::fx_city_temperature_london();

        let server = NetSourceServerActive::factory_with_test_source(vec![j1, j2, j3]);
        let stream = ForeignStreamModel::fx_city_temperature_source();
        let pump = SourceTask::new(Arc::new(Mutex::new(Box::new(server))), Arc::new(stream));

        assert_eq!(
            pump.collect_next::<TestDI>()?,
            Row::fx_city_temperature_tokyo()
        );
        assert_eq!(
            pump.collect_next::<TestDI>()?,
            Row::fx_city_temperature_osaka()
        );
        assert_eq!(
            pump.collect_next::<TestDI>()?,
            Row::fx_city_temperature_london()
        );
        assert!(matches!(
            pump.collect_next::<TestDI>().unwrap_err(),
            SpringError::ForeignSourceTimeout { .. }
        ));

        Ok(())
    }
}
