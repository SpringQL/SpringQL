use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::error::{Result, SpringError};
use crate::stream_engine::autonomous_executor::data::row::Row;
use crate::stream_engine::autonomous_executor::server::source::net::NetSourceServerStandby;
use crate::stream_engine::autonomous_executor::server::source::{
    SourceServerActive, SourceServerStandby,
};
use crate::stream_engine::dependency_injection::DependencyInjection;
use crate::stream_engine::pipeline::foreign_stream_model::ForeignStreamModel;
use crate::stream_engine::pipeline::server_model::server_type::ServerType;
use crate::stream_engine::pipeline::server_model::ServerModel;

use super::task_id::TaskId;

#[derive(Debug)]
pub(in crate::stream_engine) struct SourceTask {
    id: TaskId,

    /// 1 server can be shared to 2 or more foreign streams.
    upstream_server: Arc<Mutex<Box<dyn SourceServerActive>>>,

    downstream: Arc<ForeignStreamModel>,
}

impl SourceTask {
    pub(in crate::stream_engine) fn new(server: &ServerModel) -> Result<Self> {
        let id = TaskId::from_source_server(server.serving_foreign_stream().name().clone());
        let upstream_server = match server.server_type() {
            ServerType::SourceNet => {
                let server_standby = NetSourceServerStandby::new(server.options())?;
                let server_active = server_standby.start()?;
                Box::new(server_active)
            }
        };
        let downstream = server.serving_foreign_stream().clone();

        Ok(Self {
            id,
            upstream_server: Arc::new(Mutex::new(upstream_server)),
            downstream,
        })
    }

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
                test_support::foreign::source::TestSource,
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

        let test_source = TestSource::start(vec![j1, j2, j3])?;

        let stream = Arc::new(ForeignStreamModel::fx_city_temperature_source());
        let server = ServerModel::fx_net_source(stream, test_source.host_ip(), test_source.port());
        let pump = SourceTask::new(&server)?;

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
