use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use crate::error::{Result, SpringError};
use crate::stream_engine::autonomous_executor::data::row::Row;
use crate::stream_engine::autonomous_executor::server::sink::net::NetSinkServerStandby;
use crate::stream_engine::autonomous_executor::server::sink::SinkServerStandby;
use crate::stream_engine::autonomous_executor::server::source::net::NetSourceServerStandby;
use crate::stream_engine::autonomous_executor::server::source::{
    SourceServerActive, SourceServerStandby,
};
use crate::stream_engine::autonomous_executor::RowRepository;
use crate::stream_engine::dependency_injection::DependencyInjection;
use crate::stream_engine::pipeline::foreign_stream_model::ForeignStreamModel;
use crate::stream_engine::pipeline::server_model::server_type::ServerType;
use crate::stream_engine::pipeline::server_model::ServerModel;

use super::task_context::TaskContext;
use super::task_id::TaskId;
use super::task_state::TaskState;

#[derive(Debug)]
enum SourceTaskState {
    Stopped,
    Started {
        /// 1 server can be shared to 2 or more foreign streams.
        upstream_server: Arc<Mutex<Box<dyn SourceServerActive>>>,
    },
}

#[derive(Debug)]
pub(crate) struct SourceTask {
    id: TaskId,
    server_model: ServerModel,
    state: SourceTaskState,
    downstream: Arc<ForeignStreamModel>,
}

impl SourceTask {
    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine) fn state(&self) -> TaskState {
        match self.state {
            SourceTaskState::Stopped => TaskState::Stopped,
            SourceTaskState::Started { .. } => TaskState::Started,
        }
    }

    pub(in crate::stream_engine) fn new(server: ServerModel) -> Self {
        let id = TaskId::from_source_server(server.serving_foreign_stream().name().clone());
        let downstream = server.serving_foreign_stream().clone();
        Self {
            id,
            server_model: server,
            state: SourceTaskState::Stopped,
            downstream,
        }
    }

    pub(in crate::stream_engine) fn start(&mut self) -> Result<()> {
        let upstream_server = match self.server_model.server_type() {
            ServerType::SourceNet => {
                let server_standby = NetSourceServerStandby::new(self.server_model.options())?;
                let server_active = server_standby.start()?;
                Box::new(server_active)
            }
            ServerType::SinkNet => {
                unreachable!("sink type server ({:?}) for SourceTask", self.server_model)
            }
        };
        self.state = SourceTaskState::Started {
            upstream_server: Arc::new(Mutex::new(upstream_server)),
        };
        Ok(())
    }

    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<()> {
        debug_assert!(!context.downstream_tasks().is_empty());

        let row = self.collect_next::<DI>()?;
        context
            .row_repository()
            .emit_owned(row, &context.downstream_tasks())
    }

    fn collect_next<DI: DependencyInjection>(&self) -> Result<Row> {
        if let SourceTaskState::Started {
            ref upstream_server,
        } = self.state
        {
            let foreign_row = upstream_server
                .lock()
                .expect("other worker threads sharing the same server must not get panic")
                .next_row()?;
            foreign_row.into_row::<DI>(self.downstream.shape())
        } else {
            unreachable!()
        }
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
    fn test_source_task() -> Result<()> {
        let j1 = JsonObject::fx_city_temperature_tokyo();
        let j2 = JsonObject::fx_city_temperature_osaka();
        let j3 = JsonObject::fx_city_temperature_london();

        let test_source = TestSource::start(vec![j1, j2, j3])?;

        let stream = Arc::new(ForeignStreamModel::fx_city_temperature_source());
        let server = ServerModel::fx_net_source(stream, test_source.host_ip(), test_source.port());

        let mut source = SourceTask::new(server);
        source.start()?;

        assert_eq!(
            source.collect_next::<TestDI>()?,
            Row::fx_city_temperature_tokyo()
        );
        assert_eq!(
            source.collect_next::<TestDI>()?,
            Row::fx_city_temperature_osaka()
        );
        assert_eq!(
            source.collect_next::<TestDI>()?,
            Row::fx_city_temperature_london()
        );
        assert!(matches!(
            source.collect_next::<TestDI>().unwrap_err(),
            SpringError::ForeignSourceTimeout { .. }
        ));

        Ok(())
    }
}
