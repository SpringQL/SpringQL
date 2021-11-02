use std::sync::{Arc, Mutex};

use super::{task_context::TaskContext, task_id::TaskId};
use crate::error::Result;
use crate::stream_engine::autonomous_executor::server::sink::net::NetSinkServerStandby;
use crate::stream_engine::autonomous_executor::server::sink::{
    SinkServerActive, SinkServerStandby,
};
use crate::stream_engine::pipeline::server_model::server_type::ServerType;
use crate::stream_engine::{
    autonomous_executor::{
        data::{foreign_row::foreign_sink_row::ForeignSinkRow, row::Row},
        RowRepository,
    },
    dependency_injection::DependencyInjection,
    pipeline::server_model::ServerModel,
};

#[derive(Debug)]
pub(crate) struct SinkTask {
    id: TaskId,

    /// 1 server can be shared to 2 or more foreign streams.
    downstream_server: Arc<Mutex<Box<dyn SinkServerActive>>>,
}

impl SinkTask {
    pub(in crate::stream_engine) fn new(server: &ServerModel) -> Result<Self> {
        let id = TaskId::from_sink_server(server.serving_foreign_stream().name().clone());
        let downstream_server = match server.server_type() {
            ServerType::SinkNet => {
                let server_standby = NetSinkServerStandby::new(server.options())?;
                let server_active = server_standby.start()?;
                Box::new(server_active)
            }
            ServerType::SourceNet => unreachable!("source type server ({:?}) for SinkTask", server),
        };
        Ok(Self {
            id,
            downstream_server: Arc::new(Mutex::new(downstream_server)),
        })
    }

    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<()> {
        let row_repo = context.row_repository();

        let row = row_repo.collect_next(&context.task())?;
        let row = row.fixme_clone();  // Ahhhhhhhhhhhhhh

        self.emit(row)
    }

    fn emit(&self, row: Row) -> Result<()> {
        let f_row = ForeignSinkRow::from(row);
        self.downstream_server
            .lock()
            .expect("other worker threads sharing the same sink server must not get panic")
            .send_row(f_row)
    }
}
