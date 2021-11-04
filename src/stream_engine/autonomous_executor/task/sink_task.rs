use super::task_state::TaskState;
use super::{task_context::TaskContext, task_id::TaskId};
use crate::error::Result;
use crate::pipeline::name::ServerName;
use crate::pipeline::server_model::ServerModel;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::{
    autonomous_executor::{row::foreign_row::foreign_sink_row::ForeignSinkRow, RowRepository},
    dependency_injection::DependencyInjection,
};

#[derive(Debug)]
pub(crate) struct SinkTask {
    id: TaskId,
    server_name: ServerName,
}

impl SinkTask {
    pub(in crate::stream_engine) fn new(server_model: &ServerModel) -> Self {
        let id = TaskId::from_sink_server(server_model.serving_foreign_stream().name().clone());
        Self {
            id,
            server_name: server_model.name().clone(),
        }
    }

    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine) fn state(&self) -> TaskState {
        // server is always STARTED (not necessarily scheduled until all upstream pumps get STARTED)
        TaskState::Started
    }

    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<()> {
        let row_repo = context.row_repository();

        let row = row_repo.collect_next(&context.task())?;
        let row = row.fixme_clone(); // Ahhhhhhhhhhhhhh

        self.emit::<DI>(row, context)
    }

    fn emit<DI: DependencyInjection>(&self, row: Row, context: &TaskContext<DI>) -> Result<()> {
        let f_row = ForeignSinkRow::from(row);

        let sink_server = context
            .server_repository()
            .get_sink_server(&self.server_name);

        sink_server
            .lock()
            .expect("other worker threads sharing the same sink server must not get panic")
            .send_row(f_row)?;

        Ok(())
    }
}
