use std::fmt::Debug;
use std::sync::Arc;

use crate::error::Result;
use crate::pipeline::foreign_stream_model::ForeignStreamModel;
use crate::pipeline::name::ServerName;
use crate::pipeline::pipeline_graph::PipelineGraph;
use crate::pipeline::server_model::ServerModel;
use crate::stream_engine::autonomous_executor::row::Row;
use crate::stream_engine::autonomous_executor::RowRepository;
use crate::stream_engine::dependency_injection::DependencyInjection;

use super::task_context::TaskContext;
use super::task_id::TaskId;
use super::task_state::TaskState;

#[derive(Debug)]
pub(crate) struct SourceTask {
    id: TaskId,
    state: TaskState,
    server_name: ServerName,
    downstream: Arc<ForeignStreamModel>,
}

impl SourceTask {
    pub(in crate::stream_engine) fn new(
        server_model: &ServerModel,
        pipeline_graph: &PipelineGraph,
    ) -> Self {
        let id = TaskId::from_source_server(server_model.serving_foreign_stream().name().clone());
        let downstream = server_model.serving_foreign_stream();
        Self {
            id,
            state: TaskState::from(&server_model.state(pipeline_graph)),
            server_name: server_model.name().clone(),
            downstream,
        }
    }

    pub(in crate::stream_engine) fn id(&self) -> &TaskId {
        &self.id
    }

    pub(in crate::stream_engine) fn state(&self) -> &TaskState {
        &self.state
    }

    pub(in crate::stream_engine::autonomous_executor) fn run<DI: DependencyInjection>(
        &self,
        context: &TaskContext<DI>,
    ) -> Result<()> {
        debug_assert!(!context.downstream_tasks().is_empty());

        let row = self.collect_next::<DI>(context)?;
        context
            .row_repository()
            .emit(row, &context.downstream_tasks())
    }

    fn collect_next<DI: DependencyInjection>(&self, context: &TaskContext<DI>) -> Result<Row> {
        let source_server = context
            .server_repository()
            .get_source_server(&self.server_name);

        let foreign_row = source_server
            .lock()
            .expect("other worker threads sharing the same server must not get panic")
            .next_row()?;
        foreign_row.into_row::<DI>(self.downstream.shape())
    }
}
