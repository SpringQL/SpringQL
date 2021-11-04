pub(crate) mod server_state;
pub(crate) mod server_type;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::{server_state::ServerState, server_type::ServerType};

use crate::model::name::ServerName;

use super::{
    foreign_stream_model::ForeignStreamModel, option::Options, pipeline_graph::PipelineGraph,
};

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(crate) struct ServerModel {
    name: ServerName,
    server_type: ServerType,
    serving_foreign_stream: Arc<ForeignStreamModel>,
    options: Options,
}

impl ServerModel {
    pub(crate) fn new(
        server_type: ServerType,
        serving_foreign_stream: Arc<ForeignStreamModel>,
        options: Options,
    ) -> Self {
        Self {
            name: ServerName::from(&server_type),
            server_type,
            serving_foreign_stream,
            options,
        }
    }

    pub(crate) fn name(&self) -> &ServerName {
        &self.name
    }

    pub(crate) fn state(&self, pipeline_graph: &PipelineGraph) -> ServerState {
        pipeline_graph.source_server_state(self.serving_foreign_stream.name())
    }

    pub(crate) fn server_type(&self) -> &ServerType {
        &self.server_type
    }

    pub(crate) fn serving_foreign_stream(&self) -> Arc<ForeignStreamModel> {
        self.serving_foreign_stream.clone()
    }

    pub(crate) fn options(&self) -> &Options {
        &self.options
    }
}
