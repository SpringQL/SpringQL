pub(in crate::stream_engine) mod server_state;
pub(in crate::stream_engine) mod server_type;

use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use self::{server_state::ServerState, server_type::ServerType};

use crate::model::{
    name::{ServerName, StreamName},
    option::Options,
};

use super::{foreign_stream_model::ForeignStreamModel, stream_model::StreamModel};

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub(crate) struct ServerModel {
    name: ServerName,
    state: ServerState,
    server_type: ServerType,
    serving_foreign_stream: Arc<ForeignStreamModel>,
    options: Options,
}

impl ServerModel {
    pub(in crate::stream_engine) fn new(
        state: ServerState,
        server_type: ServerType,
        serving_foreign_stream: Arc<ForeignStreamModel>,
        options: Options,
    ) -> Self {
        Self {
            name: ServerName::from(&server_type),
            state,
            server_type,
            serving_foreign_stream,
            options,
        }
    }

    pub(in crate::stream_engine) fn name(&self) -> &ServerName {
        &self.name
    }

    pub(in crate::stream_engine) fn state(&self) -> &ServerState {
        &self.state
    }

    pub(in crate::stream_engine) fn server_type(&self) -> &ServerType {
        &self.server_type
    }

    pub(in crate::stream_engine) fn serving_foreign_stream(&self) -> Arc<ForeignStreamModel> {
        self.serving_foreign_stream.clone()
    }

    pub(in crate::stream_engine) fn options(&self) -> &Options {
        &self.options
    }
}
