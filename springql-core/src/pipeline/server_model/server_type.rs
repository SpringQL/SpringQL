use serde::{Deserialize, Serialize};

use crate::pipeline::name::ServerName;

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum ServerType {
    SourceNet,
    SinkNet,

    SinkInMemoryQueue,
}

impl From<&ServerType> for ServerName {
    fn from(server_type: &ServerType) -> Self {
        match server_type {
            ServerType::SourceNet => ServerName::net_source(),
            ServerType::SinkNet => ServerName::net_sink(),
            ServerType::SinkInMemoryQueue => ServerName::in_memory_queue_sink(),
        }
    }
}

impl ServerType {
    pub(crate) fn is_source(&self) -> bool {
        match self {
            ServerType::SourceNet => true,
            ServerType::SinkNet | ServerType::SinkInMemoryQueue => false,
        }
    }

    pub(crate) fn is_sink(&self) -> bool {
        !self.is_source()
    }
}
