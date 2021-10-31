pub(in crate::stream_engine) mod server_type;

use serde::{Deserialize, Serialize};

use crate::model::{name::StreamName, option::Options};

use self::server_type::ServerType;

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(in crate::stream_engine) struct ServerModel {
    server_type: ServerType,
    serving_foreign_stream: StreamName,
    options: Options,
}

impl ServerModel {
    pub(in crate::stream_engine) fn server_type(&self) -> &ServerType {
        &self.server_type
    }

    pub(in crate::stream_engine) fn serving_foreign_stream(&self) -> &StreamName {
        &self.serving_foreign_stream
    }
}
