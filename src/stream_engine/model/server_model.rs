pub(in crate::stream_engine) mod server_type;

use serde::{Deserialize, Serialize};

use self::server_type::ServerType;

use super::option::Options;

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Debug, Serialize, Deserialize, new)]
pub(in crate::stream_engine) struct ServerModel {
    server_type: ServerType,
    options: Options,
}
