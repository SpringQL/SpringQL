use serde::{Deserialize, Serialize};

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum ServerType {
    SourceNet,
    SinkNet,
}
