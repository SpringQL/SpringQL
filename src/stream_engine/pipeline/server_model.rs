pub(in crate::stream_engine) mod server_type;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::model::{name::StreamName, option::Options};

use self::server_type::ServerType;

use super::{foreign_stream_model::ForeignStreamModel, stream_model::StreamModel};

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize, new)]
pub(crate) struct ServerModel {
    server_type: ServerType,
    serving_foreign_stream: Arc<ForeignStreamModel>,
    options: Options,
}

impl ServerModel {
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
