// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use serde::{Deserialize, Serialize};

use crate::pipeline::name::SourceReaderName;

/// See: <https://docs.sqlstream.com/sql-reference-guide/create-statements/createserver/#prebuilt-server-objects-available-in-sserver>
#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum SourceReaderType {
    Net,
}

impl From<&SourceReaderType> for SourceReaderName {
    fn from(server_type: &SourceReaderType) -> Self {
        match server_type {
            SourceReaderType::Net => SourceReaderName::net_source(),
        }
    }
}
