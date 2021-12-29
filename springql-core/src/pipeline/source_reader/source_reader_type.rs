// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use serde::{Deserialize, Serialize};

use crate::pipeline::name::SourceReaderName;

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) enum SourceReaderType {
    Net,
}

impl From<&SourceReaderType> for SourceReaderName {
    fn from(source_reader_type: &SourceReaderType) -> Self {
        match source_reader_type {
            SourceReaderType::Net => SourceReaderName::net_source(),
        }
    }
}
