// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::mem_size::MemSize;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct StreamName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct PumpName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct SourceReaderName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct SinkWriterName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct ColumnName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct QueueName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct CorrelationAlias(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct AttributeName(String);

/// Alias to an value expression.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct ValueAlias(String);

/// Alias to an aggregate expression.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct AggrAlias(String);

impl Display for StreamName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for PumpName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for SourceReaderName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for SinkWriterName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for ColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for QueueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for CorrelationAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for AttributeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for ValueAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for AggrAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for StreamName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for ColumnName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for PumpName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for SourceReaderName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for SinkWriterName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for AttributeName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for CorrelationAlias {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for ValueAlias {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for AggrAlias {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl ColumnName {
    pub(crate) fn arrival_rowtime() -> Self {
        Self::new("ROWTIME".to_string())
    }
}

impl StreamName {
    pub(crate) fn virtual_root() -> Self {
        Self::new("__st_virtual_root__".to_string())
    }
}

impl SinkWriterName {
    pub(crate) fn net_sink() -> Self {
        Self::new("NET_CLIENT_SINK".to_string())
    }

    pub(crate) fn in_memory_queue_sink() -> Self {
        Self::new("IN_MEMORY_QUEUE_SERVER_SINK".to_string())
    }
}

impl MemSize for StreamName {
    fn mem_size(&self) -> usize {
        self.0.capacity()
    }
}
impl MemSize for ColumnName {
    fn mem_size(&self) -> usize {
        self.0.capacity()
    }
}
