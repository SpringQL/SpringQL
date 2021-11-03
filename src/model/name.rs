use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct StreamName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct PumpName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct ServerName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub(crate) struct ColumnName(String);

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
impl Display for ServerName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Display for ColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for StreamName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for PumpName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for ServerName {
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

    pub(crate) fn virtual_leaf(sink_foreign_stream: StreamName) -> Self {
        Self::new(format!("__st_virtual_leaf__{}__", sink_foreign_stream))
    }
}

impl ServerName {
    pub(crate) fn net_source() -> Self {
        Self::new("NET_SERVER_SOURCE".to_string())
    }
    pub(crate) fn net_sink() -> Self {
        Self::new("NET_SERVER_SINK".to_string())
    }
}
