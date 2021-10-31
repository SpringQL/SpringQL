use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct StreamName(String);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize, new)]
pub struct PumpName(String);

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

impl ColumnName {
    pub(crate) fn arrival_rowtime() -> Self {
        Self::new("ROWTIME".to_string())
    }
}
