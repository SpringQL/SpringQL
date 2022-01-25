use crate::pipeline::name::{CorrelationAlias, CorrelationName};

/// Correlation name with/without an alias.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(crate) struct AliasedCorrelationName {
    pub(crate) correlation_name: CorrelationName,
    pub(crate) correlation_alias: Option<CorrelationAlias>,
}
