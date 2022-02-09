pub(crate) mod aggregate;

use self::aggregate::GroupAggregateParameter;

/// Window operation parameters
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum WindowOperationParameter {
    GroupAggregation(GroupAggregateParameter),
}
