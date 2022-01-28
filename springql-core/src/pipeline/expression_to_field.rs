use crate::expression::Expression;

use super::name::FieldAlias;

/// ```sql
/// SELECT expression AS alias
/// ```
#[derive(Clone, PartialEq, Debug, new)]
pub(crate) struct ExpressionToField {
    pub(crate) expression: Expression,
    pub(crate) alias: Option<FieldAlias>,
}
