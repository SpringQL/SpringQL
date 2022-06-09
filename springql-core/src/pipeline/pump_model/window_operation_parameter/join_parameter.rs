// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    expr_resolver::expr_label::ValueExprLabel, pipeline::field::field_name::ColumnReference,
};

/// TODO `support complex expression with aggregations`
///
/// ```sql
/// SELECT s.c1, t.c2
///   FROM s
///   LEFT OUTER JOIN t
///   ON s.c1 = t.c1
///   SLIDING WINDOW ...;
/// ```
#[derive(Clone, PartialEq, Debug, new)]
pub(crate) struct JoinParameter {
    pub(crate) join_type: JoinType,

    /// Tuples from left must have the same shape.
    pub(crate) left_colrefs: Vec<ColumnReference>,
    pub(crate) right_colrefs: Vec<ColumnReference>,

    pub(crate) on_expr: ValueExprLabel,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum JoinType {
    LeftOuter,
}
