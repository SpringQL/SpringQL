// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{expr_resolver::ValueExprLabel, pipeline::field::ColumnReference};

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
pub struct JoinParameter {
    pub join_type: JoinType,

    /// Tuples from left must have the same shape.
    pub left_colrefs: Vec<ColumnReference>,
    pub right_colrefs: Vec<ColumnReference>,

    pub on_expr: ValueExprLabel,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum JoinType {
    LeftOuter,
}
