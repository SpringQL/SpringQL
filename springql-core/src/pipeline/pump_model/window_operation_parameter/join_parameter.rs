// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::{
    expr_resolver::expr_label::ValueExprLabel, pipeline::field::field_name::ColumnReference,
};

/// TODO [support complex expression with aggregations](https://gh01.base.toyota-tokyo.tech/SpringQL-internal/SpringQL/issues/152)
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
