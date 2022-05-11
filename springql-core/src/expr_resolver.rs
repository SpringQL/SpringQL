// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(crate) mod expr_label;

use crate::error::{Result, SpringError};
use crate::expression::{AggrExpr, ValueExpr};
use crate::pipeline::name::{AggrAlias, ValueAlias};
use crate::sql_processor::sql_parser::syntax::SelectFieldSyntax;
use crate::stream_engine::{SqlValue, Tuple};
use anyhow::anyhow;
use std::collections::HashMap;

use self::expr_label::{AggrExprLabel, ExprLabel, ExprLabelGenerator, ValueExprLabel};

/// ExprResolver is to:
///
/// 1. register ValueExpr / AggrExpr in select_list with their (optional) alias and get new ExprLabel.
/// 2. resolve alias in ValueExprOrAlias / AggrExprAlias and get existing ExprLabel.
/// 3. evaluate expression into SqlValue from ExprLabel.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ExprResolver {
    label_gen: ExprLabelGenerator,

    value_expressions: HashMap<ValueExprLabel, ValueExpr>,
    value_aliased_labels: HashMap<ValueAlias, ValueExprLabel>,

    aggr_expressions: HashMap<AggrExprLabel, AggrExpr>,
    aggr_aliased_labels: HashMap<AggrAlias, AggrExprLabel>,
    aggr_expression_results: HashMap<AggrExprLabel, SqlValue>,
}

impl ExprResolver {
    /// # Returns
    ///
    /// `(instance, value/aggr expr labels in select_list)
    pub(crate) fn new(select_list: Vec<SelectFieldSyntax>) -> (Self, Vec<ExprLabel>) {
        let mut label_gen = ExprLabelGenerator::default();
        let mut value_expressions = HashMap::new();
        let mut value_aliased_labels = HashMap::new();
        let mut aggr_expressions = HashMap::new();
        let mut aggr_aliased_labels = HashMap::new();

        let expr_labels = select_list
            .into_iter()
            .map(|select_field| match select_field {
                SelectFieldSyntax::ValueExpr { value_expr, alias } => {
                    let label = label_gen.next_value();
                    value_expressions.insert(label, value_expr);
                    if let Some(alias) = alias {
                        value_aliased_labels.insert(alias, label);
                    }
                    ExprLabel::Value(label)
                }
                SelectFieldSyntax::AggrExpr { aggr_expr, alias } => {
                    let label = label_gen.next_aggr();
                    aggr_expressions.insert(label, aggr_expr);
                    if let Some(alias) = alias {
                        aggr_aliased_labels.insert(alias, label);
                    }
                    ExprLabel::Aggr(label)
                }
            })
            .collect();

        (
            Self {
                label_gen,
                value_expressions,
                value_aliased_labels,
                aggr_expressions,
                aggr_aliased_labels,
                aggr_expression_results: HashMap::new(),
            },
            expr_labels,
        )
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` if alias is not in select_list.
    pub(crate) fn resolve_value_alias(&self, value_alias: ValueAlias) -> Result<ValueExprLabel> {
        self.value_aliased_labels
            .get(&value_alias)
            .cloned()
            .ok_or_else(|| {
                SpringError::Sql(anyhow!(
                    "Value alias `{}` is not in select list.",
                    value_alias
                ))
            })
    }

    /// TODO use in HAVING clause
    ///
    /// # Failures
    ///
    /// - `SpringError::Sql` if alias is not in select_list.
    #[allow(dead_code)]
    pub(crate) fn resolve_aggr_alias(&self, aggr_alias: AggrAlias) -> Result<AggrExprLabel> {
        self.aggr_aliased_labels
            .get(&aggr_alias)
            .cloned()
            .ok_or_else(|| {
                SpringError::Sql(anyhow!(
                    "Aggr alias `{}` is not in select list.",
                    aggr_alias
                ))
            })
    }

    /// # Panics
    ///
    /// -  `label` is not found
    pub(crate) fn resolve_aggr_expr(&self, label: AggrExprLabel) -> AggrExpr {
        self.aggr_expressions
            .get(&label)
            .cloned()
            .unwrap_or_else(|| panic!("label {:?} not found", label))
    }

    /// Register value expression which is not in select_list
    pub(crate) fn register_value_expr(&mut self, value_expr: ValueExpr) -> ValueExprLabel {
        let label = self.label_gen.next_value();
        self.value_expressions.insert(label, value_expr);
        label
    }

    /// TODO use in HAVING clause
    ///
    /// Register aggregate expression which is not in select_list
    #[allow(dead_code)]
    pub(crate) fn register_aggr_expr(&mut self, aggr_expr: AggrExpr) -> AggrExprLabel {
        let label = self.label_gen.next_aggr();
        self.aggr_expressions.insert(label, aggr_expr);
        label
    }

    /// label -> (internal) value expression + tuple (for ColumnReference) -> SqlValue.
    ///
    /// # Panics
    ///
    /// -  `label` is not found
    ///
    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - column reference in expression is not found in `tuple`.
    ///   - somehow failed to eval expression.
    pub(crate) fn eval_value_expr(&self, label: ValueExprLabel, tuple: &Tuple) -> Result<SqlValue> {
        let value_expr = self
            .value_expressions
            .get(&label)
            .cloned()
            .unwrap_or_else(|| panic!("label {:?} not found", label));

        let value_expr_ph2 = value_expr.resolve_colref(tuple)?;
        value_expr_ph2.eval()
    }

    /// label -> (internal) value expression inside aggr expr + tuple (for ColumnReference) -> SqlValue.
    ///
    /// _inner_ means: AGGR_FUNC(inner_value_expr)
    ///
    /// # Panics
    ///
    /// -  `label` is not found
    ///
    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   - column reference in expression is not found in `tuple`.
    ///   - somehow failed to eval expression.
    pub(crate) fn eval_aggr_expr_inner(
        &self,
        label: AggrExprLabel,
        tuple: &Tuple,
    ) -> Result<SqlValue> {
        let aggr_expr = self.resolve_aggr_expr(label);
        let value_expr = aggr_expr.aggregated;
        let value_expr_ph2 = value_expr.resolve_colref(tuple)?;
        value_expr_ph2.eval()
    }
}

#[cfg(test)]
mod tests {
    use crate::{expression::ValueExpr, stream_engine::time::timestamp::SpringTimestamp};

    use super::*;

    #[test]
    fn test_expr_resolver() {
        let select_list = vec![
            SelectFieldSyntax::ValueExpr {
                value_expr: ValueExpr::factory_add(
                    ValueExpr::factory_integer(1),
                    ValueExpr::factory_integer(1),
                ),
                alias: None,
            },
            SelectFieldSyntax::ValueExpr {
                value_expr: ValueExpr::factory_add(
                    ValueExpr::factory_integer(2),
                    ValueExpr::factory_integer(2),
                ),
                alias: Some(ValueAlias::new("a1".to_string())),
            },
        ];

        let (mut resolver, labels_select_list) = ExprResolver::new(select_list);

        if let &[ExprLabel::Value(value_label0), ExprLabel::Value(value_label1)] =
            &labels_select_list[..]
        {
            assert_eq!(
                resolver
                    .resolve_value_alias(ValueAlias::new("a1".to_string()))
                    .unwrap(),
                value_label1
            );
            assert!(resolver
                .resolve_value_alias(ValueAlias::new("a404".to_string()))
                .is_err(),);

            let label = resolver.register_value_expr(ValueExpr::factory_add(
                ValueExpr::factory_integer(3),
                ValueExpr::factory_integer(3),
            ));

            let empty_tuple = Tuple::new(SpringTimestamp::fx_ts1(), vec![]);

            assert_eq!(
                resolver
                    .eval_value_expr(value_label0, &empty_tuple)
                    .unwrap(),
                SqlValue::factory_integer(2)
            );
            assert_eq!(
                resolver
                    .eval_value_expr(value_label0, &empty_tuple)
                    .unwrap(),
                SqlValue::factory_integer(2),
                "eval twice"
            );
            assert_eq!(
                resolver
                    .eval_value_expr(value_label1, &empty_tuple)
                    .unwrap(),
                SqlValue::factory_integer(4)
            );
            assert_eq!(
                resolver.eval_value_expr(label, &empty_tuple).unwrap(),
                SqlValue::factory_integer(6)
            );
        } else {
            unreachable!()
        }
    }
}
