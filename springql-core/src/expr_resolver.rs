pub(crate) mod expr_label;

use crate::error::{Result, SpringError};
use crate::expression::{AggrExpr, ValueExpr};
use crate::pipeline::name::{AggrAlias, ValueAlias};
use crate::sql_processor::sql_parser::syntax::SelectFieldSyntax;
use crate::stream_engine::{SqlValue, Tuple};
use anyhow::anyhow;
use std::collections::HashMap;

use self::expr_label::{ExprLabel, ExprLabelGenerator};

/// ExprResolver is to:
///
/// 1. register ValueExpr / AggrExpr in select_list with their (optional) alias and get new ExprLabel.
/// 2. resolve alias in ValueExprOrAlias / AggrExprAlias and get existing ExprLabel.
/// 3. evaluate expression into SqlValue from ExprLabel.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct ExprResolver {
    label_gen: ExprLabelGenerator,

    value_expressions: HashMap<ExprLabel, ValueExpr>,
    value_aliased_labels: HashMap<ValueAlias, ExprLabel>,

    aggr_expressions: HashMap<ExprLabel, AggrExpr>,
    aggr_aliased_labels: HashMap<AggrAlias, ExprLabel>,
}

impl ExprResolver {
    /// # Returns
    ///
    /// `(instance, labels in select_list)
    pub(crate) fn new(select_list: Vec<SelectFieldSyntax>) -> (Self, Vec<ExprLabel>) {
        let mut label_gen = ExprLabelGenerator::default();
        let mut value_expressions = HashMap::new();
        let mut value_aliased_labels = HashMap::new();
        let mut aggr_expressions = HashMap::new();
        let mut aggr_aliased_labels = HashMap::new();

        let labels = select_list
            .into_iter()
            .map(|select_field| {
                let label = label_gen.next();

                match select_field {
                    SelectFieldSyntax::ValueExpr { value_expr, alias } => {
                        value_expressions.insert(label, value_expr);
                        if let Some(alias) = alias {
                            value_aliased_labels.insert(alias, label);
                        }
                    }
                    SelectFieldSyntax::AggrExpr { aggr_expr, alias } => {
                        aggr_expressions.insert(label, aggr_expr);
                        if let Some(alias) = alias {
                            aggr_aliased_labels.insert(alias, label);
                        }
                    }
                }

                label
            })
            .collect();

        (
            Self {
                label_gen,
                value_expressions,
                value_aliased_labels,
                aggr_expressions,
                aggr_aliased_labels,
            },
            labels,
        )
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` if alias is not in select_list.
    pub(crate) fn resolve_value_alias(&self, value_alias: ValueAlias) -> Result<ExprLabel> {
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

    /// # Failures
    ///
    /// - `SpringError::Sql` if alias is not in select_list.
    pub(crate) fn resolve_aggr_alias(&self, aggr_alias: AggrAlias) -> Result<ExprLabel> {
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

    /// Register value expression which is not in select_list
    pub(crate) fn register_value_expr(&mut self, value_expr: ValueExpr) -> ExprLabel {
        let label = self.label_gen.next();
        self.value_expressions.insert(label, value_expr);
        label
    }

    /// Register aggregate expression which is not in select_list
    pub(crate) fn register_aggr_expr(&mut self, aggr_expr: AggrExpr) -> ExprLabel {
        let label = self.label_gen.next();
        self.aggr_expressions.insert(label, aggr_expr);
        label
    }

    /// label -> (internal) expression + tuple (for ColumnReference) -> SqlValue.
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
    pub(crate) fn eval(&self, label: ExprLabel, tuple: &Tuple) -> Result<SqlValue> {
        let value_expr = self
            .value_expressions
            .get(&label)
            .cloned()
            .unwrap_or_else(|| panic!("label {:?} not found", label));

        let value_expr_ph2 = value_expr.resolve_colref(tuple)?;
        value_expr_ph2.eval()
    }
}

#[cfg(test)]
mod tests {
    use crate::{expression::ValueExpr, stream_engine::time::timestamp::Timestamp};

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

        assert_eq!(
            resolver
                .resolve_value_alias(ValueAlias::new("a1".to_string()))
                .unwrap(),
            labels_select_list[1]
        );
        assert!(resolver
            .resolve_value_alias(ValueAlias::new("a404".to_string()))
            .is_err(),);

        let label = resolver.register_value_expr(ValueExpr::factory_add(
            ValueExpr::factory_integer(3),
            ValueExpr::factory_integer(3),
        ));

        let empty_tuple = Tuple::new(Timestamp::fx_ts1(), vec![]);

        assert_eq!(
            resolver.eval(labels_select_list[0], &empty_tuple).unwrap(),
            SqlValue::factory_integer(2)
        );
        assert_eq!(
            resolver.eval(labels_select_list[0], &empty_tuple).unwrap(),
            SqlValue::factory_integer(2),
            "eval twice"
        );
        assert_eq!(
            resolver.eval(labels_select_list[1], &empty_tuple).unwrap(),
            SqlValue::factory_integer(4)
        );
        assert_eq!(
            resolver.eval(label, &empty_tuple).unwrap(),
            SqlValue::factory_integer(6)
        );
    }
}
