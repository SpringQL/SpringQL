pub(crate) mod expr_label;

use crate::error::Result;
use crate::expression::ValueExpr;
use crate::pipeline::name::ValueAlias;
use crate::sql_processor::sql_parser::syntax::SelectFieldSyntax;
use crate::stream_engine::{SqlValue, Tuple};

use self::expr_label::{ExprLabel, ExprLabelGenerator};

/// ExprResolver is to:
///
/// 1. register ValueExpr / AggrExpr in select_list with their (optional) alias and get new ExprLabel.
/// 2. resolve alias in ValueExprOrAlias / AggrExprAlias and get existing ExprLabel.
/// 3. evaluate expression or get cached SqlValue from ExprLabel.
#[derive(Debug)]
pub(crate) struct ExprResolver {
    label_gen: ExprLabelGenerator,
}

impl ExprResolver {
    /// # Returns
    ///
    /// `(instance, labels in select_list)
    pub(crate) fn new(select_list: Vec<SelectFieldSyntax>) -> (Self, Vec<ExprLabel>) {
        unimplemented!()
    }

    /// # Failures
    ///
    /// - `SpringError::Sql` if alias is not in select_list.
    pub(crate) fn resolve_value_alias(&mut self, value_alias: ValueAlias) -> Result<ExprLabel> {
        unimplemented!()
    }

    /// Register expression which is not in select_list
    pub(crate) fn register(&mut self, value_expr: ValueExpr) -> ExprLabel {
        unimplemented!()
    }

    /// label -> (internal) expression + tuple (for ColumnReference) -> SqlValue.
    ///
    /// SqlValue is cached and not calculated twice.
    ///
    /// # Failures
    ///
    /// - `SpringError::Sql` when:
    ///   -  `label` is alias is not found
    ///   - column reference in expression is not found in `tuple`.
    ///   - somehow failed to eval expression.
    pub(crate) fn eval(&mut self, label: ExprLabel, tuple: &Tuple) -> Result<SqlValue> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{expression::ValueExpr, stream_engine::time::timestamp::Timestamp};

    use super::*;

    #[test]
    fn test_expr_resolver() {
        let select_list = vec![
            SelectFieldSyntax {
                value_expr: ValueExpr::factory_add(
                    ValueExpr::factory_integer(1),
                    ValueExpr::factory_integer(1),
                ),
                alias: None,
            },
            SelectFieldSyntax {
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

        let label = resolver.register(ValueExpr::factory_add(
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
