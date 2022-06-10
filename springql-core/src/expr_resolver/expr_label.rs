// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct ExprLabelGenerator {
    value: u16,
    aggr: u16,
}

impl ExprLabelGenerator {
    pub fn next_value(&mut self) -> ValueExprLabel {
        let label = ValueExprLabel(self.value);
        self.value += 1;
        label
    }

    pub fn next_aggr(&mut self) -> AggrExprLabel {
        let label = AggrExprLabel(self.aggr);
        self.aggr += 1;
        label
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ValueExprLabel(u16);

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct AggrExprLabel(u16);

/// Either ValueExprLabel or AggrExprLabel.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ExprLabel {
    Value(ValueExprLabel),
    Aggr(AggrExprLabel),
}
