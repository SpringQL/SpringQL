#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub(crate) struct ExprLabelGenerator(u16);

impl ExprLabelGenerator {
    pub(crate) fn next(&mut self) -> ExprLabel {
        let label = ExprLabel(self.0);
        self.0 += 1;
        label
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub(crate) struct ExprLabel(u16);
