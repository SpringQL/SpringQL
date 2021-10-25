use std::{collections::VecDeque, rc::Rc};

use super::row::Row;

#[derive(Clone, Debug, Default)]
pub(in crate::stream_engine::executor) struct RowWindow(VecDeque<Rc<Row>>);

impl Iterator for RowWindow {
    type Item = Rc<Row>;

    /// FIFO row.
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_back()
    }
}
