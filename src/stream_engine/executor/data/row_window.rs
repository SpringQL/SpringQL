use std::collections::VecDeque;

use super::row::repository::RowRef;

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct RowWindow(VecDeque<RowRef>);

impl Iterator for RowWindow {
    type Item = RowRef;

    /// FIFO row.
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_back()
    }
}
