use std::{collections::VecDeque, rc::Rc};

use crate::stream_engine::executor::data::row::Row;

/// Note: RowWindow is a temporal structure during query execution (cannot be a pump output).
#[derive(Clone, Debug, Default, new)]
pub(in crate::stream_engine::executor) struct RowWindow(VecDeque<Rc<Row>>);

impl RowWindow {
    pub(in crate::stream_engine::executor) fn inner(&self) -> &VecDeque<Rc<Row>> {
        &self.0
    }
}
