use std::{
    collections::{vec_deque, VecDeque},
    rc::Rc,
};

use super::row::Row;

#[derive(Clone, Debug, Default, new)]
pub(in crate::stream_engine::executor) struct RowWindow(VecDeque<Rc<Row>>);

impl RowWindow {
    pub(in crate::stream_engine::executor) fn inner(&self) -> &VecDeque<Rc<Row>> {
        &self.0
    }
}
