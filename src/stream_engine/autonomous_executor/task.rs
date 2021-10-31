pub(super) mod task_id;

pub(super) mod task_graph;

use crate::error::Result;

use self::task_id::TaskId;

#[derive(Debug)]
pub(in crate::stream_engine) enum Task {}

impl Task {
    pub(super) fn id(&self) -> TaskId {
        todo!()
    }

    pub(super) fn run(&self) -> Result<()> {
        todo!()
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for Task {}
