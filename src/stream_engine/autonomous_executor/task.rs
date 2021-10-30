use crate::error::Result;

#[derive(Eq, PartialEq, Debug)]
pub(in crate::stream_engine) enum Task {}

impl Task {
    pub(super) fn run(&self) -> Result<()> {
        todo!()
    }
}
