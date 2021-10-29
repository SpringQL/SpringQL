use crate::error::Result;

#[derive(Eq, PartialEq, Debug)]
pub(super) enum Task {}

impl Task {
    pub(super) fn run(&self) -> Result<()> {
        todo!()
    }
}
