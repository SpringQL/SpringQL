#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum TaskState {
    Stopped,
    Started,
}
