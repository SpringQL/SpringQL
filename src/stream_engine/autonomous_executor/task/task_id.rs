#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, new)]
pub(in crate::stream_engine) struct TaskId(String);
