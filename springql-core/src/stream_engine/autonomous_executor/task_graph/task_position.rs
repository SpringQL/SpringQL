#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub(in crate::stream_engine::autonomous_executor) enum TaskPosition {
    Source,
    Intermediate,
    Sink,
}
