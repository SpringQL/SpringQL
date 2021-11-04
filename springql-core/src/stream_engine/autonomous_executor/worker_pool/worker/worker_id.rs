use std::fmt::Display;

#[derive(Copy, Clone, Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct WorkerId(u16);

impl Display for WorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
