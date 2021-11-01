use serde::{Deserialize, Serialize};

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
pub(in crate::stream_engine) struct PipelineVersion(u64);

impl PipelineVersion {
    pub(in crate::stream_engine) fn up(&mut self) {
        self.0 += 1;
    }
}
