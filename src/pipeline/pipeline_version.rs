use serde::{Deserialize, Serialize};

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Serialize, Deserialize,
)]
pub(crate) struct PipelineVersion(u64);

impl PipelineVersion {
    pub(crate) fn up(&mut self) {
        self.0 += 1;
    }
}
