// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize, Deserialize)]
pub(crate) struct PipelineVersion(u64);

impl PipelineVersion {
    pub(crate) fn new() -> Self {
        Self(1)
    }

    pub(crate) fn up(&mut self) {
        self.0 += 1;
    }
}
