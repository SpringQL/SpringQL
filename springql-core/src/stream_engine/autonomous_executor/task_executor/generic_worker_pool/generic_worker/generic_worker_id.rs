// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::fmt::Display;

#[derive(Copy, Clone, Eq, PartialEq, Debug, new)]
pub(in crate::stream_engine::autonomous_executor) struct GenericWorkerId(u16);

impl Display for GenericWorkerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
