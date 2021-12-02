// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use std::collections::VecDeque;

use super::interm_row::PreservedRow;

/// Note: RowWindow is a temporal structure during query task::subtaskution (cannot be a pump output).
#[derive(Debug, Default, new)]
pub(in crate::stream_engine::autonomous_executor) struct RowWindow(VecDeque<PreservedRow>);

impl RowWindow {
    #[cfg(test)] // TODO remove
    pub(in crate::stream_engine::autonomous_executor) fn inner(&self) -> &VecDeque<PreservedRow> {
        &self.0
    }
}
