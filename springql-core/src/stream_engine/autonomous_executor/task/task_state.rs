// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::pipeline::{
    pump_model::pump_state::PumpState, source_reader_model::source_reader_state::SourceReaderState,
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum TaskState {
    Stopped,
    Started,
}

impl From<&PumpState> for TaskState {
    fn from(ps: &PumpState) -> Self {
        match ps {
            PumpState::Stopped => Self::Stopped,
            PumpState::Started => Self::Started,
        }
    }
}

impl From<&SourceReaderState> for TaskState {
    fn from(ss: &SourceReaderState) -> Self {
        match ss {
            SourceReaderState::Stopped => Self::Stopped,
            SourceReaderState::Started => Self::Started,
        }
    }
}
