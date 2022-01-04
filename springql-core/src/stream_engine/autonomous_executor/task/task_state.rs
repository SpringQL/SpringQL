// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use crate::pipeline::pump_model::pump_state::PumpState;

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
