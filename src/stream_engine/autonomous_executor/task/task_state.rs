use crate::stream_engine::pipeline::{
    pump_model::pump_state::PumpState, server_model::server_state::ServerState,
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

impl From<&ServerState> for TaskState {
    fn from(ss: &ServerState) -> Self {
        match ss {
            ServerState::Stopped => Self::Stopped,
            ServerState::Started => Self::Started,
        }
    }
}
