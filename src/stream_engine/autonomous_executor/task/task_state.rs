use crate::stream_engine::pipeline::pump_model::pump_state::PumpState;

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
