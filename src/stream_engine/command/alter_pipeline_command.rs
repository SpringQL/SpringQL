pub(crate) mod alter_pump_command;

use crate::pipeline::{
    name::PumpName,
    pump_model::{pump_state::PumpState, PumpModel},
    server_model::ServerModel,
    stream_model::StreamModel,
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateStream(StreamModel),
    CreateForeignStream(ServerModel),
    CreatePump(PumpModel),
    AlterPump { name: PumpName, state: PumpState },
}
