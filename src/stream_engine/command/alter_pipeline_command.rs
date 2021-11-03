pub(crate) mod alter_pump_command;

use crate::{
    model::name::PumpName,
    stream_engine::pipeline::{
        pump_model::{pump_state::PumpState, PumpModel},
        server_model::ServerModel,
        stream_model::StreamModel,
    },
};

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateStream(StreamModel),
    CreateForeignStream(ServerModel),
    CreatePump(PumpModel),
    AlterPump { name: PumpName, state: PumpState },
}
