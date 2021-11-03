pub(crate) mod alter_pump_command;

use crate::{
    model::name::PumpName,
    stream_engine::pipeline::{
        foreign_stream_model::ForeignStreamModel,
        pump_model::{pump_state::PumpState, PumpModel},
        server_model::ServerModel,
        stream_model::StreamModel,
    },
};

use self::alter_pump_command::AlterPumpCommand;

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateStream(StreamModel),
    CreateForeignStream(ServerModel),
    CreatePump(PumpModel),
    AlterPump { name: PumpName, state: PumpState },
}
