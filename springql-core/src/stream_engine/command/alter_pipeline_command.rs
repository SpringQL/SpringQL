use crate::pipeline::{
    name::PumpName,
    pump_model::{pump_state::PumpState, PumpModel},
    server_model::ServerModel,
};

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum AlterPipelineCommand {
    CreateForeignStream(ServerModel),
    CreatePump(PumpModel),
    AlterPump { name: PumpName, state: PumpState },
}
