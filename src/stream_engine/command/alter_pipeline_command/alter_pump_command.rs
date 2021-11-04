use crate::pipeline::name::PumpName;

#[derive(Eq, PartialEq, Debug)]
pub(crate) enum AlterPumpCommand {
    Start(PumpName),
    Stop(PumpName),
}
