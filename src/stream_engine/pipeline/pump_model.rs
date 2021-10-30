use crate::model::name::{PumpName, StreamName};

#[derive(Eq, PartialEq, Debug, new)]
pub(crate) struct PumpModel {
    name: PumpName,
    upstream: StreamName,
    downstream: StreamName,
}
