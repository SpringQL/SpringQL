use crate::{
    error::Result,
    stream_engine::{executor::foreign_input_row::ForeignInputRow, model::option::Options},
};

mod net;

trait InputServerStandby<A: InputServerActive> {
    fn new(options: Options) -> Result<Self>
    where
        Self: Sized;

    /// Blocks until the server is ready to provide ForeignInputRow.
    fn start(self) -> Result<A>;
}

/// Active: ready to provide ForeignInputRow.
trait InputServerActive {
    /// Returns currently available foreign row.
    fn next_row(&mut self) -> Result<ForeignInputRow>;
}
