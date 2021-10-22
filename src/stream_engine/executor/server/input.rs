use crate::{
    error::Result,
    stream_engine::{
        executor::foreign_input_row::foreign_input_row_chunk::ForeignInputRowChunk,
        model::option::Options,
    },
};

mod net;

trait InputServerStandby<A: InputServerActive> {
    fn new(options: Options) -> Result<Self>
    where
        Self: Sized;

    fn start(self) -> Result<A>;
}

trait InputServerActive {
    /// Returns currently available foreign rows. Can be empty.
    fn next_chunk(&self) -> Result<ForeignInputRowChunk>;
}
