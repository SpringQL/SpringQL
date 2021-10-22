use crate::{
    error::Result,
    stream_engine::executor::foreign_input_row::foreign_input_row_chunk::ForeignInputRowChunk,
};

mod net;

trait InputServer {
    /// Returns currently available foreign rows. Can be empty.
    fn next() -> Result<ForeignInputRowChunk>;
}
