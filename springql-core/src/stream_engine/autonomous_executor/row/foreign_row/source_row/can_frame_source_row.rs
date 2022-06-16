// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::sync::Arc;

use socketcan::CANFrame;

use crate::{
    api::error::Result, pipeline::StreamModel, stream_engine::autonomous_executor::row::Row,
};

/// Input row from foreign sources (retrieved from SourceReader).
///
/// Immediately converted into `Row` on stream-engine boundary.
#[derive(Debug, new)]
pub struct CANFrameSourceRow(CANFrame);

impl CANFrameSourceRow {
    /// # Failure
    /// 
    /// - `SpringError::Sql` when:
    ///   - `stream_model` 
    pub fn into_row(self, stream_model: Arc<StreamModel>) -> Result<Row> {
        todo!("use fixed columns (ptime, id, data)")
    }
}

impl PartialEq for CANFrameSourceRow {
    fn eq(&self, other: &Self) -> bool {
        self.0.id() == other.0.id() && self.0.data() == other.0.data()
    }
}
impl Eq for CANFrameSourceRow {}
