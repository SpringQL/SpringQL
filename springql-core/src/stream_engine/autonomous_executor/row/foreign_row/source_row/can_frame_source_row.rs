// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use socketcan::CanFrame;

use crate::{
    api::error::Result,
    pipeline::ColumnName,
    stream_engine::{
        autonomous_executor::{row::SchemalessRow, ColumnValues},
        NnSqlValue, SqlValue,
    },
};

/// Input row from foreign sources (retrieved from SourceReader).
///
/// Immediately converted into `Row` on stream-engine boundary.
#[derive(Clone, Debug, new)]
pub struct CANFrameSourceRow(CanFrame);

impl CANFrameSourceRow {
    /// # Failure
    ///
    /// - `SpringError::Sql` when:
    ///   - `stream_model`
    pub fn into_schemaless_row(self) -> Result<SchemalessRow> {
        let column_values = self.into_column_values();
        Ok(SchemalessRow::from(column_values))
    }

    fn into_column_values(self) -> ColumnValues {
        let can_frame = self.0;
        let mut column_values = ColumnValues::default();

        let can_id_value = SqlValue::NotNull(NnSqlValue::UnsignedInteger(can_frame.id()));
        column_values
            .insert(ColumnName::new("can_id".to_string()), can_id_value)
            .expect("can_id must not duplicate");

        let can_data_value = SqlValue::NotNull(NnSqlValue::Blob(can_frame.data().to_vec()));
        column_values
            .insert(ColumnName::new("can_data".to_string()), can_data_value)
            .expect("can_data must not duplicate");

        column_values
    }
}

impl PartialEq for CANFrameSourceRow {
    fn eq(&self, other: &Self) -> bool {
        self.0.id() == other.0.id() && self.0.data() == other.0.data()
    }
}
impl Eq for CANFrameSourceRow {}
