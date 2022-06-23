// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::pipeline::{ColumnDataType, ColumnDefinition, ColumnName, SqlType, StreamShape};

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct CANSourceStreamShape(StreamShape);

impl Default for CANSourceStreamShape {
    fn default() -> Self {
        let can_id_col = ColumnDefinition::new(
            ColumnDataType::new(
                ColumnName::new("can_id".to_string()),
                SqlType::unsigned_integer(),
                false,
            ),
            vec![],
        );

        let can_data_col = ColumnDefinition::new(
            ColumnDataType::new(
                ColumnName::new("can_data".to_string()),
                SqlType::blob(),
                false,
            ),
            vec![],
        );

        let stream_shape =
            StreamShape::new(vec![can_id_col, can_data_col]).expect("must be a valid shape");

        Self(stream_shape)
    }
}
