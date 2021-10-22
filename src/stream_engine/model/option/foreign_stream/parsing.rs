use std::collections::HashMap;

use crate::stream_engine::model::column::column_name::ColumnName;

#[derive(Debug)]
pub(in crate::stream_engine) struct ParseJsonOption {
    // TODO ROW_PATH, CHARACTER_ENCODING
    column_name_path: HashMap<String, ColumnName>, // TODO Use JSON Path for key
}
