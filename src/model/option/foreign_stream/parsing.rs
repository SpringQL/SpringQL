use std::collections::HashMap;

use crate::model::name::ColumnName;

#[derive(Debug)]
pub(crate) struct ParseJsonOption {
    // TODO ROW_PATH, CHARACTER_ENCODING
    column_name_path: HashMap<String, ColumnName>, // TODO Use JSON Path for key
}
