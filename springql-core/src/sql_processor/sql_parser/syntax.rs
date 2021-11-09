use crate::pipeline::name::{ColumnName, StreamName};

#[derive(Clone, Eq, PartialEq, Debug)]
pub(super) enum ColumnConstraintSyntax {
    NotNull, // this is treated as data type in pipeline
    Rowtime,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(super) struct OptionSyntax {
    pub(super) option_name: String,
    pub(super) option_value: String,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(in crate::sql_processor) struct SelectStreamSyntax {
    pub(in crate::sql_processor) column_names: Vec<ColumnName>,
    pub(in crate::sql_processor) from_stream: StreamName,
}
