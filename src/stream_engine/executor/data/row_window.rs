use super::row::repository::RowRef;

#[derive(Debug)]
pub(in crate::stream_engine::executor) struct RowWindow(Vec<RowRef>);
