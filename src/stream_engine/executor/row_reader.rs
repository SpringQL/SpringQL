pub(in crate::stream_engine::executor) trait RowReader {
    fn next() -> Result<Rows>;
}
