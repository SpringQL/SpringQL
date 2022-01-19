use crate::stream_engine::autonomous_executor::row::Row;

/// Input queue of window tasks.
///
/// Window queue has complicated structure, compared to row queue.
///
/// ![Window queue](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/window-queue.svg)
#[derive(Debug, Default)]
pub(in crate::stream_engine::autonomous_executor) struct WindowQueue {}

impl WindowQueue {
    pub(in crate::stream_engine::autonomous_executor) fn put(&self, row: Row) {
        todo!()
    }

    /// A downstream task makes progress to a window queue and then internal state of the window queue is changed.
    ///
    /// Watermark might be updated if a row dispatched from waiting queue has large timestamp.
    ///
    /// # Returns
    ///
    /// Empty vec if no pane inside related window is closed.
    /// Non-empty vec if at least 1 pane is closed and it has non-empty final state.
    pub(in crate::stream_engine::autonomous_executor) fn progress(&self) -> Vec<Row> {
        todo!()
    }
}
