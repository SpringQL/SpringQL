pub(super) mod collect_subtask;
pub(super) mod window_subtask;

use std::fmt::Debug;

use self::{collect_subtask::CollectSubtask, window_subtask::SlidingWindowSubtask};

#[derive(Debug)]
pub(super) enum QuerySubtaskNode {
    Collect(CollectSubtask),
    Stream(StreamSubtask),
    Window(WindowSubtask),
}

#[derive(Debug)]
pub(super) enum StreamSubtask {}

#[derive(Debug)]
pub(super) enum WindowSubtask {
    Sliding(SlidingWindowSubtask),
}
