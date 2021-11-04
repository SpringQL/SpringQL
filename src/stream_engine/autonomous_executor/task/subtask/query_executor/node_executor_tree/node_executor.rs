pub(super) mod collect_executor;
pub(super) mod window_executor;

use std::fmt::Debug;

use self::{collect_executor::CollectExecutor, window_executor::SlidingWindowExecutor};

#[derive(Debug)]
pub(super) enum NodeExecutor {
    Collect(CollectNodeExecutor),
    Stream(StreamNodeExecutor),
    Window(WindowNodeExecutor),
}

#[derive(Debug)]
pub(super) enum CollectNodeExecutor {
    Collect(CollectExecutor),
}

#[derive(Debug)]
pub(super) enum StreamNodeExecutor {}

#[derive(Debug)]
pub(super) enum WindowNodeExecutor {
    Sliding(SlidingWindowExecutor),
}
