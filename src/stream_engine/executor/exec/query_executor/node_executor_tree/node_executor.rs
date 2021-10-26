pub(in crate::stream_engine::executor::exec::query_executor) mod collect_executor;

use std::fmt::Debug;

use crate::stream_engine::executor::exec::query_executor::window_executor::SlidingWindowExecutor;

use self::collect_executor::CollectExecutor;

#[derive(Debug)]
pub(in crate::stream_engine::executor::exec::query_executor) enum NodeExecutor {
    Collect(CollectNodeExecutor),
    Stream(StreamNodeExecutor),
    Window(WindowNodeExecutor),
}

#[derive(Debug)]
pub(in crate::stream_engine::executor::exec::query_executor) enum CollectNodeExecutor {
    Collect(CollectExecutor),
}

#[derive(Debug)]
pub(in crate::stream_engine::executor::exec::query_executor) enum StreamNodeExecutor {}

#[derive(Debug)]
pub(in crate::stream_engine::executor::exec::query_executor) enum WindowNodeExecutor {
    Sliding(SlidingWindowExecutor),
}
