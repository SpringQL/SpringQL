pub(in crate::stream_engine::autonomous_executor::exec::query_executor) mod collect_executor;
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) mod window_executor;

use std::fmt::Debug;

use crate::stream_engine::dependency_injection::DependencyInjection;

use self::{collect_executor::CollectExecutor, window_executor::SlidingWindowExecutor};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) enum NodeExecutor {
    Collect(CollectNodeExecutor),
    Stream(StreamNodeExecutor),
    Window(WindowNodeExecutor),
}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) enum CollectNodeExecutor {
    Collect(CollectExecutor),
}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) enum StreamNodeExecutor {}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) enum WindowNodeExecutor {
    Sliding(SlidingWindowExecutor),
}
