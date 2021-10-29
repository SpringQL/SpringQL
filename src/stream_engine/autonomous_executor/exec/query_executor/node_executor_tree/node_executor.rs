pub(in crate::stream_engine::autonomous_executor::exec::query_executor) mod collect_executor;
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) mod window_executor;

use std::fmt::Debug;

use crate::dependency_injection::DependencyInjection;

use self::{collect_executor::CollectExecutor, window_executor::SlidingWindowExecutor};

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) enum NodeExecutor<DI>
where
    DI: DependencyInjection,
{
    Collect(CollectNodeExecutor<DI>),
    Stream(StreamNodeExecutor),
    Window(WindowNodeExecutor),
}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) enum CollectNodeExecutor<DI>
where
    DI: DependencyInjection,
{
    Collect(CollectExecutor<DI>),
}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) enum StreamNodeExecutor {}

#[derive(Debug)]
pub(in crate::stream_engine::autonomous_executor::exec::query_executor) enum WindowNodeExecutor {
    Sliding(SlidingWindowExecutor),
}
