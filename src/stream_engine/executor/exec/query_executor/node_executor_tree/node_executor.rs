pub(in crate::stream_engine::executor::exec::query_executor) mod collect_executor;

use std::fmt::Debug;

use crate::{
    dependency_injection::DependencyInjection,
    stream_engine::executor::exec::query_executor::window_executor::SlidingWindowExecutor,
};

use self::collect_executor::CollectExecutor;

#[derive(Debug)]
pub(in crate::stream_engine::executor::exec::query_executor) enum NodeExecutor<DI>
where
    DI: DependencyInjection,
{
    Collect(CollectNodeExecutor<DI>),
    Stream(StreamNodeExecutor),
    Window(WindowNodeExecutor),
}

#[derive(Debug)]
pub(in crate::stream_engine::executor::exec::query_executor) enum CollectNodeExecutor<DI>
where
    DI: DependencyInjection,
{
    Collect(CollectExecutor<DI>),
}

#[derive(Debug)]
pub(in crate::stream_engine::executor::exec::query_executor) enum StreamNodeExecutor {}

#[derive(Debug)]
pub(in crate::stream_engine::executor::exec::query_executor) enum WindowNodeExecutor {
    Sliding(SlidingWindowExecutor),
}
