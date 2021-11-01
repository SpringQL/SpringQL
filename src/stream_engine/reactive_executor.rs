use super::dependency_injection::DependencyInjection;

/// Executor of pipeline management.
///
/// All methods (recursive) are called from main thread.
#[derive(Debug, Default)]
pub(in crate::stream_engine) struct ReactiveExecutor;
