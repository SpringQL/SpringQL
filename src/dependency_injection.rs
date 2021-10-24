#[cfg(test)]
pub(crate) mod test_di;

use crate::stream_engine::CurrentTimestamp;

/// Compile-time dependency injection.
pub(crate) trait DependencyInjection {
    type CurrentTimestampType: CurrentTimestamp;
}
