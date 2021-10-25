#[cfg(test)]
pub(crate) mod test_di;

use crate::stream_engine::{CurrentTimestamp, RowRepository};

/// Compile-time dependency injection.
pub(crate) trait DependencyInjection {
    type CurrentTimestampType: CurrentTimestamp;
    type RowRepositoryType: RowRepository;

    fn row_repository(&self) -> &Self::RowRepositoryType;
}
