use super::Timestamp;

/// Mock-able current timestamp.
pub(crate) trait CurrentTimestamp {
    fn now() -> Timestamp;
}
