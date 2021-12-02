// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

use super::Timestamp;

/// Mock-able current timestamp.
pub(crate) trait CurrentTimestamp {
    fn now() -> Timestamp;
}
