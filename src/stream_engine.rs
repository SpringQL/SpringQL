//! Stream Engine component.
//!
//! Responsible for pipeline management and execution.
//!
//! Stream engine has 2 executors:
//!
//! 1. Reactive executor
//! 2. Autonomous executor
//!
//! Reactive executor quickly changes some status of a pipeline. It does not deal with stream data (Row, to be precise).
//! Autonomous executor deals with stream data.
//!
//! Reactive executor resides in a main thread, while autonomous executor works with separate threads (models as WorkerPool).
//! ```

mod autonomous_executor;

pub(crate) use autonomous_executor::{CurrentTimestamp, RowRepository, Timestamp};

#[cfg(test)]
pub(crate) use autonomous_executor::TestRowRepository;

#[derive(Debug)]
pub(crate) struct StreamEngine;
