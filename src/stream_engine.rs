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
//! Both reactive executor and autonomous executor instance run at a main thread, while autonomous executor has workers which run at different worker threads.
//! ```

pub(crate) mod pipeline;

mod autonomous_executor;
mod dependency_injection;

use crate::error::Result;
use autonomous_executor::{CurrentTimestamp, NaiveRowRepository, RowRepository, Scheduler};

use self::pipeline::Pipeline;

/// Stream engine has reactive executor and autonomous executor inside.
///
/// Stream engine has access methods.
/// External components (sql-processor) access methods to change stream engine's states and get result from it.
#[derive(Debug, Default)]
pub(crate) struct StreamEngine;

impl StreamEngine {
    pub(crate) fn create_pipeline(&mut self) -> Result<Pipeline> {
        Ok(Pipeline::default())
    }
}
