//! Objects to model pipelines.
//!
//! Created by sql-processor.
//! Executed by stream-engine.
//!
//! - Pipeline
//!   - Stream
//!     - (native) Stream
//!     - Input/Output Foreign Stream
//!   - Input/Output Server
//!   - Pump

pub(crate) mod column;
pub(crate) mod name;
pub(crate) mod option;
pub(crate) mod query_plan;
pub(crate) mod server_model;
pub(crate) mod sql_type;
pub(crate) mod stream_model;
