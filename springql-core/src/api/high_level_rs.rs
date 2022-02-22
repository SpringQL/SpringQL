// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

//! High-level Rust API to execute / register SpringQL from Rust.

use crate::{
    error::Result,
    low_level_rs::{spring_command, spring_open, SpringConfig, SpringPipeline},
};

/// Pipeline.
#[derive(Debug)]
pub struct SpringPipelineHL(SpringPipeline);

impl SpringPipelineHL {
    /// Creates and open an in-process stream pipeline.
    pub fn new(config: SpringConfig) -> Result<Self> {
        let low_level = spring_open(config)?;
        Ok(Self(low_level))
    }

    /// Execute commands (DDL).
    ///
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Invalid SQL syntax.
    ///   - Refers to undefined objects (streams, pumps, etc)
    ///   - Other semantic errors.
    /// - [SpringError::InvalidOption](crate::error::SpringError::Sql) when:
    ///   - `OPTIONS` in `CREATE` statement includes invalid key or value.
    pub fn command<S: AsRef<str>>(&self, sql: S) -> Result<()> {
        spring_command(&self.0, sql.as_ref())
    }
}
