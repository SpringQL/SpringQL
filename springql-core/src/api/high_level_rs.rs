// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! High-level Rust API to execute / register SpringQL from Rust.

use crate::{
    error::{Result, SpringError},
    low_level_rs::{spring_command, spring_open, spring_pop, SpringConfig, SpringPipeline},
    stream_engine::{SinkRow, SpringValue, SqlValue},
};

/// Pipeline.
#[derive(Debug)]
pub struct SpringPipelineHL(SpringPipeline);

impl SpringPipelineHL {
    /// Creates and open an in-process stream pipeline.
    pub fn new(config: &SpringConfig) -> Result<Self> {
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

    /// Pop a row from an in memory queue. This is a blocking function.
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    pub fn pop(&self, queue: &str) -> Result<SpringRowHL> {
        spring_pop(&self.0, queue).map(|row| SpringRowHL(row.0))
    }
}

/// Row object from an in memory queue.
#[derive(Debug)]
pub struct SpringRowHL(SinkRow);

impl SpringRowHL {
    /// Get a i-th column value from the row.
    ///
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Column index out of range
    /// - [SpringError::Null](crate::error::SpringError::Null) when:
    ///   - Column value is NULL
    pub fn get_not_null_by_index<T>(&self, i_col: usize) -> Result<T>
    where
        T: SpringValue,
    {
        let sql_value = self.0.get_by_index(i_col)?;

        match sql_value {
            SqlValue::Null => Err(SpringError::Null {
                stream_name: self.0.stream_name().clone(),
                i_col,
            }),
            SqlValue::NotNull(nn_sql_value) => nn_sql_value.unpack(),
        }
    }
}

impl SpringConfig {
    /// Configuration by TOML format string.
    ///
    /// # Parameters
    ///
    /// - `overwrite_config_toml`: TOML format configuration to overwrite default. See `SPRING_CONFIG_DEFAULT` in [spring_config.rs](https://github.com/SpringQL/SpringQL/tree/main/springql-core/src/api/low_level_rs/spring_config.rs) for full-set default configuration.
    ///
    /// # Failures
    ///
    /// - [SpringError::InvalidConfig](crate::error::SpringError::InvalidConfig) when:
    ///   - `overwrite_config_toml` includes invalid key and/or value.
    /// - [SpringError::InvalidFormat](crate::error::SpringError::InvalidFormat) when:
    ///   - `overwrite_config_toml` is not valid as TOML.
    pub fn from_toml(overwrite_config_toml: &str) -> Result<SpringConfig> {
        SpringConfig::new(overwrite_config_toml)
    }
}
