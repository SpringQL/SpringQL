// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! High-level Rust API to execute / register SpringQL from Rust.

use crate::{
    error::{Result, SpringError},
    low_level_rs::{
        spring_command, spring_open, spring_pop, spring_pop_non_blocking, SpringConfig,
        SpringPipeline as PipelineLL,
    },
    pipeline::SpringRow as RowLL,
    stream_engine::{SpringValue, SqlValue},
};

/// Pipeline
#[deprecated(note = "use crate::SpringPipeline")]
pub type SpringPipelineHL = crate::SpringPipeline;

/// Pipeline.
pub trait SpringPipelineApi<Row>
where
    Row: SpringRow,
{
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
    fn command<S: AsRef<str>>(&self, sql: S) -> Result<()>;

    /// Pop a row from an in memory queue. This is a blocking function.
    ///
    /// **Do not call this function from threads.**
    /// If you need to pop from multiple in-memory queues using threads, use `spring_pop_non_blocking()`.
    /// See: <https://github.com/SpringQL/SpringQL/issues/125>
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    fn pop(&self, queue: &str) -> Result<Row>;

    /// Pop a row from an in memory queue. This is a non-blocking function.
    ///
    /// # Returns
    ///
    /// - `Ok(Some)` when at least a row is in the queue.
    /// - `None` when no row is in the queue.
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    fn pop_non_blocking(&self, queue: &str) -> Result<Option<Row>>;
}

/// Row object from an in memory queue.
pub trait SpringRow {
    /// Get a i-th column value from the row.
    ///
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Column index out of range
    /// - [SpringError::Null](crate::error::SpringError::Null) when:
    ///   - Column value is NULL
    fn get_not_null_by_index<T>(&self, i_col: usize) -> Result<T>
    where
        T: SpringValue;
}

impl PipelineLL {
    /// Creates and open an in-process stream pipeline.
    pub fn new(config: &SpringConfig) -> Result<Self> {
        let low_level = spring_open(config)?;
        Ok(low_level)
    }
}

impl SpringPipelineApi<RowLL> for PipelineLL {
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
    fn command<S: AsRef<str>>(&self, sql: S) -> Result<()> {
        spring_command(&self, sql.as_ref())
    }

    /// Pop a row from an in memory queue. This is a blocking function.
    ///
    /// **Do not call this function from threads.**
    /// If you need to pop from multiple in-memory queues using threads, use `spring_pop_non_blocking()`.
    /// See: <https://github.com/SpringQL/SpringQL/issues/125>
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    fn pop(&self, queue: &str) -> Result<RowLL> {
        spring_pop(&self, queue)
    }

    /// Pop a row from an in memory queue. This is a non-blocking function.
    ///
    /// # Returns
    ///
    /// - `Ok(Some)` when at least a row is in the queue.
    /// - `None` when no row is in the queue.
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    fn pop_non_blocking(&self, queue: &str) -> Result<Option<RowLL>> {
        spring_pop_non_blocking(&self, queue)
    }
}

impl SpringRow for RowLL {
    /// Get a i-th column value from the row.
    ///
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::error::SpringError::Sql) when:
    ///   - Column index out of range
    /// - [SpringError::Null](crate::error::SpringError::Null) when:
    ///   - Column value is NULL
    fn get_not_null_by_index<T>(&self, i_col: usize) -> Result<T>
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
