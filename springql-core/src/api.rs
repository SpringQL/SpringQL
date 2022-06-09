// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub mod error;
mod spring_config;
pub use crate::{
    api::{
        error::{Result, SpringError},
        spring_config::*,
        SpringConfig,
    },
    stream_engine::{
        time::{duration::event_duration::SpringEventDuration, timestamp::SpringTimestamp},
        SpringValue,
    },
};

// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::{
    connection::Connection,
    stream_engine::{SinkRow, SqlValue},
};

/// Pipeline.
#[derive(Debug)]
pub struct SpringPipeline(Connection);

impl SpringPipeline {
    /// Creates and open an in-process stream pipeline.
    pub fn new(config: &SpringConfig) -> Result<Self> {
        let conn = Connection::new(config);
        Ok(Self(conn))
    }

    /// Execute commands (DDL).
    ///
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::api::error::SpringError::Sql) when:
    ///   - Invalid SQL syntax.
    ///   - Refers to undefined objects (streams, pumps, etc)
    ///   - Other semantic errors.
    /// - [SpringError::InvalidOption](crate::api::error::SpringError::Sql) when:
    ///   - `OPTIONS` in `CREATE` statement includes invalid key or value.
    pub fn command<S: AsRef<str>>(&self, sql: S) -> Result<()> {
        self.0.command(sql.as_ref())
    }

    /// Pop a row from an in memory queue. This is a blocking function.
    ///
    /// **Do not call this function from threads.**
    /// If you need to pop from multiple in-memory queues using threads, use `spring_pop_non_blocking()`.
    /// See: <https://github.com/SpringQL/SpringQL/issues/125>
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::api::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    pub fn pop(&self, queue: &str) -> Result<SpringRow> {
        self.0.pop(queue).map(SpringRow)
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
    /// - [SpringError::Unavailable](crate::api::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    pub fn pop_non_blocking(&self, queue: &str) -> Result<Option<SpringRow>> {
        self.0
            .pop_non_blocking(queue)
            .map(|opt_row| opt_row.map(SpringRow))
    }
}

/// Row object from an in memory queue.
#[derive(Debug)]
pub struct SpringRow(SinkRow);

impl SpringRow {
    /// Get a i-th column value from the row.
    ///
    /// # Failure
    ///
    /// - [SpringError::Sql](crate::api::error::SpringError::Sql) when:
    ///   - Column index out of range
    /// - [SpringError::Null](crate::api::error::SpringError::Null) when:
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
    /// - [SpringError::InvalidConfig](crate::api::error::SpringError::InvalidConfig) when:
    ///   - `overwrite_config_toml` includes invalid key and/or value.
    /// - [SpringError::InvalidFormat](crate::api::error::SpringError::InvalidFormat) when:
    ///   - `overwrite_config_toml` is not valid as TOML.
    pub fn from_toml(overwrite_config_toml: &str) -> Result<SpringConfig> {
        SpringConfig::new(overwrite_config_toml)
    }
}
