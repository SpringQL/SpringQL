// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use springql_core::{
    api::{error::Result, SpringConfig, SpringSinkRow, SpringSourceRow},
    Connection,
};

/// Pipeline.
#[derive(Debug)]
pub struct Pipeline(Connection);

impl Pipeline {
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
    /// If you need to pop from multiple in-memory queues using threads, use `pop_non_blocking()`.
    /// See: <https://github.com/SpringQL/SpringQL/issues/125>
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::api::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    pub fn pop(&self, queue: &str) -> Result<SpringSinkRow> {
        self.0.pop(queue).map(SpringSinkRow::new)
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
    pub fn pop_non_blocking(&self, queue: &str) -> Result<Option<SpringSinkRow>> {
        self.0
            .pop_non_blocking(queue)
            .map(|opt_row| opt_row.map(SpringSinkRow::new))
    }

    /// Push a row into an in memory queue. This is a non-blocking function.
    ///
    /// # Failure
    ///
    /// - [SpringError::Unavailable](crate::api::error::SpringError::Unavailable) when:
    ///   - queue named `queue` does not exist.
    ///
    /// TODO remove `queue` parameter
    pub fn apply(&self, queue: &str, row: SpringSourceRow) -> Result<()> {
        self.0.push(queue, row.into_schemaless_row()?)
    }
}
