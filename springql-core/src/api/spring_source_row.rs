// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use crate::stream_engine::autonomous_executor::SourceRow;

/// Row object from an in memory sink queue.
#[derive(Clone, Debug)]
pub struct SpringSourceRow(SourceRow);

impl SpringSourceRow {}
