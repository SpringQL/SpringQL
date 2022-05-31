// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub(in crate::stream_engine) mod format;
pub(in crate::stream_engine) mod sink_row;
pub(in crate::stream_engine) mod source_row;

pub(crate) use crate::stream_engine::autonomous_executor::row::foreign_row::sink_row::SinkRow;
