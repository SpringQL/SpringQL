// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

pub mod error;

mod spring_config;
mod spring_pipeline;
mod spring_sink_row;
mod spring_source_row;

pub use crate::{
    api::{
        error::{Result, SpringError},
        spring_config::*,
        spring_pipeline::SpringPipeline,
        spring_sink_row::SpringSinkRow,
        SpringConfig,
    },
    stream_engine::{
        time::{SpringEventDuration, SpringTimestamp},
        SpringValue,
    },
};
