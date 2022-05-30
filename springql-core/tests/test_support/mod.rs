// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::time::Duration;

use springql_core::{
    api::high_level_rs::SpringPipelineHL,
    api::low_level_rs::{spring_command, spring_open, SpringConfig, SpringPipeline},
};
use springql_foreign_service::sink::ForeignSink;

#[allow(dead_code)]
pub(crate) fn apply_ddls(ddls: &[String], config: SpringConfig) -> SpringPipelineHL {
    let pipeline = SpringPipelineHL::new(&config).unwrap();
    for ddl in ddls {
        pipeline.command(ddl).unwrap();
    }
    pipeline
}

#[allow(dead_code)]
pub(crate) fn apply_ddls_low_level(ddls: &[String], config: SpringConfig) -> SpringPipeline {
    let pipeline = spring_open(&config).unwrap();
    for ddl in ddls {
        spring_command(&pipeline, ddl).unwrap();
    }
    pipeline
}

#[allow(dead_code)]
pub(crate) fn drain_from_sink(sink: &ForeignSink) -> Vec<serde_json::Value> {
    let mut received = Vec::new();
    while let Some(v) = sink.try_receive(Duration::from_secs(1)) {
        received.push(v);
    }
    received
}
