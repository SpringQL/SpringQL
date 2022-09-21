// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::time::Duration;

use springql_foreign_service::sink::ForeignSink;
use springql_release_test::{SpringConfig, SpringPipeline};

pub mod request_body;

#[allow(dead_code)]
pub fn apply_ddls(ddls: &[String], config: SpringConfig) -> SpringPipeline {
    let pipeline = SpringPipeline::new(&config).unwrap();
    for ddl in ddls {
        pipeline.command(ddl).unwrap();
    }
    pipeline
}

#[allow(dead_code)]
pub fn drain_from_sink(sink: &ForeignSink) -> Vec<serde_json::Value> {
    let mut received = Vec::new();
    while let Some(v) = sink.try_receive(Duration::from_secs(2)) {
        received.push(v);
    }
    received
}
