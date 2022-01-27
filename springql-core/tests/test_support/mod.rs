use std::time::Duration;

use springql_core::low_level_rs::*;
use springql_foreign_service::sink::ForeignSink;

pub fn apply_ddls(ddls: &[String], config: SpringConfig) -> SpringPipeline {
    let pipeline = spring_open(config).unwrap();
    for ddl in ddls {
        spring_command(&pipeline, ddl).unwrap();
    }
    pipeline
}

pub fn drain_from_sink(sink: &ForeignSink) -> Vec<serde_json::Value> {
    let mut received = Vec::new();
    while let Some(v) = sink.try_receive(Duration::from_secs(1)) {
        received.push(v);
    }
    received
}
