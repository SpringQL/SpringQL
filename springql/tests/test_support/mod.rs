// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

use std::{time::Duration, io::copy};

use springql::{SpringConfig, SpringPipeline};
use springql_foreign_service::sink::ForeignSink;
use tempfile::NamedTempFile;

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

#[allow(dead_code)]
pub(crate) fn http_download_file_to_tempdir(url: &str) -> NamedTempFile {
    log::info!("Downloading file from {}", url);

    let fname = url.split('/').last().unwrap();

    let mut tempfile = tempfile::Builder::new()
        .suffix(&format!("-{}", fname))
        .tempfile()
        .unwrap();
    let response = reqwest::blocking::get(url).unwrap();

    let content = response.text().unwrap();
    copy(&mut content.as_bytes(), &mut tempfile).unwrap();

    log::info!("Finish downloading into {}", tempfile.path().display());
    tempfile
}
