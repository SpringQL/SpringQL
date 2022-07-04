// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Usage:
//!
//! ```bash
//! # Launch your HTTP server first.
//!
//! cargo run --example http_client_sink_writer -- REMOTE_HOST REMOTE_PORT
//! ```

use std::{env, thread, time::Duration};

use springql::SpringSourceRowBuilder;
use springql_core::api::{SpringConfig, SpringPipeline};

fn parse_remote_args() -> (String, u16) {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 3);

    let host = args[1].clone();
    let port = args[2].parse::<u16>().unwrap();
    log::info!("Remote info - {}:{}", host, port);

    (host, port)
}

fn main() {
    let (sink_host, sink_port) = parse_remote_args();

    let pipeline = SpringPipeline::new(&SpringConfig::default()).unwrap();

    pipeline
        .command(
            "
            CREATE SOURCE STREAM source_1 (
                bytes BLOB NOT NULL
              );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK STREAM sink_1 (
                http_body BLOB NOT NULL
              );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE PUMP pu_1 AS
            INSERT INTO sink_1 (http_body)
            SELECT STREAM source_1.bytes FROM source_1;
            ",
        )
        .unwrap();

    pipeline
        .command(format!(
            "
            CREATE SINK WRITER http_sink_1 FOR sink_1
              TYPE HTTP1_CLIENT OPTIONS (
                REMOTE_HOST '{}',
                REMOTE_PORT '{}',
                METHOD 'POST',
                PATH '/test',
                HEADER_Content-Type 'application/octet-stream',
                HEADER_Connection 'keep-alive',
                BLOB_BODY_COLUMN 'http_body'
            );
            ",
            remote_host = sink_host,
            remote_port = sink_port
        ))
        .unwrap();

    pipeline
        .command(
            "
            CREATE SOURCE READER q_source_1 FOR source_1
            TYPE IN_MEMORY_QUEUE OPTIONS (
              NAME 'q'
            );
            ",
        )
        .unwrap();

    let bytes = vec![b'h', b'e', b'l', b'l', b'o', 0, 1, 2];

    let source_rows = vec![SpringSourceRowBuilder::default()
        .add_column("bytes", bytes)
        .unwrap()
        .build()];

    for row in source_rows {
        pipeline.push("q", row).unwrap()
    }

    thread::sleep(Duration::from_secs(1));
}
