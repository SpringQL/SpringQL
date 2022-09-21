// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Usage:
//!
//! ```bash
//! cargo run --example http_client_sink_writer -- REMOTE_HOST REMOTE_PORT
//! ```

use std::{env, process::Command, thread, time::Duration};

use springql_core_release_test::api::{SpringConfig, SpringPipeline};
use springql_release_test::SpringSourceRowBuilder;
use springql_test_logger::setup_test_logger;

fn parse_remote_args() -> (String, u16) {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 3);

    let host = args[1].clone();
    let port = args[2].parse::<u16>().unwrap();
    log::info!("Remote info - {}:{}", host, port);

    (host, port)
}

fn launch_http_server(port: u16) {
    Command::new("bash")
        .arg("-c")
        .arg(format!("nc -l {}", port))
        .spawn()
        .expect("failed to launch http server");
}

fn main() {
    setup_test_logger();

    let (sink_host, sink_port) = parse_remote_args();

    launch_http_server(sink_port);

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
                REMOTE_HOST '{remote_host}',
                REMOTE_PORT '{remote_port}',
                METHOD 'POST',
                URL 'http://{remote_host}:{remote_port}/test',
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

    let bytes = vec![b'h', b'e', b'l', b'l', b'o', 0xF0, 0x9F, 0x98, 0x84];

    let source_rows = vec![SpringSourceRowBuilder::default()
        .add_column("bytes", bytes)
        .unwrap()
        .build()];

    for row in source_rows {
        pipeline.push("q", row).unwrap()
    }

    thread::sleep(Duration::from_secs(1));
}
