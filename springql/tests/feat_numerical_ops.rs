// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use float_cmp::approx_eq;
use pretty_assertions::assert_eq;
use serde_json::json;
use springql_foreign_service::{
    sink::ForeignSink,
    source::{ForeignSource, ForeignSourceInput},
};
use springql_release_test::*;
use springql_test_logger::setup_test_logger;

use crate::test_support::{apply_ddls, drain_from_sink};

#[test]
fn test_feat_add_mul_integer() {
    setup_test_logger();

    let json1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
    });
    let source_input = vec![json1];

    let test_source = ForeignSource::new().unwrap();
    let test_sink = ForeignSink::start().unwrap();

    let ddls = vec![
        "
        CREATE SOURCE STREAM source_1 (
          ts TIMESTAMP NOT NULL ROWTIME
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_1 (
          ts TIMESTAMP NOT NULL ROWTIME,
          answer_add INTEGER NOT NULL,
          answer_mul INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_add AS
          INSERT INTO sink_1 (ts, answer_add, answer_mul)
          SELECT STREAM source_1.ts, 1+1, 2*2 AS two FROM source_1;
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_1 FOR sink_1
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = test_sink.host_ip(),
            remote_port = test_sink.port()
        ),
        format!(
            "
        CREATE SOURCE READER tcp_1 FOR source_1
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
          );
        ",
            remote_host = test_source.host_ip(),
            remote_port = test_source.port()
        ),
    ];

    let _pipeline = apply_ddls(&ddls, SpringConfig::default());
    test_source.start(ForeignSourceInput::new_fifo_batch(source_input));
    let sink_received = drain_from_sink(&test_sink);
    let r = sink_received.get(0).unwrap();

    assert_eq!(r["answer_add"], 2);
    assert_eq!(r["answer_mul"], 4);
}

#[test]
fn test_feat_add_mul_float() {
    setup_test_logger();

    let json1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
    });
    let source_input = vec![json1];

    let test_source = ForeignSource::new().unwrap();
    let test_sink = ForeignSink::start().unwrap();

    let ddls = vec![
        "
        CREATE SOURCE STREAM source_1 (
          ts TIMESTAMP NOT NULL ROWTIME
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_1 (
          ts TIMESTAMP NOT NULL ROWTIME,
          answer_add FLOAT NOT NULL,
          answer_mul FLOAT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_add AS
          INSERT INTO sink_1 (ts, answer_add, answer_mul)
          SELECT STREAM source_1.ts, 1.5+1.0, 1.5*2.0 AS two FROM source_1;
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_1 FOR sink_1
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = test_sink.host_ip(),
            remote_port = test_sink.port()
        ),
        format!(
            "
        CREATE SOURCE READER tcp_1 FOR source_1
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
          );
        ",
            remote_host = test_source.host_ip(),
            remote_port = test_source.port()
        ),
    ];

    let _pipeline = apply_ddls(&ddls, SpringConfig::default());
    test_source.start(ForeignSourceInput::new_fifo_batch(source_input));
    let sink_received = drain_from_sink(&test_sink);
    let r = sink_received.get(0).unwrap();

    assert!(approx_eq!(f64, r["answer_add"].as_f64().unwrap(), 2.5));
    assert!(approx_eq!(f64, r["answer_mul"].as_f64().unwrap(), 3.0));
}

#[test]
fn test_feat_unsigned_integer() {
    setup_test_logger();

    let json1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
    });
    let source_input = vec![json1];

    let test_source = ForeignSource::new().unwrap();
    let test_sink = ForeignSink::start().unwrap();

    let ddls = vec![
        "
        CREATE SOURCE STREAM source_1 (
          ts TIMESTAMP NOT NULL ROWTIME
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_1 (
          ts TIMESTAMP NOT NULL ROWTIME,
          u32 UNSIGNED INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_add AS
          INSERT INTO sink_1 (ts, u32)
          SELECT STREAM source_1.ts, 4294967295 FROM source_1;
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_1 FOR sink_1
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = test_sink.host_ip(),
            remote_port = test_sink.port()
        ),
        format!(
            "
        CREATE SOURCE READER tcp_1 FOR source_1
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
          );
        ",
            remote_host = test_source.host_ip(),
            remote_port = test_source.port()
        ),
    ];

    let _pipeline = apply_ddls(&ddls, SpringConfig::default());
    test_source.start(ForeignSourceInput::new_fifo_batch(source_input));
    let sink_received = drain_from_sink(&test_sink);
    let r = sink_received.get(0).unwrap();

    assert_eq!(r["u32"], u32::MAX);
}
