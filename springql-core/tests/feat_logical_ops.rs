// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;
use springql_core::low_level_rs::*;
use springql_foreign_service::sink::ForeignSink;
use springql_foreign_service::source::source_input::ForeignSourceInput;
use springql_foreign_service::source::ForeignSource;
use springql_test_logger::setup_test_logger;

use crate::test_support::{apply_ddls, drain_from_sink};

#[test]
fn test_feat_and() {
    setup_test_logger();

    let json1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
    });
    let source_input = vec![json1];

    let test_source =
        ForeignSource::start(ForeignSourceInput::new_fifo_batch(source_input)).unwrap();
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
          answer_true_and_true BOOLEAN NOT NULL,
          answer_true_and_false BOOLEAN NOT NULL,
          answer_false_and_true BOOLEAN NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_add AS
          INSERT INTO sink_1 (ts, answer_true_and_true, answer_true_and_false, answer_false_and_true)
          SELECT STREAM source_1.ts, TRUE AND TRUE, TRUE AND FALSE, FALSE AND TRUE FROM source_1;
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

    let _pipeline = apply_ddls(&ddls, spring_config_default());
    let sink_received = drain_from_sink(&test_sink);
    let r = sink_received.get(0).unwrap();

    assert_eq!(r["answer_true_and_true"], true);
    assert_eq!(r["answer_true_and_false"], false);
    assert_eq!(r["answer_false_and_true"], false);
}
