// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use crate::test_support::{apply_ddls, drain_from_sink};
use serde_json::json;
use springql_core::low_level_rs::*;
use springql_foreign_service::sink::ForeignSink;
use springql_foreign_service::source::source_input::ForeignSourceInput;
use springql_foreign_service::source::ForeignSource;
use springql_test_logger::setup_test_logger;

fn t(worker_config: SpringWorkerConfig) {
    setup_test_logger();

    let json_oracle = json!({
        "ts": "2021-11-04 23:02:52.123456789",
        "ticker": "ORCL",
        "amount": 20,
    });
    let json_ibm = json!({
        "ts": "2021-11-04 23:03:29.123456789",
        "ticker": "IBM",
        "amount": 30,
    });
    let json_google = json!({
        "ts": "2021-11-04 23:03:42.123456789",
        "ticker": "GOOGL",
        "amount": 100,
    });
    let source_input = vec![json_oracle, json_ibm, json_google];

    let test_source =
        ForeignSource::start(ForeignSourceInput::new_fifo_batch(source_input.clone())).unwrap();
    let test_sink = ForeignSink::start().unwrap();

    let ddls = vec![
        "
        CREATE SOURCE STREAM source_trade (
          ts TIMESTAMP NOT NULL ROWTIME,    
          ticker TEXT NOT NULL,
          amount INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_trade (
          ts TIMESTAMP NOT NULL ROWTIME,    
          ticker TEXT NOT NULL,
          amount INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_passthrough AS
          INSERT INTO sink_trade (ts, ticker, amount)
          SELECT STREAM source_trade.ts, source_trade.ticker, source_trade.amount FROM source_trade;
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_trade FOR sink_trade
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
        CREATE SOURCE READER tcp_trade FOR source_trade
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

    let config = SpringConfig {
        worker: worker_config,
        ..Default::default()
    };

    let _pipeline = apply_ddls(&ddls, config);
    let sink_received = drain_from_sink(&test_sink);

    // because worker takes source input in multi-thread, order may be changed
    assert!(sink_received.contains(source_input.get(0).unwrap()));
    assert!(sink_received.contains(source_input.get(1).unwrap()));
    assert!(sink_received.contains(source_input.get(2).unwrap()));
}

#[test]
fn test_feat_1generic_1source() {
    t(SpringWorkerConfig {
        n_generic_worker_threads: 1,
        n_source_worker_threads: 1,
    })
}

#[test]
fn test_feat_5generic_1source() {
    t(SpringWorkerConfig {
        n_generic_worker_threads: 5,
        n_source_worker_threads: 1,
    })
}

#[test]
fn test_feat_1generic_5source() {
    t(SpringWorkerConfig {
        n_generic_worker_threads: 1,
        n_source_worker_threads: 5,
    })
}

#[test]
fn test_feat_5generic_5source() {
    t(SpringWorkerConfig {
        n_generic_worker_threads: 5,
        n_source_worker_threads: 5,
    })
}
