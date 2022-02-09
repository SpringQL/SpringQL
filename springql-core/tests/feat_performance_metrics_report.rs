// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use chrono::NaiveDateTime;
use pretty_assertions::assert_eq;
use rand::prelude::{IteratorRandom, SliceRandom};
use serde_json::json;
use springql_core::low_level_rs::*;
use springql_foreign_service::sink::ForeignSink;
use springql_foreign_service::source::source_input::ForeignSourceInput;
use springql_foreign_service::source::ForeignSource;
use springql_test_logger::setup_test_logger;
use springql_test_web_console_mock::builder::WebConsoleMockBuilder;

use crate::test_support::{apply_ddls, drain_from_sink};

/// 1 sec tick data
fn gen_source_input(n: u64) -> impl Iterator<Item = serde_json::Value> {
    let mut rng = rand::thread_rng();

    (0..n).map(move |i| {
        let ts = NaiveDateTime::from_timestamp(i as i64, 0);
        let ts = ts.format("%Y-%m-%d %H:%M:%S.0000000000").to_string();

        let ticker = ["ORCL", "GOOGL", "MSFT", "AAPL", "FB"]
            .choose(&mut rng)
            .unwrap();
        let amount = (1..10000).choose(&mut rng).unwrap();

        json!({
            "ts": ts,
            "ticker": ticker,
            "amount": amount,
        })
    })
}

#[test]
fn test_performance_metrics_report() {
    setup_test_logger();

    let source_input = gen_source_input(100000).collect();

    let test_source =
        ForeignSource::start(ForeignSourceInput::new_fifo_batch(source_input)).unwrap();
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
        CREATE PUMP pu_floor_time AS
          INSERT INTO sink_trade (ts, ticker, amount)
          SELECT STREAM FLOOR_TIME(source_trade.ts, DURATION_SECS(1)), source_trade.ticker, source_trade.amount FROM source_trade;
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_trade FOR sink_trade
          TYPE NET_SERVER OPTIONS (
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
          TYPE NET_SERVER OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
          );
        ",
            remote_host = test_source.host_ip(),
            remote_port = test_source.port()
        ),
    ];

    let mock = WebConsoleMockBuilder::default()
        .add_callback_post_pipeline(|| {
            log::error!("post_pipeline");
        })
        .bulid();

    let mut config = spring_config_default();
    config.web_console.enable_report_post = true;
    config.web_console.report_interval_msec = 100;
    config.web_console.host = mock.sock_addr().ip().to_string();
    config.web_console.port = mock.sock_addr().port();

    mock.start();

    let _pipeline = apply_ddls(&ddls, config);

    let sink_received = drain_from_sink(&test_sink);
    let r = sink_received.get(0).unwrap();

    assert_eq!(r["ts"], "2020-01-01 23:59:59.000000000");
}
