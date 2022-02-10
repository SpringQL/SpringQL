// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use std::sync::{Arc, Mutex};

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
use springql_test_web_console_mock::request_body::PostTaskGraphBody;
use springql_test_web_console_mock::WebConsoleMock;
use test_support::drain_from_sink;

use crate::test_support::apply_ddls;

/// 1 sec tick data
fn _gen_source_input(n: u64) -> impl Iterator<Item = serde_json::Value> {
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

fn _config(mock: &WebConsoleMock) -> SpringConfig {
    let mut config = spring_config_default();
    config.web_console.enable_report_post = true;
    config.web_console.report_interval_msec = 100;
    config.web_console.host = mock.sock_addr().ip().to_string();
    config.web_console.port = mock.sock_addr().port();
    config
}

fn _set_callback(
    mock_builder: WebConsoleMockBuilder,
    pile: Arc<Mutex<Vec<PostTaskGraphBody>>>,
) -> WebConsoleMockBuilder {
    mock_builder.add_callback_post_pipeline(move |body| {
        pile.lock().unwrap().push(body);
    })
}

fn start_pile_reports(
    ddls: &[String],
    test_sink: &ForeignSink,
) -> Arc<Mutex<Vec<PostTaskGraphBody>>> {
    let body_pile = Arc::new(Mutex::new(Vec::new()));

    let mock_builder = WebConsoleMockBuilder::default();
    let mock_builder = _set_callback(mock_builder, body_pile.clone());
    let mock = mock_builder.build();

    let config = _config(&mock);

    mock.start();

    let _pipeline = apply_ddls(ddls, config);
    let _sink_received = drain_from_sink(test_sink);

    body_pile
}

#[test]
fn test_performance_metrics_report() {
    setup_test_logger();

    let source_input = _gen_source_input(100000).collect();

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

    let body_pile = start_pile_reports(&ddls, &test_sink);

    log::error!("a: {:#?}", body_pile.lock().unwrap());
}

#[test]
fn test_performance_metrics_report_sampling() {
    setup_test_logger();

    let source_input = _gen_source_input(100000).collect();

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
        CREATE SINK STREAM sink_sampled_trade_amount (
          ts TIMESTAMP NOT NULL ROWTIME,    
          amount INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_passthrough AS
          INSERT INTO sink_sampled_trade_amount (ts, amount)
          SELECT STREAM
            FLOOR_TIME(source_trade.ts, DURATION_SECS(10)) AS sampled_ts,
            AVG(source_trade.amount) AS avg_amount
          FROM source_trade
          GROUP BY sampled_ts
          FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_trade FOR sink_sampled_trade_amount
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

    let body_pile = start_pile_reports(&ddls, &test_sink);

    log::error!("a: {:#?}", body_pile.lock().unwrap());
}
