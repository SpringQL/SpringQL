// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use std::sync::{Arc, Mutex};

use chrono::NaiveDateTime;
use float_cmp::approx_eq;
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
fn test_performance_metrics_report_floor_time() {
    setup_test_logger();

    let source_input = _gen_source_input(10000).collect();

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
        CREATE SOURCE READER tcp_source_trade FOR source_trade
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
    let body_pile = body_pile.lock().unwrap();

    assert!(body_pile
        .iter()
        .any(|body| !body.tasks.is_empty() && !body.queues.is_empty()));

    let task_pu_floor_time = body_pile
        .iter()
        .find_map(|body| body.tasks.iter().find(|task| task.id == "pu_floor_time"))
        .expect("at least 1 should exist");
    assert_eq!(task_pu_floor_time.type_, "pump-task");
    assert!(
        approx_eq!(f32, task_pu_floor_time.avg_gain_bytes_per_sec, 0.0),
        "pu_floor_time just converts FLOAT into FLOAT"
    );

    let task_tcp_source_trade = body_pile
        .iter()
        .find_map(|body| body.tasks.iter().find(|task| task.id == "tcp_source_trade"))
        .expect("at least 1 should exist");
    assert_eq!(task_tcp_source_trade.type_, "source-task");

    let task_tcp_sink_trade = body_pile
        .iter()
        .find_map(|body| body.tasks.iter().find(|task| task.id == "tcp_sink_trade"))
        .expect("at least 1 should exist");
    assert_eq!(task_tcp_sink_trade.type_, "sink-task");

    let non_empty_q_pu_floor_time = body_pile
        .iter()
        .find_map(|body| {
            body.queues.iter().find(|queue| {
                queue.id == "pu_floor_time" && queue.row_queue.as_ref().unwrap().num_rows > 0
            })
        })
        .expect("at least 1 should exist (but in very rare case input queue is always empty)");
    assert_eq!(
        non_empty_q_pu_floor_time.upstream_task_id,
        "tcp_source_trade"
    );
    assert_eq!(
        non_empty_q_pu_floor_time.downstream_task_id,
        "pu_floor_time"
    );
    assert!(
        non_empty_q_pu_floor_time
            .row_queue
            .as_ref()
            .unwrap()
            .total_bytes
            > 0
    );
    assert!(non_empty_q_pu_floor_time.window_queue.is_none());

    let non_empty_q_tcp_sink_trade = body_pile
        .iter()
        .find_map(|body| {
            body.queues.iter().find(|queue| {
                queue.id == "tcp_sink_trade" && queue.row_queue.as_ref().unwrap().num_rows > 0
            })
        })
        .expect("at least 1 should exist (but in very rare case input queue is always empty)");
    assert_eq!(non_empty_q_tcp_sink_trade.upstream_task_id, "pu_floor_time");
    assert_eq!(
        non_empty_q_tcp_sink_trade.downstream_task_id,
        "tcp_sink_trade"
    );
    assert!(
        non_empty_q_tcp_sink_trade
            .row_queue
            .as_ref()
            .unwrap()
            .total_bytes
            > 0
    );
    assert!(non_empty_q_tcp_sink_trade.window_queue.is_none());
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
        CREATE PUMP pu_sampling AS
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
        CREATE SOURCE READER tcp_source_trade FOR source_trade
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
    let body_pile = body_pile.lock().unwrap();

    assert!(body_pile
        .iter()
        .any(|body| !body.tasks.is_empty() && !body.queues.is_empty()));

    let task_pu_sampling = body_pile
        .iter()
        .find_map(|body| body.tasks.iter().find(|task| task.id == "pu_sampling"))
        .expect("at least 1 should exist");
    assert_eq!(task_pu_sampling.type_, "pump-task");

    let task_tcp_source_trade = body_pile
        .iter()
        .find_map(|body| body.tasks.iter().find(|task| task.id == "tcp_source_trade"))
        .expect("at least 1 should exist");
    assert_eq!(task_tcp_source_trade.type_, "source-task");

    let task_tcp_sink_trade = body_pile
        .iter()
        .find_map(|body| body.tasks.iter().find(|task| task.id == "tcp_sink_trade"))
        .expect("at least 1 should exist");
    assert_eq!(task_tcp_sink_trade.type_, "sink-task");

    let non_empty_q_pu_sampling = body_pile
        .iter()
        .find_map(|body| {
            body.queues.iter().find(|queue| {
                queue.id == "pu_sampling"
                    && queue.window_queue.as_ref().unwrap().num_rows_waiting > 0
            })
        })
        .expect("at least 1 should exist (but in very rare case input queue is always empty)");
    assert_eq!(non_empty_q_pu_sampling.upstream_task_id, "tcp_source_trade");
    assert_eq!(non_empty_q_pu_sampling.downstream_task_id, "pu_sampling");
    assert!(
        non_empty_q_pu_sampling
            .window_queue
            .as_ref()
            .unwrap()
            .total_bytes
            > 0
    );
    assert!(non_empty_q_pu_sampling.row_queue.is_none());

    let q_tcp_sink_trade = body_pile
        .iter()
        .find_map(|body| {
            body.queues
                .iter()
                .find(|queue| queue.id == "tcp_sink_trade")
        })
        .expect("at least 1 should exist");

    assert_eq!(q_tcp_sink_trade.upstream_task_id, "pu_sampling");
    assert_eq!(q_tcp_sink_trade.downstream_task_id, "tcp_sink_trade");
    assert!(q_tcp_sink_trade.window_queue.is_none());

    if q_tcp_sink_trade.row_queue.as_ref().unwrap().num_rows > 0 {
        assert!(q_tcp_sink_trade.row_queue.as_ref().unwrap().total_bytes > 0);
    } else {
        // often comes here because tcp_sink_trade queue only get input row on pane close (per 10 secs in event time)
        assert!(q_tcp_sink_trade.row_queue.as_ref().unwrap().total_bytes == 0);
    }
}
