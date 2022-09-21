// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.
#![allow(unused_imports)] // conditional compilation used in this mod

mod test_support;

use float_cmp::approx_eq;
use pretty_assertions::assert_eq;
use rand::prelude::{IteratorRandom, SliceRandom};
use serde_json::json;
use time::{macros::format_description, OffsetDateTime, PrimitiveDateTime};

use crate::test_support::{apply_ddls, drain_from_sink};
use springql_foreign_service::{
    sink::ForeignSink,
    source::{ForeignSource, ForeignSourceInput},
};
use springql_release_test::*;
use springql_test_logger::setup_test_logger;

#[cfg(feature = "stub_web_console")]
use springql_core_release_test::stubed_requests;

/// 1 sec tick data
#[cfg(feature = "stub_web_console")]
fn _gen_source_input(n: u64) -> impl Iterator<Item = serde_json::Value> {
    let mut rng = rand::thread_rng();

    (0..n).map(move |i| {
        let odt = OffsetDateTime::from_unix_timestamp_nanos((i as i128) * 1_000_000_000).unwrap();
        let ts = PrimitiveDateTime::new(odt.date(), odt.time());
        let ts = ts
            .format(format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:9]"
            ))
            .unwrap();

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

#[cfg(feature = "stub_web_console")]
fn _config() -> SpringConfig {
    let mut config = SpringConfig::default();
    config.web_console.enable_report_post = true;
    config.web_console.report_interval_msec = 100;
    config.web_console.host = "localhost".to_string();
    config.web_console.port = 12345;
    config
}

#[cfg(feature = "stub_web_console")]
fn start_pile_reports(
    ddls: &[String],
    source_input: ForeignSourceInput,
    test_source: ForeignSource,
    test_sink: &ForeignSink,
) {
    let config = _config();
    let _pipeline = apply_ddls(ddls, config);
    test_source.start(source_input);
    let _sink_received = drain_from_sink(test_sink);
}

#[cfg(feature = "stub_web_console")]
#[test]
fn test_performance_metrics_report_floor_time() {
    setup_test_logger();

    let source_input = _gen_source_input(10000).collect();

    let test_source = ForeignSource::new().unwrap();
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
        CREATE SOURCE READER tcp_source_trade FOR source_trade
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

    start_pile_reports(
        &ddls,
        ForeignSourceInput::new_fifo_batch(source_input),
        test_source,
        &test_sink,
    );
    let request_values = stubed_requests();
    let mut body_pile = Vec::new();
    for val in request_values {
        let body: crate::test_support::request_body::PostTaskGraphBody =
            serde_json::from_value(val).unwrap();
        body_pile.push(body);
    }

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
                queue.id == "pu_floor_time-source_trade"
                    && queue.row_queue.as_ref().unwrap().num_rows > 0
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

#[cfg(feature = "stub_web_console")]
#[test]
fn test_performance_metrics_report_sampling() {
    setup_test_logger();

    let source_input = _gen_source_input(100000).collect();

    let test_source = ForeignSource::new().unwrap();
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
          amount FLOAT NOT NULL
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
        CREATE SOURCE READER tcp_source_trade FOR source_trade
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

    start_pile_reports(
        &ddls,
        ForeignSourceInput::new_fifo_batch(source_input),
        test_source,
        &test_sink,
    );
    let request_values = stubed_requests();
    let mut body_pile = Vec::new();
    for val in request_values {
        let body: crate::test_support::request_body::PostTaskGraphBody =
            serde_json::from_value(val).unwrap();
        body_pile.push(body);
    }

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
                queue.id == "pu_sampling-source_trade"
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
