// Copyright (c) 2022 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;
use springql_core::error::Result;
use springql_core::low_level_rs::*;
use springql_foreign_service::sink::ForeignSink;
use springql_foreign_service::source::source_input::ForeignSourceInput;
use springql_foreign_service::source::ForeignSource;
use springql_test_logger::setup_test_logger;

use crate::test_support::*;

fn gen_source_input() -> Vec<serde_json::Value> {
    let json_00_1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
        "ticker": "ORCL",
        "amount": 10,
    });
    let json_00_2 = json!({
        "ts": "2020-01-01 00:00:09.9999999999",
        "ticker": "GOOGL",
        "amount": 30,
    });
    let json_10_1 = json!({
        "ts": "2020-01-01 00:00:10.0000000000",
        "ticker": "IBM",
        "amount": 50,
    });
    let json_20_1 = json!({
        "ts": "2020-01-01 00:00:20.0000000000",
        "ticker": "IBM",
        "amount": 70,
    });

    vec![json_00_1, json_00_2, json_10_1, json_20_1]
}

fn run_and_drain(ddls: &[String], test_sink: &ForeignSink) -> Vec<serde_json::Value> {
    let _pipeline = apply_ddls(ddls, spring_config_default());
    let mut sink_received = drain_from_sink(test_sink);
    sink_received.sort_by_key(|r| {
        let ts = &r["ts"];
        ts.as_str().unwrap().to_string()
    });
    sink_received
}

#[test]
fn test_e2e_sampling_fixed_window() -> Result<()> {
    setup_test_logger();

    let source_input = gen_source_input();

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
          amount FLOAT NOT NULL
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

    let sink_received = run_and_drain(&ddls, &test_sink);

    assert_eq!(sink_received.len(), 2);

    assert_eq!(
        sink_received[0]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:00.000000000"
    );
    assert_eq!(
        sink_received[0]["amount"].as_f64().unwrap().round() as i32,
        20,
    );

    assert_eq!(
        sink_received[1]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:10.000000000"
    );
    assert_eq!(
        sink_received[1]["amount"].as_f64().unwrap().round() as i32,
        50,
    );

    Ok(())
}

#[test]
fn test_e2e_sampling_sliding_window() -> Result<()> {
    setup_test_logger();

    let source_input = gen_source_input();

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
          amount FLOAT NOT NULL
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
          SLIDING WINDOW DURATION_SECS(10), DURATION_SECS(5), DURATION_SECS(0);
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

    let sink_received = run_and_drain(&ddls, &test_sink);

    assert_eq!(sink_received.len(), 3);

    assert_eq!(
        sink_received[0]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:00.000000000"
    );
    assert_eq!(
        sink_received[0]["amount"].as_f64().unwrap().round() as i32,
        20,
    );

    assert_eq!(
        sink_received[1]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:00.000000000"
    );
    assert_eq!(
        sink_received[1]["amount"].as_f64().unwrap().round() as i32,
        40,
    );

    assert_eq!(
        sink_received[2]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:10.000000000"
    );
    assert_eq!(
        sink_received[2]["amount"].as_f64().unwrap().round() as i32,
        50,
    );

    Ok(())
}

#[test]
fn test_e2e_sampling_pump_insert_to_window_queue() -> Result<()> {
    setup_test_logger();

    let source_input = gen_source_input();

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
        CREATE STREAM trade_amount (
          ts TIMESTAMP NOT NULL ROWTIME,    
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
        CREATE PUMP pu_projection AS
          INSERT INTO trade_amount (ts, amount)
          SELECT STREAM
            source_trade.ts,
            source_trade.amount
          FROM source_trade;
        "
        .to_string(),
        "
        CREATE PUMP pu_sampling AS
          INSERT INTO sink_sampled_trade_amount (ts, amount)
          SELECT STREAM
            FLOOR_TIME(trade_amount.ts, DURATION_SECS(10)) AS sampled_ts,
            AVG(trade_amount.amount) AS avg_amount
          FROM trade_amount
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

    let sink_received = run_and_drain(&ddls, &test_sink);

    assert_eq!(sink_received.len(), 2);

    assert_eq!(
        sink_received[0]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:00.000000000"
    );
    assert_eq!(
        sink_received[0]["amount"].as_f64().unwrap().round() as i32,
        20,
    );

    assert_eq!(
        sink_received[1]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:10.000000000"
    );
    assert_eq!(
        sink_received[1]["amount"].as_f64().unwrap().round() as i32,
        50,
    );

    Ok(())
}
