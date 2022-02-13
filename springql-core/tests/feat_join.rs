// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;
use springql_core::low_level_rs::*;
use springql_foreign_service::sink::ForeignSink;
use springql_foreign_service::source::source_input::ForeignSourceInput;
use springql_foreign_service::source::ForeignSource;
use springql_test_logger::setup_test_logger;

use crate::test_support::*;

fn gen_source_trade() -> Vec<serde_json::Value> {
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

fn gen_source_city_temperature() -> Vec<serde_json::Value> {
    let json_00_1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
        "city": "Tokyo",
        "temperature": -3,
    });
    let json_00_2 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
        "city": "London",
        "temperature": -8,
    });

    vec![json_00_1, json_00_2]
}

fn run_and_drain(ddls: &[String], test_sink: &ForeignSink) -> Vec<serde_json::Value> {
    let _pipeline = apply_ddls(ddls, spring_config_default());
    let mut sink_received = drain_from_sink(test_sink);
    sink_received.sort_by_key(|r| {
        let ts = &r["ts"];
        let temperature = &r["temperature"];
        (
            ts.as_str().unwrap().to_string(),
            temperature.as_i64().unwrap(),
        )
    });
    sink_received
}

#[test]
fn test_e2e_left_outer_join() {
    setup_test_logger();

    let source_trade = gen_source_trade();
    let source_city_temperature = gen_source_city_temperature();

    let test_source_trade =
        ForeignSource::start(ForeignSourceInput::new_fifo_batch(source_trade)).unwrap();
    let test_source_city_temperature =
        ForeignSource::start(ForeignSourceInput::new_fifo_batch(source_city_temperature)).unwrap();

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
        CREATE SOURCE STREAM source_city_temperature (
          ts TIMESTAMP NOT NULL ROWTIME,    
          city TEXT NOT NULL,
          temperature INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_joined (
          ts TIMESTAMP NOT NULL ROWTIME,    
          amount INTEGER NOT NULL,
          temperature INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_join AS
          INSERT INTO sink_joined (ts, amount, temperature)
          SELECT STREAM
            source_trade.ts,
            source_trade.amount,
            source_city_temperature.temperature
          FROM source_trade
          LEFT OUTER JOIN source_city_temperature
            ON source_trade.ts = source_city_temperature.ts
          FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_joined FOR sink_joined
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
            remote_host = test_source_trade.host_ip(),
            remote_port = test_source_trade.port()
        ),
        format!(
            "
        CREATE SOURCE READER tcp_source_city_temperature FOR source_city_temperature
          TYPE NET_SERVER OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
        );
      ",
            remote_host = test_source_city_temperature.host_ip(),
            remote_port = test_source_city_temperature.port()
        ),
    ];

    let sink_received = run_and_drain(&ddls, &test_sink);

    assert_eq!(sink_received.len(), 4);

    let r0 = sink_received[0].clone();
    assert_eq!(r0["ts"].as_str().unwrap(), "2020-01-01 00:00:00.000000000");
    assert_eq!(r0["amount"].as_i64().unwrap(), 10);
    assert_eq!(r0["temperature"].as_i64().unwrap(), -8);

    let r1 = sink_received[1].clone();
    assert_eq!(r1["ts"].as_str().unwrap(), "2020-01-01 00:00:00.000000000");
    assert_eq!(r1["amount"].as_i64().unwrap(), 10);
    assert_eq!(r1["temperature"].as_i64().unwrap(), -3);

    let r2 = sink_received[2].clone();
    assert_eq!(r2["ts"].as_str().unwrap(), "2020-01-01 00:00:09.999999999");
    assert_eq!(r2["amount"].as_i64().unwrap(), 30);
    assert!(r2["temperature"].is_null());

    let r3 = sink_received[3].clone();
    assert_eq!(r3["ts"].as_str().unwrap(), "2020-01-01 00:00:10.000000000");
    assert_eq!(r3["amount"].as_i64().unwrap(), 50);
    assert!(r3["temperature"].is_null());
}
