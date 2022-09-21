// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;
use springql_core_release_test::api::{error::Result, *};
use springql_foreign_service::{
    sink::ForeignSink,
    source::{ForeignSource, ForeignSourceInput},
};
use springql_test_logger::setup_test_logger;

use crate::test_support::*;

fn gen_source_input() -> Vec<serde_json::Value> {
    let json_00_1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
        "ticker": "ORCL",
        "amount": 10,
    });
    let json_00_2 = json!({
        "ts": "2020-01-01 00:00:09.999999999",
        "ticker": "GOOGL",
        "amount": 30,
    });
    let json_10_1 = json!({
        "ts": "2020-01-01 00:00:10.000000000",
        "ticker": "IBM",
        "amount": 50,
    });
    let json_20_1 = json!({
        "ts": "2020-01-01 00:00:20.000000000",
        "ticker": "IBM",
        "amount": 70,
    });

    vec![json_00_1, json_00_2, json_10_1, json_20_1]
}

fn run_and_drain(
    ddls: &[String],
    source_input: ForeignSourceInput,
    test_source: ForeignSource,
    test_sink: &ForeignSink,
) -> Vec<serde_json::Value> {
    let _pipeline = apply_ddls(ddls, SpringConfig::default());
    test_source.start(source_input);
    drain_from_sink(test_sink)
}

/// See: <https://github.com/SpringQL/SpringQL/issues/126>
#[test]
fn test_feat_aggregation_without_group_by() -> Result<()> {
    setup_test_logger();

    let source_input = gen_source_input();

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
        CREATE SINK STREAM sink_avg_all (
          avg_amount FLOAT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP avg_all AS
        INSERT INTO sink_avg_all (avg_amount)
        SELECT STREAM
            AVG(source_trade.amount) AS avg_amount
        FROM source_trade
        FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_trade FOR sink_avg_all
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

    let sink_received = run_and_drain(
        &ddls,
        ForeignSourceInput::new_fifo_batch(source_input),
        test_source,
        &test_sink,
    );

    assert_eq!(sink_received.len(), 2);

    assert_eq!(
        sink_received[0]["avg_amount"].as_f64().unwrap().round() as i32,
        20,
    );

    assert_eq!(
        sink_received[1]["avg_amount"].as_f64().unwrap().round() as i32,
        50,
    );

    Ok(())
}

#[test]
fn test_feat_aggregation_with_2_group_bys() -> Result<()> {
    setup_test_logger();

    let source_input = gen_source_input();

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
        CREATE SINK STREAM sink_avg_by_ticker (
            ts TIMESTAMP NOT NULL ROWTIME,    
            ticker TEXT NOT NULL,
            avg_amount FLOAT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP avg_by_ticker AS
        INSERT INTO sink_avg_by_ticker (ts, ticker, avg_amount)
        SELECT STREAM
            FLOOR_TIME(source_trade.ts, DURATION_SECS(10)) AS min_ts,
            source_trade.ticker AS ticker,
            AVG(source_trade.amount) AS avg_amount
        FROM source_trade
        GROUP BY min_ts, ticker
        FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_trade FOR sink_avg_by_ticker
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

    let mut sink_received = run_and_drain(
        &ddls,
        ForeignSourceInput::new_fifo_batch(source_input),
        test_source,
        &test_sink,
    );
    sink_received.sort_by_key(|r| {
        (
            r["ts"].as_str().unwrap().to_string(),
            r["ticker"].as_str().unwrap().to_string(),
        )
    });

    assert_eq!(sink_received.len(), 3);

    assert_eq!(
        sink_received[0]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:00.000000000",
    );
    assert_eq!(sink_received[0]["ticker"].as_str().unwrap(), "GOOGL",);
    assert_eq!(
        sink_received[0]["avg_amount"].as_f64().unwrap().round() as i32,
        30,
    );

    assert_eq!(
        sink_received[1]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:00.000000000",
    );
    assert_eq!(sink_received[1]["ticker"].as_str().unwrap(), "ORCL",);
    assert_eq!(
        sink_received[1]["avg_amount"].as_f64().unwrap().round() as i32,
        10,
    );

    assert_eq!(
        sink_received[2]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:10.000000000",
    );
    assert_eq!(sink_received[2]["ticker"].as_str().unwrap(), "IBM",);
    assert_eq!(
        sink_received[2]["avg_amount"].as_f64().unwrap().round() as i32,
        50,
    );

    Ok(())
}
