// Copyright (c) 2021 TOYOTA MOTOR CORPORATION. Licensed under MIT OR Apache-2.0.

mod test_support;

use std::thread;
use std::time::Duration;

use serde_json::json;
use springql_core::low_level_rs::*;
use springql_foreign_service::sink::ForeignSink;
use springql_foreign_service::source::source_input::ForeignSourceInput;
use springql_foreign_service::source::ForeignSource;
use springql_test_logger::setup_test_logger;

use crate::test_support::*;

/// every row has the same timestamp (a time-based window preserves rows forever)
fn _gen_source_input(n: u64) -> impl Iterator<Item = serde_json::Value> {
    (0..n).map(move |_| {
        let ts = "2020-01-01 00:00:00.0000000000".to_string();
        let ticker = String::from_iter(std::iter::repeat('X').take(1000));
        let amount = 100;
        json!({
            "ts": ts,
            "ticker": ticker,
            "amount": amount,
        })
    })
}

fn t(n_in_rows: u64, upper_limit_bytes: u64) {
    setup_test_logger();

    let source_trade = _gen_source_input(n_in_rows).collect();

    let test_source_trade =
        ForeignSource::start(ForeignSourceInput::new_fifo_batch(source_trade)).unwrap();
    let test_source_city_temperature =
        ForeignSource::start(ForeignSourceInput::new_fifo_batch(vec![])).unwrap();

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
          temperature INTEGER
        );
        "
        .to_string(),
        // input rows have the same timestamps, so the window preserves all rows until purge
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

    let mut config = spring_config_default();
    config.memory.upper_limit_bytes = upper_limit_bytes;

    let _pipeline = apply_ddls(&ddls, config);
    thread::sleep(Duration::from_secs(100));
}

#[test]
fn test_feat_purger() {
    t(10000, 100000)
}
