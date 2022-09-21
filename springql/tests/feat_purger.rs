// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use std::thread;
use std::time::Duration;

use log::LevelFilter;
use serde_json::json;
use springql_core_release_test::api::*;
use springql_foreign_service::{
    sink::ForeignSink,
    source::{ForeignSource, ForeignSourceInput},
};
use springql_test_logger::setup_test_logger_with_level;

use crate::test_support::*;

/// every row has the same timestamp (a time-based window preserves rows forever)
fn _gen_source_input(n: u64) -> impl Iterator<Item = serde_json::Value> {
    (0..n).map(move |_| {
        let ts = "2020-01-01 00:00:00.000000000".to_string();
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
    setup_test_logger_with_level(LevelFilter::Warn);

    let source_trade = _gen_source_input(n_in_rows).collect();

    let test_source_trade = ForeignSource::new().unwrap();
    let test_source_city_temperature = ForeignSource::new().unwrap();

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
            remote_host = test_source_trade.host_ip(),
            remote_port = test_source_trade.port()
        ),
        format!(
            "
        CREATE SOURCE READER tcp_source_city_temperature FOR source_city_temperature
          TYPE NET_CLIENT OPTIONS (
            PROTOCOL 'TCP',
            REMOTE_HOST '{remote_host}',
            REMOTE_PORT '{remote_port}'
        );
      ",
            remote_host = test_source_city_temperature.host_ip(),
            remote_port = test_source_city_temperature.port()
        ),
    ];

    let mut config = SpringConfig::default();
    config.memory.upper_limit_bytes = upper_limit_bytes;
    config.memory.severe_to_critical_percent = 60;
    config.memory.moderate_to_severe_percent = 30;
    config.memory.critical_to_severe_percent = 50;
    config.memory.severe_to_moderate_percent = 20;

    let _pipeline = apply_ddls(&ddls, config);
    test_source_trade.start(ForeignSourceInput::new_fifo_batch(source_trade));
    test_source_city_temperature.start(ForeignSourceInput::new_fifo_batch(vec![]));
    thread::sleep(Duration::from_secs(10));
}

#[test]
fn test_feat_purger() {
    t(10000, 100000)
}
