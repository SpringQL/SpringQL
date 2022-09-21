// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;

use springql_foreign_service::{
    sink::ForeignSink,
    source::{ForeignSource, ForeignSourceInput},
};
use springql_release_test::{Result, SpringConfig};
use springql_test_logger::setup_test_logger;

use crate::test_support::*;

#[test]
fn test_e2e_source_sink() -> Result<()> {
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

    let _pipeline = apply_ddls(&ddls, SpringConfig::default());
    test_source.start(ForeignSourceInput::new_fifo_batch(source_input.clone()));
    let sink_received = drain_from_sink(&test_sink);

    // because worker takes source input in multi-thread, order may be changed
    assert!(sink_received.contains(source_input.get(0).unwrap()));
    assert!(sink_received.contains(source_input.get(1).unwrap()));
    assert!(sink_received.contains(source_input.get(2).unwrap()));

    Ok(())
}

#[test]
fn test_e2e_projection() -> Result<()> {
    setup_test_logger();

    let json_oracle = json!({
        "ts": "2021-11-04 23:02:52.123456789",
        "ticker": "ORCL",
        "amount": 20,
    });

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
          ticker TEXT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_projection AS
          INSERT INTO sink_trade (ts, ticker)
          SELECT STREAM source_trade.ts, source_trade.ticker FROM source_trade;
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

    let _pipeline = apply_ddls(&ddls, SpringConfig::default());
    test_source.start(ForeignSourceInput::new_fifo_batch(vec![json_oracle]));
    let sink_received = drain_from_sink(&test_sink);

    assert_eq!(
        sink_received.get(0).unwrap(),
        &json!({
            "ts": "2021-11-04 23:02:52.123456789",
            "ticker": "ORCL"
        })
    );

    Ok(())
}

#[test]
fn test_e2e_pop_from_in_memory_queue() {
    setup_test_logger();

    let queue_name = "queue_trade";
    let ts = "2021-11-04 23:02:52.123456789";
    let ticker = "ORCL";
    let amount = 20;

    let json_oracle = json!({
        "ts": ts,
        "ticker": ticker,
        "amount": amount,
    });
    let trade_times = 5;

    let test_source = ForeignSource::new().unwrap();

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
          ts TIMESTAMP NOT NULL,    
          amount INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_projection AS
          INSERT INTO sink_trade (ts, amount)
          SELECT STREAM source_trade.ts, source_trade.amount FROM source_trade;
        "
        .to_string(),
        format!(
            "
      CREATE SINK WRITER queue_sink_trade FOR sink_trade
        TYPE IN_MEMORY_QUEUE OPTIONS (
          NAME '{queue_name}'
      );
      ",
            queue_name = queue_name,
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

    let pipeline = apply_ddls(&ddls, SpringConfig::default());
    test_source.start(ForeignSourceInput::new_fifo_batch(
        (0..trade_times)
            .into_iter()
            .map(|_| json_oracle.clone())
            .collect(),
    ));

    for _ in 0..trade_times {
        let row = pipeline.pop(queue_name).unwrap();
        assert_eq!(row.get_not_null_by_index::<String>(0).unwrap(), ts);
        assert_eq!(row.get_not_null_by_index::<i32>(1).unwrap(), amount);
    }
}

#[test]
fn test_e2e_pop_non_blocking_from_in_memory_queue() {
    setup_test_logger();

    let queue_name = "queue_trade_nb"; // FIXME using the same name as in test_e2e_pop_from_in_memory_queue causes panic
    let ts = "2021-11-04 23:02:52.123456789";
    let ticker = "ORCL";
    let amount = 20;

    let json_oracle = json!({
        "ts": ts,
        "ticker": ticker,
        "amount": amount,
    });
    let trade_times = 5;

    let test_source = ForeignSource::new().unwrap();

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
          ts TIMESTAMP NOT NULL,    
          amount INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_projection AS
          INSERT INTO sink_trade (ts, amount)
          SELECT STREAM source_trade.ts, source_trade.amount FROM source_trade;
        "
        .to_string(),
        format!(
            "
      CREATE SINK WRITER queue_sink_trade FOR sink_trade
        TYPE IN_MEMORY_QUEUE OPTIONS (
          NAME '{queue_name}'
      );
      ",
            queue_name = queue_name,
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

    let pipeline = apply_ddls(&ddls, SpringConfig::default());
    test_source.start(ForeignSourceInput::new_fifo_batch(
        (0..trade_times)
            .into_iter()
            .map(|_| json_oracle.clone())
            .collect(),
    ));

    for _ in 0..trade_times {
        loop {
            match pipeline.pop_non_blocking(queue_name).unwrap() {
                Some(row) => {
                    assert_eq!(row.get_not_null_by_index::<String>(0).unwrap(), ts);
                    assert_eq!(row.get_not_null_by_index::<i32>(1).unwrap(), amount);
                    break;
                }
                None => {}
            }
        }
    }
}
