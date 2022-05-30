// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;
use springql_core::api::error::Result;
use springql_core::api::low_level_rs::*;
use springql_foreign_service::sink::ForeignSink;
use springql_foreign_service::source::source_input::ForeignSourceInput;
use springql_foreign_service::source::ForeignSource;
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

    let _pipeline = apply_ddls_low_level(&ddls, spring_config_default());
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

    let _pipeline = apply_ddls_low_level(&ddls, spring_config_default());
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

    let pipeline = apply_ddls_low_level(&ddls, spring_config_default());
    test_source.start(ForeignSourceInput::new_fifo_batch(
        (0..trade_times)
            .into_iter()
            .map(|_| json_oracle.clone())
            .collect(),
    ));

    for _ in 0..trade_times {
        let row = spring_pop(&pipeline, queue_name).unwrap();
        assert_eq!(spring_column_text(&row, 0).unwrap(), ts);
        assert_eq!(spring_column_i32(&row, 1).unwrap(), amount);
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

    let pipeline = apply_ddls_low_level(&ddls, spring_config_default());
    test_source.start(ForeignSourceInput::new_fifo_batch(
        (0..trade_times)
            .into_iter()
            .map(|_| json_oracle.clone())
            .collect(),
    ));

    for _ in 0..trade_times {
        loop {
            match spring_pop_non_blocking(&pipeline, queue_name).unwrap() {
                Some(row) => {
                    assert_eq!(spring_column_text(&row, 0).unwrap(), ts);
                    assert_eq!(spring_column_i32(&row, 1).unwrap(), amount);
                    break;
                }
                None => {}
            }
        }
    }
}

#[test]
fn test_e2e_spring_column_apis() {
    setup_test_logger();

    let queue_name = "queue_many_types";
    let c_timestamp = "2021-11-04 23:02:52.123456789";
    let c_integer = i32::MAX;
    let c_text = "HELLO";
    let c_boolean = true;
    let c_float = f32::MAX;

    let json_many_types = json!({
        "c_timestamp": c_timestamp,
        "c_integer": c_integer,
        "c_text": c_text,
        "c_boolean": c_boolean,
        "c_float": c_float,
    });

    let test_source = ForeignSource::new().unwrap();

    let ddls = vec![
        "
        CREATE SOURCE STREAM source_many_types (
          c_timestamp TIMESTAMP NOT NULL ROWTIME,    
          c_integer INTEGER NOT NULL,
          c_text TEXT NOT NULL,
          c_boolean BOOLEAN NOT NULL,
          c_float FLOAT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_many_types (
          c_timestamp TIMESTAMP NOT NULL ROWTIME,    
          c_integer INTEGER NOT NULL,
          c_text TEXT NOT NULL,
          c_boolean BOOLEAN NOT NULL,
          c_float FLOAT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_through AS
          INSERT INTO sink_many_types (c_timestamp, c_integer, c_text, c_boolean, c_float)
          SELECT STREAM source_many_types.c_timestamp, source_many_types.c_integer, source_many_types.c_text, source_many_types.c_boolean, source_many_types.c_float
            FROM source_many_types;
        "
        .to_string(),
        format!(
            "
      CREATE SINK WRITER queue_sink_many_types FOR sink_many_types
        TYPE IN_MEMORY_QUEUE OPTIONS (
          NAME '{queue_name}'
      );
      ",
            queue_name = queue_name,
        ),
        format!(
            "
        CREATE SOURCE READER tcp_many_types FOR source_many_types
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

    let pipeline = apply_ddls_low_level(&ddls, spring_config_default());
    test_source.start(ForeignSourceInput::new_fifo_batch(vec![json_many_types]));

    let row = spring_pop(&pipeline, queue_name).unwrap();
    assert_eq!(spring_column_text(&row, 0).unwrap(), c_timestamp); // TIMESTAMP type is usually converted into string in low-level API.
    assert_eq!(spring_column_i32(&row, 1).unwrap(), c_integer);
    assert_eq!(spring_column_text(&row, 2).unwrap(), c_text);
    assert_eq!(spring_column_bool(&row, 3).unwrap(), c_boolean);
    assert_eq!(spring_column_f32(&row, 4).unwrap(), c_float);
}
