use serde_json::json;
use springql_core::error::Result;
use springql_core::low_level_rs::*;
use test_foreign_service::sink::TestForeignSink;
use test_foreign_service::source::TestForeignSource;
use test_logger::setup_test_logger;

fn apply_ddls(ddls: &[String]) -> SpringPipeline {
    let pipeline = spring_open().unwrap();
    for ddl in ddls {
        spring_command(&pipeline, ddl).unwrap();
    }
    pipeline
}

fn drain_from_sink(sink: &TestForeignSink) -> Vec<serde_json::Value> {
    let mut received = Vec::new();
    while let Ok(v) = sink.receive() {
        received.push(v);
    }
    received
}

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

    let test_source = TestForeignSource::start(source_input.clone()).unwrap();
    let test_sink = TestForeignSink::start().unwrap();

    let ddls = vec![
        format!(
            "
        CREATE SOURCE STREAM source_trade (
          ts TIMESTAMP NOT NULL ROWTIME,    
          ticker TEXT NOT NULL,
          amount INTEGER NOT NULL
        ) SERVER NET_SERVER OPTIONS (
          PROTOCOL 'TCP',
          REMOTE_HOST '{remote_host}',
          REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = test_source.host_ip(),
            remote_port = test_source.port()
        ),
        format!(
            "
        CREATE SINK STREAM sink_trade (
          ts TIMESTAMP NOT NULL,    
          ticker TEXT NOT NULL,
          amount INTEGER NOT NULL
        ) SERVER NET_SERVER OPTIONS (
          PROTOCOL 'TCP',
          REMOTE_HOST '{remote_host}',
          REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = test_sink.host_ip(),
            remote_port = test_sink.port()
        ),
        "
        CREATE PUMP pu_passthrough AS
          INSERT INTO sink_trade (ts, ticker, amount)
          SELECT STREAM ts, ticker, amount FROM source_trade;
        "
        .to_string(),
        "
        ALTER PUMP pu_passthrough START;
        "
        .to_string(),
    ];

    let _pipeline = apply_ddls(&ddls);
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

    let test_source = TestForeignSource::start(vec![json_oracle]).unwrap();
    let test_sink = TestForeignSink::start().unwrap();

    let ddls = vec![
        format!(
            "
        CREATE SOURCE STREAM source_trade (
          ts TIMESTAMP NOT NULL ROWTIME,    
          ticker TEXT NOT NULL,
          amount INTEGER NOT NULL
        ) SERVER NET_SERVER OPTIONS (
          PROTOCOL 'TCP',
          REMOTE_HOST '{remote_host}',
          REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = test_source.host_ip(),
            remote_port = test_source.port()
        ),
        format!(
            "
        CREATE SINK STREAM sink_trade (
          ts TIMESTAMP NOT NULL,    
          ticker TEXT NOT NULL
        ) SERVER NET_SERVER OPTIONS (
          PROTOCOL 'TCP',
          REMOTE_HOST '{remote_host}',
          REMOTE_PORT '{remote_port}'
        );
        ",
            remote_host = test_sink.host_ip(),
            remote_port = test_sink.port()
        ),
        "
        CREATE PUMP pu_projection AS
          INSERT INTO sink_trade (ts, ticker)
          SELECT STREAM ts, ticker FROM source_trade;
        "
        .to_string(),
        "
        ALTER PUMP pu_projection START;
        "
        .to_string(),
    ];

    let _pipeline = apply_ddls(&ddls);
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
