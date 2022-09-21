// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;
use springql_foreign_service::{
    sink::ForeignSink,
    source::{ForeignSource, ForeignSourceInput},
};
use springql_release_test::*;
use springql_test_logger::setup_test_logger;
use time::macros::format_description;

use crate::test_support::*;

fn gen_source_input() -> Vec<serde_json::Value> {
    let json1 = json!({
        "ticker": "ORCL",
        "amount": 10,
    });
    let json2 = json!({
        "ticker": "GOOGL",
        "amount": 30,
    });
    let json3 = json!({
        "ticker": "IBM",
        "amount": 50,
    });
    let json4 = json!({
        "ticker": "IBM",
        "amount": 70,
    });

    vec![json1, json2, json3, json4]
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
fn test_feat_processing_time_ptime() -> Result<()> {
    setup_test_logger();

    let source_input = gen_source_input();

    let test_source = ForeignSource::new().unwrap();
    let test_sink = ForeignSink::start().unwrap();

    let ddls = vec![
        "
        CREATE SOURCE STREAM source_trade (
          ticker TEXT NOT NULL,
          amount INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_ptime (
          source_trade_ptime TIMESTAMP NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP select_ptime AS
        INSERT INTO sink_ptime (source_trade_ptime)
        SELECT STREAM source_trade.ptime FROM source_trade;
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER tcp_sink_ptime FOR sink_ptime
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

    assert_eq!(sink_received.len(), 4);

    let ptimes = sink_received
        .into_iter()
        .map(|json| {
            let ptime = json["source_trade_ptime"].as_str().unwrap();
            time::PrimitiveDateTime::parse(
                ptime,
                format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:9]"
                ),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let ptimes_sorted = {
        let mut s = ptimes.clone();
        s.sort();
        s
    };

    assert_eq!(ptimes, ptimes_sorted);

    Ok(())
}
