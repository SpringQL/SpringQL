// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;
use springql_core_release_test::api::*;
use springql_foreign_service::source::{ForeignSource, ForeignSourceInput};
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

    vec![json_00_1, json_00_2, json_10_1]
}

/// See: <https://github.com/SpringQL/SpringQL/issues/50>
#[test]
fn test_select_list_order_with_aggr() {
    setup_test_logger();

    const QUEUE_NAME: &str = "q";

    let source_input = gen_source_input();

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
        CREATE SINK STREAM sink_sampled_trade_amount (
          ts TIMESTAMP NOT NULL ROWTIME,    
          amount FLOAT NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_passthrough AS
          INSERT INTO sink_sampled_trade_amount (amount, ts)
          SELECT STREAM
            AVG(source_trade.amount) AS avg_amount,
            FLOOR_TIME(source_trade.ts, DURATION_SECS(10)) AS sampled_ts
          FROM source_trade
          GROUP BY sampled_ts
          FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER queue_sink_trade FOR sink_sampled_trade_amount
          TYPE IN_MEMORY_QUEUE OPTIONS (
            NAME '{queue_name}'
          );
        ",
            queue_name = QUEUE_NAME
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
    test_source.start(ForeignSourceInput::new_fifo_batch(source_input));

    let row = pipeline.pop(QUEUE_NAME).unwrap();
    assert_eq!(
        row.get_not_null_by_index::<String>(0).unwrap(),
        "2020-01-01 00:00:00.000000000"
    );
    assert_eq!(row.get_not_null_by_index::<i32>(1).unwrap(), 20);
}
