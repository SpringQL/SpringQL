// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use pretty_assertions::assert_eq;
use serde_json::json;
use springql_foreign_service::source::{ForeignSource, ForeignSourceInput};
use springql_release_test::*;
use springql_test_logger::setup_test_logger;

use crate::test_support::apply_ddls;

/// See: <https://github.com/SpringQL/SpringQL/issues/132>
#[test]
fn test_feat_split_from_source() {
    setup_test_logger();

    let json1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
        "c": 42,
    });
    let source_input = vec![json1];

    let test_source = ForeignSource::new().unwrap();

    let ddls = vec![
        "
        CREATE SOURCE STREAM source_1 (
          ts TIMESTAMP NOT NULL ROWTIME,
          c INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_1 (
          ts TIMESTAMP NOT NULL ROWTIME,
          c_mul_10 INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_2 (
          ts TIMESTAMP NOT NULL ROWTIME,
          c_add_1 INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_mul AS
          INSERT INTO sink_1 (ts, c_mul_10)
          SELECT STREAM source_1.ts, source_1.c * 10
              FROM source_1;
        "
        .to_string(),
        "
        CREATE PUMP pu_add AS
          INSERT INTO sink_2 (ts, c_add_1)
          SELECT STREAM source_1.ts, source_1.c + 1
              FROM source_1;
        "
        .to_string(),
        "
        CREATE SINK WRITER q_sink_1 FOR sink_1
          TYPE IN_MEMORY_QUEUE OPTIONS (
            NAME 'q1'
        );
        "
        .to_string(),
        "
        CREATE SINK WRITER q_sink_2 FOR sink_2
          TYPE IN_MEMORY_QUEUE OPTIONS (
            NAME 'q2'
        );
        "
        .to_string(),
        format!(
            "
        CREATE SOURCE READER tcp_1 FOR source_1
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

    let row = pipeline.pop("q1").unwrap();
    assert_eq!(row.get_not_null_by_index::<i32>(1).unwrap(), 42 * 10);

    let row = pipeline.pop("q2").unwrap();
    assert_eq!(row.get_not_null_by_index::<i32>(1).unwrap(), 42 + 1);
}
