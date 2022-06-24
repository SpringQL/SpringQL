// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use springql::{SpringConfig, SpringPipeline, SpringSourceRow};

use crate::test_support::*;

fn pipeline() -> SpringPipeline {
    let ddls = vec![
        "
        CREATE SOURCE STREAM source_1 (
          ts TIMESTAMP NOT NULL ROWTIME,
          n INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE SINK STREAM sink_1 (
          ts TIMESTAMP NOT NULL ROWTIME,
          n INTEGER NOT NULL
        );
        "
        .to_string(),
        "
        CREATE PUMP pu_1 AS
          INSERT INTO sink_1 (ts, n)
          SELECT STREAM source_1.ts, source_1.n FROM source_1;
        "
        .to_string(),
        format!(
            "
        CREATE SINK WRITER q_sink_1 FOR sink_1
          TYPE IN_MEMORY_QUEUE OPTIONS (
            NAME 'q_sink_1'
        );
        ",
        ),
        format!(
            "
        CREATE SOURCE READER q_source_1 FOR source_1
          TYPE IN_MEMORY_QUEUE OPTIONS (
            NAME 'q_source_1'
          );
        ",
        ),
    ];

    apply_ddls(&ddls, SpringConfig::default())
}

#[test]
fn test_source_row_from_json() {
    let pipeline = pipeline();

    let source_rows = vec![
        SpringSourceRow::from_json(r#"{"ts": "2022-01-01 13:00:00.000000000", "n": 42}"#).unwrap(),
        SpringSourceRow::from_json(r#"{"ts": "2022-01-01 14:00:00.000000000", "n": 43}"#).unwrap(),
    ];

    for row in source_rows {
        pipeline.push("q_source_1", row).unwrap();
    }

    let sink_row1 = pipeline.pop("q_sink_1").unwrap();
    let sink_row2 = pipeline.pop("q_sink_1").unwrap();

    assert_eq!(
        sink_row1.get_not_null_by_index::<String>(0).unwrap(),
        "2022-01-01 13:00:00.000000000",
    );
    assert_eq!(sink_row1.get_not_null_by_index::<i32>(1).unwrap(), 42);

    assert_eq!(
        sink_row2.get_not_null_by_index::<String>(0).unwrap(),
        "2022-01-01 14:00:00.000000000",
    );
    assert_eq!(sink_row2.get_not_null_by_index::<i32>(1).unwrap(), 43);
}
