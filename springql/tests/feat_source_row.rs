// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use std::str::FromStr;

use springql_release_test::{
    SpringConfig, SpringError, SpringPipeline, SpringSourceRow, SpringSourceRowBuilder,
    SpringTimestamp,
};

use crate::test_support::*;

// Requires queue name parameters for: <https://github.com/SpringQL/SpringQL/issues/219>
fn pipeline(source_queue_name: &str, sink_queue_name: &str) -> SpringPipeline {
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
            NAME '{}'
        );
        ",
            sink_queue_name
        ),
        format!(
            "
        CREATE SOURCE READER q_source_1 FOR source_1
          TYPE IN_MEMORY_QUEUE OPTIONS (
            NAME '{}'
          );
        ",
            source_queue_name
        ),
    ];

    apply_ddls(&ddls, SpringConfig::default())
}

#[test]
fn test_source_row_from_json() {
    let pipeline = pipeline("q_source_from_json", "q_sink_from_json");

    let source_rows = vec![
        SpringSourceRow::from_json(r#"{"ts": "2022-01-01 13:00:00.000000000", "n": 42}"#).unwrap(),
        SpringSourceRow::from_json(r#"{"ts": "2022-01-01 14:00:00.000000000", "n": 43}"#).unwrap(),
    ];

    for row in source_rows {
        pipeline.push("q_source_from_json", row).unwrap();
    }

    let sink_row1 = pipeline.pop("q_sink_from_json").unwrap();
    let sink_row2 = pipeline.pop("q_sink_from_json").unwrap();

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

#[test]
fn test_source_row_from_builder() -> Result<(), SpringError> {
    let pipeline = pipeline("q_source_from_builder", "q_sink_from_builder");

    let source_rows = vec![
        SpringSourceRowBuilder::default()
            .add_column(
                "ts",
                SpringTimestamp::from_str("2022-01-01 13:00:00.000000000").unwrap(),
            )?
            .add_column("n", 42i32)?
            .build(),
        SpringSourceRowBuilder::default()
            .add_column(
                "ts",
                SpringTimestamp::from_str("2022-01-01 14:00:00.000000000").unwrap(),
            )?
            .add_column("n", 43i32)?
            .build(),
    ];

    for row in source_rows {
        pipeline.push("q_source_from_builder", row).unwrap();
    }

    let sink_row1 = pipeline.pop("q_sink_from_builder").unwrap();
    let sink_row2 = pipeline.pop("q_sink_from_builder").unwrap();

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

    Ok(())
}
