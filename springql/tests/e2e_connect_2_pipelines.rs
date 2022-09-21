// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

mod test_support;

use serde_json::json;
use springql_foreign_service::{
    sink::ForeignSink,
    source::{ForeignSource, ForeignSourceInput},
};
use springql_release_test::{SpringConfig, SpringPipeline};

use crate::test_support::*;

fn gen_pipeline1_input() -> Vec<serde_json::Value> {
    let json1 = json!({
        "ts": "2020-01-01 00:00:00.000000000",
    });
    let json2 = json!({
        "ts": "2020-01-01 00:00:10.000000000",
    });

    vec![json1, json2]
}

fn pipeline1(test_source: &ForeignSource) -> SpringPipeline {
    let ddls = vec![
        "
        CREATE SOURCE STREAM source_1 (
          ts TIMESTAMP NOT NULL ROWTIME
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
          SELECT STREAM source_1.ts, 42 FROM source_1;
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

    apply_ddls(&ddls, SpringConfig::default())
}

fn pipeline2(test_sink: &ForeignSink) -> SpringPipeline {
    let ddls = vec![
        "
      CREATE SOURCE STREAM source_2 (
        ts TIMESTAMP NOT NULL ROWTIME,
        n INTEGER NOT NULL
      );
      "
        .to_string(),
        "
      CREATE SINK STREAM sink_2 (
        ts TIMESTAMP NOT NULL ROWTIME,
        n INTEGER NOT NULL
      );
      "
        .to_string(),
        "
      CREATE PUMP pu_2 AS
        INSERT INTO sink_2 (ts, n)
        SELECT STREAM source_2.ts, source_2.n * 10 FROM source_2;
      "
        .to_string(),
        format!(
            "
      CREATE SINK WRITER tcp_sink_2 FOR sink_2
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
      CREATE SOURCE READER q_source_2 FOR source_2
        TYPE IN_MEMORY_QUEUE OPTIONS (
          NAME 'q_source_2'
        );
      ",
        ),
    ];

    apply_ddls(&ddls, SpringConfig::default())
}

#[test]
fn test_connect_2_pipelines() {
    let test_source = ForeignSource::new().unwrap();
    let test_sink = ForeignSink::start().unwrap();

    let pipeline1 = pipeline1(&test_source);
    let pipeline2 = pipeline2(&test_sink);

    test_source.start(ForeignSourceInput::new_fifo_batch(gen_pipeline1_input()));

    for _ in 0..gen_pipeline1_input().len() {
        let row = pipeline1.pop("q_sink_1").unwrap();
        pipeline2.push("q_source_2", row.into()).unwrap();
    }

    let sink_received = drain_from_sink(&test_sink);
    assert_eq!(sink_received.len(), 2);

    assert_eq!(
        sink_received[0]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:00.000000000",
    );
    assert_eq!(sink_received[0]["n"].as_i64().unwrap(), 420,);

    assert_eq!(
        sink_received[1]["ts"].as_str().unwrap(),
        "2020-01-01 00:00:10.000000000",
    );
    assert_eq!(sink_received[1]["n"].as_i64().unwrap(), 420,);
}
