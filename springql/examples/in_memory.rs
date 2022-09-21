// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! A simple pipeline whose source and sink are both in-memory queue.
//!
//! Usage:
//!
//! ```bash
//! cargo run --example in_memory
//! ```

use springql_release_test::{SpringConfig, SpringPipeline, SpringSourceRowBuilder};

fn push_row_to_pipeline(pipeline: &SpringPipeline, queue_name: &str) {
    let row = SpringSourceRowBuilder::default()
        .add_column("ts", "2022-01-01 13:00:00.000000000".to_string())
        .unwrap()
        .add_column("temperature", 5.3)
        .unwrap()
        .build();

    pipeline.push(queue_name, row).unwrap()
}

fn main() {
    let pipeline = SpringPipeline::new(&SpringConfig::default()).unwrap();

    pipeline
        .command(
            "
            CREATE SOURCE STREAM source_temperature_celsius (
                ts TIMESTAMP NOT NULL ROWTIME,    
                temperature FLOAT NOT NULL
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK STREAM sink_temperature_fahrenheit (
                ts TIMESTAMP NOT NULL ROWTIME,    
                temperature FLOAT NOT NULL
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE PUMP c_to_f AS
                INSERT INTO sink_temperature_fahrenheit (ts, temperature)
                SELECT STREAM
                    source_temperature_celsius.ts,
                    32.0 + source_temperature_celsius.temperature * 1.8
                FROM source_temperature_celsius;
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK WRITER queue_temperature_fahrenheit FOR sink_temperature_fahrenheit
            TYPE IN_MEMORY_QUEUE OPTIONS (
                NAME 'q_sink'
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SOURCE READER queue_temperature_celsius FOR source_temperature_celsius
            TYPE IN_MEMORY_QUEUE OPTIONS (
                NAME 'q_src'
            );
            ",
        )
        .unwrap();

    // push rows to source
    for _ in 0..10 {
        push_row_to_pipeline(&pipeline, "q_src");
    }

    // pop rows (waits forever after all source rows are fetched)
    while let Ok(row) = pipeline.pop("q_sink") {
        let ts: String = row.get_not_null_by_index(0).unwrap();
        let temperature_fahrenheit: f32 = row.get_not_null_by_index(1).unwrap();
        eprintln!("{}\t{}", ts, temperature_fahrenheit);
    }
}
