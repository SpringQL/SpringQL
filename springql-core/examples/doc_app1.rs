// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

/// Demo application in <https://springql.github.io/get-started/write-basic-apps/#app1-simple-arithmetic-conversion-over-a-stream>.
use std::{thread, time::Duration};

use springql_core::{high_level_rs::SpringPipelineHL, low_level_rs::SpringConfig};

fn main() -> ! {
    let pipeline = SpringPipelineHL::new(&SpringConfig::default()).unwrap();

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
            CREATE SINK WRITER tcp_temperature_fahrenheit FOR sink_temperature_fahrenheit
            TYPE NET_CLIENT OPTIONS (
                PROTOCOL 'TCP',
                REMOTE_HOST '127.0.0.1',
                REMOTE_PORT '54301'
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SOURCE READER tcp_trade FOR source_temperature_celsius
            TYPE NET_SERVER OPTIONS (
                PROTOCOL 'TCP',
                PORT '54300'
            );
            ",
        )
        .unwrap();

    // stop main thread and do the dataflow in worker threads
    loop {
        thread::sleep(Duration::from_millis(100));
    }
}
