// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Usage:
//!
//! ```bash
//! # Enable your SocketCAN interface (e.g. vcan0, slcan0) first.
//!
//! cargo run --example can_source_reader -- SOCKET_CAN_INTERFACE
//! ```

use std::env;

use springql_core_release_test::api::{SpringConfig, SpringPipeline};

fn parse_can_interface_arg() -> String {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 2);

    let interface = args[1].clone();
    log::info!("Using CAN interface: {}", interface);

    interface
}

fn main() {
    let can_interface = parse_can_interface_arg();

    let pipeline = SpringPipeline::new(&SpringConfig::default()).unwrap();

    pipeline
        .command(
            "
            CREATE SOURCE STREAM source_can (
                can_id UNSIGNED INTEGER NOT NULL,
                can_data BLOB NOT NULL
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK STREAM sink_can (
                ts TIMESTAMP NOT NULL ROWTIME,    
                can_id UNSIGNED INTEGER NOT NULL,    
                can_data BLOB NOT NULL
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE PUMP pump_can AS
                INSERT INTO sink_can (ts, can_id, can_data)
                SELECT STREAM
                    source_can.ptime,
                    source_can.can_id,
                    source_can.can_data
                FROM source_can;
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK WRITER queue_can FOR sink_can
            TYPE IN_MEMORY_QUEUE OPTIONS (
                NAME 'q'
            );
            ",
        )
        .unwrap();

    pipeline
        .command(format!(
            "
            CREATE SOURCE READER can FOR source_can
            TYPE CAN OPTIONS (
                INTERFACE '{}'
            );
            ",
            can_interface
        ))
        .unwrap();

    while let Ok(row) = pipeline.pop("q") {
        let ts: String = row.get_not_null_by_index(0).unwrap();
        let can_id: u32 = row.get_not_null_by_index(1).unwrap();
        let can_data: Vec<u8> = row.get_not_null_by_index(2).unwrap();
        eprintln!("{}\t{:X}\t{:02X?}", ts, can_id, can_data);
    }
}
