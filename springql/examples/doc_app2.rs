// This file is part of https://github.com/SpringQL/SpringQL which is licensed under MIT OR Apache-2.0. See file LICENSE-MIT or LICENSE-APACHE for full license details.

//! Demo application in <https://springql.github.io/get-started/write-basic-apps/#app2-window-aggregation>.
//!
//! Usage:
//!
//! ```bash
//! cargo run --example doc_app2
//! ```
//!
//! ```bash
//! echo '{"ts": "2022-01-01 13:00:00.000000000", "symbol": "ORCL", "amount": 10}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:01.000000000", "symbol": "ORCL", "amount": 30}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:01.000000000", "symbol": "GOOGL", "amount": 50}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:02.000000000", "symbol": "ORCL", "amount": 40}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:05.000000000", "symbol": "GOOGL", "amount": 60}' |nc localhost 54300
//! echo '{"ts": "2022-01-01 13:00:10.000000000", "symbol": "APPL", "amount": 100}' |nc localhost 54300
//! ```

use std::{
    process::Command,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use springql_release_test::{SpringConfig, SpringPipeline};

fn send_data_to_pipeline() {
    fn send_row(json: &str) {
        let cmd_text = format!(r#"echo '{}' |nc localhost 54300"#, json);
        Command::new("bash")
            .arg("-c")
            .arg(cmd_text)
            .spawn()
            .expect("send failed");
    }

    send_row(r#"{"ts": "2022-01-01 13:00:00.000000000", "symbol": "ORCL", "amount": 10}"#);
    send_row(r#"{"ts": "2022-01-01 13:00:01.000000000", "symbol": "ORCL", "amount": 30}"#);
    send_row(r#"{"ts": "2022-01-01 13:00:01.000000000", "symbol": "GOOGL", "amount": 50}"#);
    send_row(r#"{"ts": "2022-01-01 13:00:02.000000000", "symbol": "ORCL", "amount": 40}"#);
    send_row(r#"{"ts": "2022-01-01 13:00:05.000000000", "symbol": "GOOGL", "amount": 60}"#);
    send_row(r#"{"ts": "2022-01-01 13:00:10.000000000", "symbol": "APPL", "amount": 100}"#);
}

fn main() {
    const SOURCE_PORT: u16 = 54300;

    // Using Arc to share the reference between threads feeding sink rows.
    let pipeline = Arc::new(SpringPipeline::new(&SpringConfig::default()).unwrap());

    pipeline
        .command(
            "
            CREATE SOURCE STREAM source_trade (
                ts TIMESTAMP NOT NULL ROWTIME,    
                symbol TEXT NOT NULL,
                amount INTEGER NOT NULL
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK STREAM sink_avg_all (
                ts TIMESTAMP NOT NULL ROWTIME,    
                avg_amount FLOAT NOT NULL
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK STREAM sink_avg_by_symbol (
                ts TIMESTAMP NOT NULL ROWTIME,    
                symbol TEXT NOT NULL,
                avg_amount FLOAT NOT NULL
            );
            ",
        )
        .unwrap();

    // Creates windows per 10 seconds ([:00, :10), [:10, :20), ...),
    // and calculate the average amount over the rows inside each window.
    //
    // Second parameter `DURATION_SECS(0)` means allowed latency for late data. You can ignore here.
    pipeline
        .command(
            "
            CREATE PUMP avg_all AS
                INSERT INTO sink_avg_all (ts, avg_amount)
                SELECT STREAM
                    FLOOR_TIME(source_trade.ts, DURATION_SECS(10)) AS min_ts,
                    AVG(source_trade.amount) AS avg_amount
                FROM source_trade
                GROUP BY min_ts
                FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
            ",
        )
        .unwrap();

    // Creates windows per 2 seconds ([:00, :02), [:02, :04), ...),
    // and then group the rows inside each window having the same symbol.
    // Calculate the average amount for each group.
    pipeline
        .command(
            "
            CREATE PUMP avg_by_symbol AS
                INSERT INTO sink_avg_by_symbol (ts, symbol, avg_amount)
                SELECT STREAM
                    FLOOR_TIME(source_trade.ts, DURATION_SECS(2)) AS min_ts,
                    source_trade.symbol AS symbol,
                    AVG(source_trade.amount) AS avg_amount
                FROM source_trade
                GROUP BY min_ts, symbol
                FIXED WINDOW DURATION_SECS(2), DURATION_SECS(0);
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK WRITER queue_avg_all FOR sink_avg_all
            TYPE IN_MEMORY_QUEUE OPTIONS (
                NAME 'q_avg_all'
            );
            ",
        )
        .unwrap();

    pipeline
        .command(
            "
            CREATE SINK WRITER queue_avg_by_symbol FOR sink_avg_by_symbol
            TYPE IN_MEMORY_QUEUE OPTIONS (
                NAME 'q_avg_by_symbol'
            );
            ",
        )
        .unwrap();

    pipeline
        .command(format!(
            "
            CREATE SOURCE READER tcp_trade FOR source_trade
            TYPE NET_SERVER OPTIONS (
                PROTOCOL 'TCP',
                PORT '{}'
            );
            ",
            SOURCE_PORT
        ))
        .unwrap();

    eprintln!("waiting JSON records in tcp/{} port...", SOURCE_PORT);
    let start_at = Instant::now();

    send_data_to_pipeline();

    loop {
        // Fetching rows from q_avg_all.
        if let Some(row) = pipeline.pop_non_blocking("q_avg_all").unwrap() {
            let ts: String = row.get_not_null_by_index(0).unwrap();
            let avg_amount: f32 = row.get_not_null_by_index(1).unwrap();
            eprintln!("[q_avg_all] {}\t{}", ts, avg_amount);
        }

        // Fetching rows from q_avg_by_symbol
        if let Some(row) = pipeline.pop_non_blocking("q_avg_by_symbol").unwrap() {
            let ts: String = row.get_not_null_by_index(0).unwrap();
            let symbol: String = row.get_not_null_by_index(1).unwrap();
            let avg_amount: f32 = row.get_not_null_by_index(2).unwrap();
            eprintln!("[q_avg_by_symbol] {}\t{}\t{}", ts, symbol, avg_amount);
        }

        // Avoid busy loop
        thread::sleep(Duration::from_millis(10));
        // exit with 5 second
        if Instant::now() - start_at > Duration::from_secs(5) {
            return;
        }
    }
}
