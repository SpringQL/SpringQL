<!-- markdownlint-disable MD041 -->
 SpringQL implementation.

## High-level architecture diagram

![High-level architecture diagram](https://raw.githubusercontent.com/SpringQL/SpringQL/main/springql-core/doc/img/springql-architecture.drawio.svg)

## Examples

### Simple pipeline example

- create pipeline instance: [SpringPipeline::new](crate::SpringPipeline::new)
- execute DDLs: [SpringPipeline::command](crate::SpringPipeline::command)
- fetch row from pipeline: [SpringPipeline::pop](crate::SpringPipeline::pop)

```rust
use springql_release_test::{SpringPipeline, SpringConfig};

fn main() {
    const SOURCE_PORT: u16 = 54300;

    // create pipeline instans
    let pipeline = SpringPipeline::new(&SpringConfig::default()).unwrap();

    // execute DDLs for build pipeline

    // source stream inputs to SpringQL pipeline
    pipeline.command(
        "CREATE SOURCE STREAM source_temperature_celsius (
            ts TIMESTAMP NOT NULL ROWTIME,    
            temperature FLOAT NOT NULL
        );", ).unwrap();

    // sink stream output from SpringQL pipeline
    pipeline.command(
        "CREATE SINK STREAM sink_temperature_fahrenheit (
            ts TIMESTAMP NOT NULL ROWTIME,    
            temperature FLOAT NOT NULL
        );", ).unwrap();

    // create pump for fetching rows from source
    pipeline.command(
        "CREATE PUMP c_to_f AS
            INSERT INTO sink_temperature_fahrenheit (ts, temperature)
            SELECT STREAM
                source_temperature_celsius.ts,
                32.0 + source_temperature_celsius.temperature * 1.8
            FROM source_temperature_celsius;", ).unwrap();

    // create sink writer, accessible by name "q", You can fetch row from `pipeline.pop("q")`
    pipeline.command(
        "CREATE SINK WRITER queue_temperature_fahrenheit FOR sink_temperature_fahrenheit
        TYPE IN_MEMORY_QUEUE OPTIONS (
            NAME 'q'
        );", ).unwrap();

#   return ();
    // create source reader, input come from net_server
    pipeline.command(format!(
        "CREATE SOURCE READER tcp_temperature_celsius FOR source_temperature_celsius
        TYPE NET_SERVER OPTIONS (
            PROTOCOL 'TCP',
            PORT '{}'
        );", SOURCE_PORT)).unwrap();

    eprintln!("waiting JSON records in tcp/{} port...", SOURCE_PORT);
    // fetch row from "q" , "q" is a sink writer defined above.
    while let Ok(row) = pipeline.pop("q") {
        // get field value from row by field index
        let ts: String = row.get_not_null_by_index(0).unwrap();
        let temperature_fahrenheit: f32 = row.get_not_null_by_index(1).unwrap();
        // show in STDERR
        eprintln!("{}\t{}", ts, temperature_fahrenheit);
    }
}
```

Run this shell script to input data.

```bash
echo '{"ts": "2022-01-01 13:00:00.000000000", "temperature": 5.3}' | nc localhost 54300
```

### Using Window and share pipeline for many threads

- To share pipeline for threads, use [std::sync::Arc](std::sync::Arc)
- non blocking fetch rom for sink [pop_non_blocking](SpringPipeline::pop_non_blocking)

```rust
use std::{sync::Arc, thread, time::Duration};
use springql_release_test::{SpringPipeline, SpringConfig};

fn main() {
    const SOURCE_PORT: u16 = 54300;

    // Using Arc to share the reference between threads feeding sink rows.
    let pipeline = Arc::new(SpringPipeline::new(&SpringConfig::default()).unwrap());

    pipeline.command(
        "CREATE SOURCE STREAM source_trade (
            ts TIMESTAMP NOT NULL ROWTIME,    
            symbol TEXT NOT NULL,
            amount INTEGER NOT NULL
         );", ).unwrap();

    pipeline.command(
        "CREATE SINK STREAM sink_avg_all (
                ts TIMESTAMP NOT NULL ROWTIME,    
                avg_amount FLOAT NOT NULL
         );", ).unwrap();

    pipeline.command(
        "CREATE SINK STREAM sink_avg_by_symbol (
            ts TIMESTAMP NOT NULL ROWTIME,    
            symbol TEXT NOT NULL,
            avg_amount FLOAT NOT NULL
         );", ).unwrap();

    // Creates windows per 10 seconds ([:00, :10), [:10, :20), ...),
    // and calculate the average amount over the rows inside each window.
    //
    // Second parameter `DURATION_SECS(0)` means allowed latency for late data.
    //
    // You can ignore here.
    pipeline.command(
        "CREATE PUMP avg_all AS
            INSERT INTO sink_avg_all (ts, avg_amount)
            SELECT STREAM
                FLOOR_TIME(source_trade.ts, DURATION_SECS(10)) AS min_ts,
                AVG(source_trade.amount) AS avg_amount
            FROM source_trade
            GROUP BY min_ts
            FIXED WINDOW DURATION_SECS(10), DURATION_SECS(0);
        ", ).unwrap();

    // Creates windows per 2 seconds ([:00, :02), [:02, :04), ...),
    // and then group the rows inside each window having the same symbol.
    // Calculate the average amount for each group.
    pipeline.command(
        "CREATE PUMP avg_by_symbol AS
            INSERT INTO sink_avg_by_symbol (ts, symbol, avg_amount)
            SELECT STREAM
                FLOOR_TIME(source_trade.ts, DURATION_SECS(2)) AS min_ts,
                source_trade.symbol AS symbol,
                AVG(source_trade.amount) AS avg_amount
            FROM source_trade
            GROUP BY min_ts, symbol
            FIXED WINDOW DURATION_SECS(2), DURATION_SECS(0);
        ", ).unwrap();

    pipeline.command(
        "CREATE SINK WRITER queue_avg_all FOR sink_avg_all
            TYPE IN_MEMORY_QUEUE OPTIONS (
                NAME 'q_avg_all'
            );", ).unwrap();

    pipeline.command(
        "CREATE SINK WRITER queue_avg_by_symbol FOR sink_avg_by_symbol
            TYPE IN_MEMORY_QUEUE OPTIONS (
                NAME 'q_avg_by_symbol'
            );", ).unwrap();

#   return ();
    pipeline.command(format!(
        "CREATE SOURCE READER tcp_trade FOR source_trade
        TYPE NET_SERVER OPTIONS (
            PROTOCOL 'TCP',
            PORT '{}'
        );", SOURCE_PORT)).unwrap();

    eprintln!("waiting JSON records in tcp/{} port...", SOURCE_PORT);
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
        thread::sleep(Duration::from_millis(10))
    }
}
```
