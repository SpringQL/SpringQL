**Status: Implementing important feature internally. We want to soon announce initial usable version.**

## What is SpringQL?

SpringQL is an open-source stream processor specialized in memory efficiency. It is supposed to run on embedded systems like IoT devices and in-vehicle computers.

- **SQL support**

  Like other stream processors, infinite number of rows flow through SpringQL system. You can register SQL-like (continuous) queries to filter, transform, and analyze rows. The query language is named SpringQL as well as the whole system itself.
  SpringQL has the ability to make windows from stream of rows so that you can perform `GROUP BY`, `JOIN`, and even more operations targeting on finite number of rows.

- **Memory efficient**

  Unlike other stream processors, the goal of SpringQL is saving working memory during stream processing. Of course the end-to-end latency and throughput are also important but they aren't the main targets.

- **Minimum dependencies**

  Embedded systems may have small storage size and only have quite fundamental libraries (`libc`, for example). SpringQL depends only on... **TBD**

## Why SpringQL?

Complex stream analysis (heavy machine learning, visualization, and so on) would be done in server-side. SpringQL is useful to **reduce upload data size** from embedded devices to servers.

You can sample, filter (`WHERE`), aggregate (`GROUP BY`), shrink (`SELECT necessary_column, ...`) stream rows to reduce data size inside embedded devices.

## Getting started

You can use SpringQL in 3 ways:

1. **Embedded mode**: Link `libspringql` from your apprecation binary and call API functions using a client library for each programming language.
2. **IPC mode**: IPC (Iterprocess Communication) with `springqld` from your applications. Multiple applications can share stream in this mode.
3. **Command line mode**: Run `springql` from shell and write SpringQL. This mode is mostly for demonstration.

Here introduces "Command line mode" first to quickly grasp SpringQL usage.

### Command line mode demo

`springql` command is available from [`springql-cmd` crate](https://github.com/SpringQL/springql-cmd).

#### Requirements

- [Rust toolchain](https://rustup.rs/)

#### Installation

```bash
$ cargo install springql-cmd
```

#### Running demo

In this demo, you will make a _pipeline_ to process stock trade data.

A pipeline represents rules to process stream data and it consists of the following stuffs.

**TODO Diagram**

- Source: Streaming data source outside of SpringQL.
- Sink: ...
- Stream: Relational schema like tables in RDBMS. While tables in RDBMS hold rows inside, rows flow streams.
  - Native stream: ...
  - Foreign stream: ...
- Pump: ...
- Server: ...

You firstly need streaming data source to organize pipeline. `springql-cmd` crate provides `springql-trade-source` command and it provides trade logs in JSON format.

```bash
$ springql-trade-source --port 17890

Waiting for connection from input server...
Sample row: {"timestamp": "2021-10-28 13:20:51.310480519", "ticker": "GOOGL", "amount": 100}
```

Now you can start your pipeline and input from `springql-trade-source` to a foreign stream.

```sql
$ springql --output fst_trade_oracle.log

⛲> CREATE FOREIGN STREAM "fst_trade" (
 ->   "timestamp" TIMESTAMP NOT NULL ROWTIME,
 ->   "ticker" TEXT NOT NULL,
 ->   "amount" INTEGER NOT NULL
 -> ) SERVER NET_SERVER OPTIONS (
 ->   remote_port 17890  
 -> );
```

`ROWTIME` keyword means `"timestamp"` field is used as timestamp of each row.

Data do not start to flow until you create a pump which input rows from `"trade"` stream.
A pump needs output stream so here creates `"trade_oracle"` stream too.

```sql
⛲> CREATE STREAM "st_trade_oracle" (
 ->   "timestamp" TIMESTAMP NOT NULL ROWTIME,
 ->   "ticker" TEXT NOT NULL,
 ->   "amount" INTEGER NOT NULL
 -> );

⛲> CREATE PUMP "pu_filter_oracle" AS
 ->   INSERT INTO "st_trade_oracle" ("timestamp", "ticker", "amount")
 ->   SELECT STREAM "timestamp", "ticker", "amount" FROM "fst_trade" WHERE "ticker" = "ORCL";
```

Unfortunately, you cannot see records from `"st_trade_oracle"` directly because you can only observe output data from sink. Let's create in-memory queue sink and pump to it here.

```sql
⛲> CREATE FOREIGN STREAM "fst_trade_oracle" (
 ->   "ts" TIMESTAMP NOT NULL,
 ->   "ticker" TEXT NOT NULL,
 ->   "amount" INTEGER NOT NULL
 -> ) SERVER IN_MEMORY_QUEUE;

⛲> CREATE PUMP "pu_oracle_to_sink" AS
 ->   INSERT INTO "fst_trade_oracle" ("ts", "ticker", "amount")
 ->   SELECT STREAM "timestamp", "ticker", "amount" FROM "st_trade_oracle";
```

You might realize that `"timestamp"` column has changed into `"ts"` and `ROWTIME` keyword disappears.
A sink does not have a concept of `ROWTIME` so it is meaningless to specify `ROWTIME` keyword to output foreign stream.

After activating 2 pumps, you can finally get output stream.

```sql
⛲> ALTER PUMP "pu_filter_oracle", "pu_oracle_to_sink" START;

⛲> SELECT "ts", "ticker", "amount" FROM "fst_trade_oracle";
Output are written to: `fst_trade_oracle.log` ...
```

```bash
$ tail -f fst_trade_oracle.log
{"ts":"2021-10-28 13:23:34.826434031","ticker":"ORCL","amount":100}
{"ts":"2021-10-28 13:23:36.382103952","ticker":"ORCL","amount":200}
...
```

### Embedded mode

Client libraries for the following languages are currently available:

- [C](https://github.com/SpringQL/SpringQL-client-c)

C client, for example, is composed of a header file and a dynamic library.
See [C client repository](https://github.com/SpringQL/SpringQL-client-c) for more details on how to use SpringQL in embedded mode.

### IPC mode

#### Installation

```bash
$ cargo install springql-daemon
```

**TBD** launch `sqringqld` via systemctl?

#### Launch daemon with a pipeline commonly used by applications

TODO config file?

share the same interm streams and sinks (not in memory but like redis)

#### Writing and building sample application

TODO read from redis sink

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in SpringQL by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

Copyright (c) 2021 TOYOTA MOTOR CORPORATION.
