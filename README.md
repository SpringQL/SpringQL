# SpringQL

[![crates.io](https://img.shields.io/crates/v/springql-core.svg)](https://crates.io/crates/springql-core)
[![Crates.io](https://img.shields.io/crates/d/springql-core?label=cargo%20installs)](https://crates.io/crates/springql-core)
[![docs.rs](https://img.shields.io/badge/API%20doc-docs.rs-blueviolet)](https://docs.rs/springql-core)
![MSRV](https://img.shields.io/badge/rustc-1.56+-lightgray.svg)
[![ci](https://github.com/SpringQL/SpringQL/actions/workflows/ci.yml/badge.svg?branch=main&event=push)](https://github.com/SpringQL/SpringQL/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/SpringQL/SpringQL/branch/main/graph/badge.svg?token=XI0IR5QVU3)](https://codecov.io/gh/laysakura/springql-core)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/laysakura/springql-core/blob/master/LICENSE-MIT)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](https://github.com/laysakura/springql-core/blob/master/LICENSE-APACHE)

**A stream processor for IoT devices and in-vehicle computers**

## What is SpringQL?

SpringQL is an open-source stream processor working with low and hard-limited working memory. It is supposed to run on resource-poor such as IoT devices and in-vehicle computers.

- **SQL support**

  Like other stream processors, infinite number of rows flow through SpringQL system. You can register SQL-like (continuous) queries to filter, transform, and analyze rows.
  SpringQL has the ability to make windows from stream of rows so that you can perform `GROUP BY`, `JOIN`, and even more operations targeting on finite number of rows.

- **Small memory footprint**

  SpringQL usually works with as low working memory as around 1MB.
  Even when processing heavy workload (e.g. with large size windows, or with rapid stream inputs), SpringQL tries to recover from high memory pressure by switching the internal task schedulers.

  You can set maximum memory allowed to consumed by SpringQL by configuration.

## Why SpringQL?

- **Realtime stream processing completed in edge**

  Some computations uses real-time data, such as sensor data, emerged from edge devices themselves.
  If such computations need to calculate results quickly, you may not have time to send the data to servers and get the result back from them.

  For real-time processing in edge devices, SpringQL provides fundamental tools to time-series calculations including window operations via SQL-like language.

- **Reducing data size before sending to servers**

  If you would perform complex stream analysis, such as heavy machine learning, visualization, using data from edge devices, you may perform stream processing mainly on resource-rich servers.

  If you have so many edge devices and need to shrink costs for data transfer, storage, and computation in servers, you may want to shrink data size in edges before sending them to servers.

  SpringQL helps you reduce data size by filtering, aggregation, and projection.

## Getting started

You can use SpringQL in 3 ways:

1. **Embedded mode**: Link `libspringql` from your application binary and call API functions using a client library for each programming language.
2. **Embedded mode (Rust)**: Almost the same as normal embedded mode but you statically link to `springql-core` crate from your Rust application.
3. **(TODO) IPC mode**: IPC (Iter-Process Communication) with `springqld` from your applications. Multiple applications can share the same pipeline (stream processing definition) in this mode.

Here introduces Rust embedded mode.

### Pipeline to create

We define the following data flow (called pipeline in SpringQL) in this demo.

![Demo pipeline](https://raw.githubusercontent.com/SpringQL/SpringQL.github.io/main/static/img/demo-pipeline.svg)

### Requirements

- [Rust toolchain](https://rustup.rs/)
- [replayman](https://github.com/SpringQL/replayman), a log agent to replay time-stamped logs.

    ```bash
    cargo install replayman
    ```

- `nc` (netcat) command

### Running demo

```bash
nc -l 19872  # waits for outputs from demo pipeline
```

```bash
cargo new --bin springql-demo
cd springql-demo

curl https://raw.githubusercontent.com/SpringQL/SpringQL/main/springql-core/examples/demo_pipeline.rs -O src/main.rs

cargo run
```

Then the terminal running nc will show the following output.

```json
TODO
```

## Learn more

- [C library for embedded mode](https://github.com/SpringQL/SpringQL-client-c)
- TODO paper after accepted

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in SpringQL by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

Copyright (c) 2022 TOYOTA MOTOR CORPORATION.
