# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog][Keep a Changelog] and this project adheres to [Semantic Versioning][Semantic Versioning].

We originally added `### For developers` sections in order to separate changes for end-users and developers.
All other sections are for end-users.

<!-- markdownlint-disable MD024 -->
## [Unreleased]

## [v1.0.0-a1] - 2022-08-22

## [v0.17.2] - 2022-08-19

## [v0.17.2] - 2022-08-03

### Fixed

- GenericWorker almost always got asleep after running a window task ([#235](https://github.com/SpringQL/SpringQL/pull/235))

## [v0.17.1] - 2022-07-13

### Fixed

- High CPU usage before issuing any SQL through `SpringPipeline::command()` ([#232](https://github.com/SpringQL/SpringQL/pull/232))

## [v0.17.0] - 2022-07-13

### Added

- `worker.sleep_msec_no_row` config value ([#229](https://github.com/SpringQL/SpringQL/pull/229))

### Fixed

- Changed the default sleep length from 10ms to 100ms for when a source / a generic worker does not receive any row ([#229](https://github.com/SpringQL/SpringQL/pull/229))

## [v0.16.1] - 2022-07-13

### Fixed

- High CPU usage even when no rows are coming from source ([#225](https://github.com/SpringQL/SpringQL/pull/225))

### Added

- `in_memory` example ([#225](https://github.com/SpringQL/SpringQL/pull/225))

## [v0.16.0] - 2022-07-05

### Added

- `HTTP1_CLIENT` sink writer ([#224](https://github.com/SpringQL/SpringQL/pull/224))

## [v0.15.0] - 2022-06-29

### Added

- `SpringSourceRowBuilder` to make `SpringSourceRow`s from native values (not from JSON), enabling rows with BLOB columns, for example ([#218](https://github.com/SpringQL/SpringQL/pull/218))

## [v0.14.0] - 2022-06-24

### Changed

- Bump up MSRV from 1.56.1 to 1.57.0 ([#203](https://github.com/SpringQL/SpringQL/pull/203))
- Name changed: `SpringRow` -> `SpringSinkRow` ([#210](https://github.com/SpringQL/SpringQL/pull/210))

### Added

- In-memory queue source reader and `SpringPipeline::push()` API ([#208](https://github.com/SpringQL/SpringQL/pull/208))
- `SpringSourceRow` and its constructor `SpringSourceRow::from_json()`([#214](https://github.com/SpringQL/SpringQL/pull/214))

### Fixed

- Fix `examples/in_vehicle_pipeline.rs` to run again ([#205](https://github.com/SpringQL/SpringQL/pull/205))
- Fix examples fleaky ([#207](https://github.com/SpringQL/SpringQL/pull/207))

### For developers

- add `release.yml` for automate release process ([#215](https://github.com/SpringQL/SpringQL/pull/215))
  - currently: any updates DISABLED, we need more test for use in release process.

## [v0.13.0]

### Changed

- Migrate dependencies `chrono` -> `time` ([#194](https://github.com/SpringQL/SpringQL/pull/194))
  - SpringTimestamp::from_str can accept more strictly
    - subsecond part must 9 digits
  - relates security advisory [RUSTSEC-2020-0071](https://rustsec.org/advisories/RUSTSEC-2020-0071)
    - [Tracking issue](https://github.com/SpringQL/SpringQL/issues/173)
- Bump up MSRV from 1.56.0 to 1.56.1 ([#199](https://github.com/SpringQL/SpringQL/pull/199))

### Added

- Implicit `ptime` column (processing time) for streams without `ROWTIME` keyword (event time) ([#195](https://github.com/SpringQL/SpringQL/pull/195))
- `BLOB` type ([#187](https://github.com/SpringQL/SpringQL/pull/187))
- `UNSIGNED INTEGER` type ([#201](https://github.com/SpringQL/SpringQL/pull/201))
- CAN source reader, which feeds SocketCAN frames into a stream with `can_id UNSIGNED INTEGER NOT NULL, can_data BLOB NOT NULL` columns ([#170](https://github.com/SpringQL/SpringQL/pull/170))

### Changed

- add crate `springql` ([#193](https://github.com/SpringQL/SpringQL/pull/193))
  - re-export `springql-core` API from `springql`

### For developpers

- Add deny lint option for `rustdoc::broken_intra_doc_links` ([#185](https://github.com/SpringQL/SpringQL/pull/185))
- Refactor : Hide detail module structure ([#177](https://github.com/SpringQL/SpringQL/pull/177))
  - Make private to internal modules
  - When publishing members outside the module, we recommend re-export(`pub use`) rather than `pub(crate)`
- Refactor : refactor test for web-console ([#200](https://github.com/SpringQL/SpringQL/pull/200))
  - relates security advisory [RUSTSEC-2020-0071](https://rustsec.org/advisories/RUSTSEC-2020-0071)
    - remove test-web-console-mock crate and dependent simple-server
  - add `stub_web_console` feature flag : for development and test only

## [v0.12.0]

### Changed

- Remove `spring_config_default()` ([#182](https://github.com/SpringQL/SpringQL/pull/182))
  - You may use `SpringConfig::default()` instead

### Fixed

- Fixed some broken links in rustdoc ([#183](https://github.com/SpringQL/SpringQL/pull/183))

## [v0.11.0]

### Changed

- re organize public API ([#169](https://github.com/SpringQL/SpringQL/pull/169))
  - public mod `api` and hide `high_level_rs`
    - high level APIs are exported from `springql_core_release_test::api`
  - remove low level API
  - rename `SpringPipelineHL` to `SpringPipeline`
  - rename `SpringRowHL` to `SpringRow`

### For Developers

- CI : add ignore for known deadlink ([#175](https://github.com/SpringQL/SpringQL/pull/157))
- Temporally turn off the security advisory RUSTSEC-2020-0071 ([#174](https://github.com/SpringQL/SpringQL/pull/174))
- Refactor : introduce wrapping to `chrono` types ([[#172](https://github.com/SpringQL/SpringQL/pull/172))

## [v0.10.0]

### Added

- docs: add code example to crate document([#156](https://github.com/SpringQL/SpringQL/pull/156))

### For Developers

- Refactor : crate/module document move to pure Markdown ([#136](https://github.com/SpringQL/SpringQL/pull/136))
- add `cargo-make` task for runs [actionlint](https://github.com/rhysd/actionlint)

## [v0.9.0]

### Added

- Non-blocking pop feature from in-memory queues both in high-level and low-level APIs ([#134](https://github.com/SpringQL/SpringQL/pull/134)).
  - High-level: `SpringPipelineHL::pop_non_blocking()`
  - Low-level: `spring_pop_non_blocking()`
- Warning doc comments to `SpringPipelineHL::pop()` and `spring_pop()` to prevent being used with threads ([#134](https://github.com/SpringQL/SpringQL/pull/134)).

## [v0.8.0]

### Added

- Create multiple pumps from a source stream (splitting pipeline from a source stream) ([#133](https://github.com/SpringQL/SpringQL/pull/133)).

## [v0.7.1]

### Fixed

- Aggregation without GROUP BY clause ([#129](https://github.com/SpringQL/SpringQL/pull/129)).
- Panic on multiple GROUP BY elements ([#129](https://github.com/SpringQL/SpringQL/pull/129)).
- Fields order in SELECT was ignored in aggregation query ([#128](https://github.com/SpringQL/SpringQL/pull/128)).

## [v0.7.0]

### Added

- Low-level APIs ([#121](https://github.com/SpringQL/SpringQL/pull/121)).
  - `spring_column_bool`
  - `spring_column_f32`
  - `spring_column_i16`
  - `spring_column_i64`

## [v0.6.0]

### Added

- `SpringPipelineHL::pop()` to get `SpringRowHL` from an in-memory queue ([#119](https://github.com/SpringQL/SpringQL/pull/119)).
- `SpringRowHL::get_not_null_by_index()` to get a column value from `SpringRowHL` ([#119](https://github.com/SpringQL/SpringQL/pull/119)).
- Made public ([#119](https://github.com/SpringQL/SpringQL/pull/119)):
  - `SpringTimestamp`
  - `SpringEventDuration`
  - `SpringValue` trait

## [v0.5.0]

### Fixed

- Made DDL strongly-consistent among internal worker threads ([#108](https://github.com/SpringQL/SpringQL/pull/108))

## [v0.4.2]

### Removed

- remove `Serialize, Deserialize` from most of (private) strct/enums ([#90](https://github.com/SpringQL/SpringQL/pull/90))

## [v0.4.1]

### Fixed

- Potential panic soon after a task graph has been changed by DDL ([#86](https://github.com/SpringQL/SpringQL/pull/86))

## [v0.4.0]

### Changed

- Change task threads default number to 1 to keep in-order processing ([#81](https://github.com/SpringQL/SpringQL/pull/81))

## [v0.3.3]

### Fixed

- Use parking_lot::RwLock instead of std::sync::RwLock ([#64](https://github.com/SpringQL/SpringQL/pull/64)): for the same reason as the fix in v0.3.2.

## [v0.3.2]

### Fixed

- Purger waited for task executors too long in Linux platforms ([#62](https://github.com/SpringQL/SpringQL/pull/62)).

## [v0.3.1]

### Added

- `SpringConfig::from_toml()` to construct a configuration in runtime from a TOML file (Rust interface).

## [v0.3.0]

### Changed

- `spring_open()` and `SpringPipelineHL::new()` are changed to take `SpringConfig`'s reference instead of owned value.

## [v0.2.0]

### Added

- Features to comply memory upper limit presented in the paper "Memory Efficient and Flexible Stream Processor for In-Vehicle Computers and IoT Devises".
- Window JOIN (LEFT OUTER).
- Window aggregation with grouping.
- Types:
  - FLOAT
  - BOOLEAN
- Out-of-order event-time processing.
- Windows:
  - Time-based fixed-window
  - Time-based sliding-window
- Rust high-level APIs.

## [v0.1.1] - 2021-12-07

### Changed

- Changed sleep time

## [v0.1.0] - 2021-12-07

### Added

- Initial release

---

<!-- Links -->
[Keep a Changelog]: https://keepachangelog.com/
[Semantic Versioning]: https://semver.org/

<!-- Versions -->
[Unreleased]: https://github.com/SpringQL/SpringQL/compare/v0.17.2...HEAD
[v1.0.0-a1]: https://github.com/SpringQL/SpringQL/compare/v0.10.0...v1.0.0-a1
[v0.17.2]: https://github.com/SpringQL/SpringQL/compare/v0.17.1...v0.17.2
[v0.17.1]: https://github.com/SpringQL/SpringQL/compare/v0.17.0...v0.17.1
[v0.17.0]: https://github.com/SpringQL/SpringQL/compare/v0.16.1...v0.17.0
[v0.16.1]: https://github.com/SpringQL/SpringQL/compare/v0.16.0...v0.16.1
[v0.16.0]: https://github.com/SpringQL/SpringQL/compare/v0.15.0...v0.16.0
[v0.15.0]: https://github.com/SpringQL/SpringQL/compare/v0.14.0...v0.15.0
[v0.14.0]: https://github.com/SpringQL/SpringQL/compare/v0.13.0...v0.14.0
[v0.13.0]: https://github.com/SpringQL/SpringQL/compare/v0.12.0...v0.13.0
[v0.12.0]: https://github.com/SpringQL/SpringQL/compare/v0.11.0...v0.12.0
[v0.11.0]: https://github.com/SpringQL/SpringQL/compare/v0.10.0...v0.11.0
[v0.10.0]: https://github.com/SpringQL/SpringQL/compare/v0.9.0...v0.10.0
[v0.9.0]: https://github.com/SpringQL/SpringQL/compare/v0.8.0...v0.9.0
[v0.8.0]: https://github.com/SpringQL/SpringQL/compare/v0.7.1...v0.8.0
[v0.7.1]: https://github.com/SpringQL/SpringQL/compare/v0.7.0...v0.7.1
[v0.7.0]: https://github.com/SpringQL/SpringQL/compare/v0.6.0...v0.7.0
[v0.6.0]: https://github.com/SpringQL/SpringQL/compare/v0.5.0...v0.6.0
[v0.5.0]: https://github.com/SpringQL/SpringQL/compare/v0.4.2...v0.5.0
[v0.4.2]: https://github.com/SpringQL/SpringQL/compare/v0.4.1...v0.4.2
[v0.4.1]: https://github.com/SpringQL/SpringQL/compare/v0.4.0...v0.4.1
[v0.4.0]: https://github.com/SpringQL/SpringQL/compare/v0.3.3...v0.4.0
[v0.3.3]: https://github.com/SpringQL/SpringQL/compare/v0.3.2...v0.3.3
[v0.3.2]: https://github.com/SpringQL/SpringQL/compare/v0.3.1...v0.3.2
[v0.3.1]: https://github.com/SpringQL/SpringQL/compare/v0.3.0...v0.3.1
[v0.3.0]: https://github.com/SpringQL/SpringQL/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/SpringQL/SpringQL/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/SpringQL/SpringQL/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/SpringQL/SpringQL/releases/v0.1.0
