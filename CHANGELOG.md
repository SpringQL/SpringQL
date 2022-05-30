# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog][Keep a Changelog] and this project adheres to [Semantic Versioning][Semantic Versioning].

We originally added `### For developers` sections in order to separate changes for end-users and developers.
All other sections are for end-users.

<!-- markdownlint-disable MD024 -->
## [Unreleased]

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
[Unreleased]: https://github.com/SpringQL/SpringQL/compare/v0.10.0...HEAD
[Released]: https://github.com/SpringQL/SpringQL/releases
[v0.9.0]: https://github.com/SpringQL/SpringQL/compare/v0.9.0...v0.10.0
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
