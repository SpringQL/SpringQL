# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog][Keep a Changelog] and this project adheres to [Semantic Versioning][Semantic Versioning].

## [Unreleased]

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
[Unreleased]: https://github.com/SpringQL/SpringQL/compare/v0.3.3...HEAD
[Released]: https://github.com/SpringQL/SpringQL/releases
[v0.3.3]: https://github.com/SpringQL/SpringQL/compare/v0.3.2...v0.3.3
[v0.3.2]: https://github.com/SpringQL/SpringQL/compare/v0.3.1...v0.3.2
[v0.3.1]: https://github.com/SpringQL/SpringQL/compare/v0.3.0...v0.3.1
[v0.3.0]: https://github.com/SpringQL/SpringQL/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/SpringQL/SpringQL/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/SpringQL/SpringQL/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/SpringQL/SpringQL/releases/v0.1.0
