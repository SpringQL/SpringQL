# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog][Keep a Changelog] and this project adheres to [Semantic Versioning][Semantic Versioning].

## [Unreleased]

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
[Unreleased]: https://github.com/SpringQL/SpringQL/compare/v0.2.0...HEAD
[Released]: https://github.com/SpringQL/SpringQL/releases
[v0.2.0]: https://github.com/SpringQL/SpringQL/compare/v0.1.1...v0.2.0
[v0.1.1]: https://github.com/SpringQL/SpringQL/compare/v0.1.0...v0.1.1
[v0.1.0]: https://github.com/SpringQL/SpringQL/releases/v0.1.0
