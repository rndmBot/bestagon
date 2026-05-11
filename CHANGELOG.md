# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.7.0] - 2026-05-09
### Added
- New module `aiosqlite_db` which contains adapters for `aiosqlite` library. 
The Framework now provides implementations of checkpoint store and projection using aiosqlite library - 
`AIOSQLiteCheckpointStore` and `AIOSQLiteProjection`.

### Changed
- `AsyncKurrentDBSubscription` renamed to `KurrentDBSubscription`
- `AsyncKurrentDBEventStore` renamed to `KurrentDBEventStore`
-  Documentation improvements
