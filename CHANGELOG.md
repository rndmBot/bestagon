# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.9.0] - 2026-09
WARNING - the update contains breaking changes which makes this version not backward compatible with
previous versions. The metadata propagation mechanism have been reworked toward automatic metadata
population and propagation making `Aggregate` class pure and free from necessity to populate metadata 
manually.

### Added
- `CommandMetadata` class to store medatadata propagated with command.
- `CommandMessage` class which serves as an envelope for `Command` and `CommandMetadata`.
- `DomainEventMetadata` class to store metadata for the specific domain event.
- `DomainMessage` class to serve as an envelope for `DomainEvent` and `DomainMetadata`

### Changed
- `DomainEvent` no longer contain metadata
- `EventProcessor`'s `process_event` method renamed to `process_message` and accepts `DomainMessage`
  as parameter.
- `Aggregate`'s `_create` method renamed to `create_aggregate`
- `EventSourcedRepository` now uses `aggregate_id` as stream name instead of combination of
  `aggregate_type` + `aggregate_id`. This change is breaking, it will not be possible to retrieve agregates
  created with previous versions of framework.


## [0.8.0] - 2026-08
### Added
- New documentation topic - Installation
- New abstract method for `EventStore` class - `get_stream_version` the method should return version
of the specified stream or -1 if stream not exists.
- New adapter - `AIOSQLiteEventStore` event store implementation using `aiosqlite` module.
- New abstract method for `EventStoreSubscription` class - `start`.


### Changed
- Documentation improvements
- `NewStreamEvent` stream_position parameter removed, the stream position should be assigned
by event store automatically, not by passing a parameter in new event.
- Removed property `running` of `EventStoreSubscription`.
- `EventStore` `connect` method renamed to `initialize`
- `EventStore` `close` method renamed to `shutdown`
- `NewStreamEvent` renamed to `NewEventStoreEvent`
- `StreamEvent` renamed to `EventStoreEvent`
- `EventStore's` method `appnd_events` now requires additional parameter - 'expected_version' that is
required for optimistic concurrency.


## [0.7.0] - 2026-05-09
### Added
- New module `aiosqlite_db` which contains adapters for `aiosqlite` library. 
The Framework now provides implementations of checkpoint store and projection using aiosqlite library - 
`AIOSQLiteCheckpointStore` and `AIOSQLiteProjection`.

### Changed
- `AsyncKurrentDBSubscription` renamed to `KurrentDBSubscription`
- `AsyncKurrentDBEventStore` renamed to `KurrentDBEventStore`
-  Documentation improvements
