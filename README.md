# AggregateEvent Sourcing in Python with Bestagon

Bestagon is an asynchronous framework for rapid development of event-sourced applications.
It follows the principles of DDD + ES + CQRS and provides necessary building blocks for development of an event sourced system:
- AggregateEvent sourced Aggregate.
- AggregateEvent-sourced repository to store and retreive aggregates.
- AggregateEvent Store and its implementation using KurrentDB.
- AggregateEvent-sourced Application for use cases.
- Projection to implement views using CQRS pattern.

The framework is event-store agnostic, it contains abstract classes that can be iplemented with required 
technology.

The documentation is hosted on readthedocs - https://bestagon.readthedocs.io/en/latest/

## Installation
```bash
pip install bestagon
```
