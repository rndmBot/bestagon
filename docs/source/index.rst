.. bestagon documentation master file, created by
   sphinx-quickstart on Sat Mar 21 18:29:45 2026.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=======================
Welcome to Bestagon
=======================

Bestagon - is an asynchronous framework for event-sourced applications.

Its main purpose is to provide an efficient way to develop event-driven microservices
according to hexagonal (clean, ports and adapters) architecture.

It can be useful for anyone who is interested in Domain Driven Design (DDD) combined
with event-sourcing and CQRS, and provides all necessary blocks to build a scalable,
reactive system:

- Event-sourced aggregates.
- Persistence mechanism using event-sourced repository.
- Reactive application for use cases.
- CQRS implementation.
- Event sourced system to handle commands and queries.
- EventStore abstraction that allows to use different technlogies to store your events.

Installation
------------

You can use pip to install library from PyPi:

::

   pip install bestagon


Supported Python versions
-------------------------

The library is compatible with Python of versions >= 3.10


.. toctree::
   :maxdepth: 2

   topics/acknowledgments
   topics/tutorial
