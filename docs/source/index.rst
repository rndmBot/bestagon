.. bestagon documentation master file, created by
   sphinx-quickstart on Sat Mar 21 18:29:45 2026.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=======================
Welcome to Bestagon
=======================

Bestagon - is an asynchronous framework for event-sourcing in Python. It provides fast and easy way
to build event-sourced applications according to hexagonal architecture and following DDD+ES+CQRS
concepts:

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


The project is hosted on GitHub - https://github.com/rndmBot/bestagon


.. toctree::
   :maxdepth: 2

   topics/tutorial
