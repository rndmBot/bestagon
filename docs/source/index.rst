.. bestagon documentation master file, created by
   sphinx-quickstart on Sat Mar 21 18:29:45 2026.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=======================
Welcome to Bestagon
=======================

**Bestagon** is an asynchronous framework for event-sourcing in Python. It provides a fast and easy way
to build event-sourced applications following the best practices of DDD+ES+CQRS and helps to
gracefully manage the complexity of various business domains.

The Bestagon grasped a lot of
inspiration from
`Hexagonal Architecture <https://en.wikipedia.org/wiki/Hexagonal_architecture_(software)>`_, and
it is the perfect choice for building microservices according to this pattern.

At the core of the application lies a domain model that consists of one or more event-sourced
aggregates that contain all the business logic and completely decoupled from any technical
details like databases, which allows you to modify business logic without taking
technical details into consideration.

Use cases are implemented inside event sourced applications and the framework provides
CQRS out of the box with the `Projection` class. All these components are combined into one
`EventSourcedSystem` and you can attach various adapters to interact with it,
like REST API's, Kafka consumers / producers, MCP servers and many more without introducing
a lot of coupling to these technical details.


.. note::
   The project is under active development.


Installation
------------

You can use pip to install library from PyPi:

::

   pip install bestagon


Supported Python versions
-------------------------

The library is compatible with Python of versions >= 3.10


External Links
--------------

The project is hosted on `GitHub <https://github.com/rndmBot/bestagon>`_


.. toctree::
   :maxdepth: 2

   topics/installation
   topics/tutorial
   topics/concepts
   topics/support
   topics/examples


.. meta::
   :google-site-verification: nP1zSJ35h6KfS8gFSqm2nKH2hUIc-j6MCLP6_GWmlKw
