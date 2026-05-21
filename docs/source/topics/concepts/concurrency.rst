Concurrency
===========

Event-sourcing is very IO bound by nature. Every time you execute use cases, there is a lot of
communication happening with different storage under the hood:
- repository communicates with the event store when it retrieves and saves aggregates
- projections update and retrieve data from the reporting database
- subscriptions constantly waiting for new events and propagating them to subscribers
- checkpoints are updated every time the event is consumed
- and so on

To make this process as fluent as possible, Bestagon framework uses `asyncio` to avoid unnecessary
i/o blocks. There are several things that are worth mentioning before starting to work with Bestagon.

Aggregate
---------

**Aggregate is always synchronous**.
When you write your own aggregate you should always use standard synchronous syntax and avoid any
i/o blocks inside the aggregate. The business logic should be executed as quickly as possible.
If you find yourself in a situation when your business logic requires you to wait for another
process to be executed, then it is a clear signal that you need to review your aggregate
boundaries and use sagas for long-running processes.


Repository
----------

**Repository is asynchronous**.
Bestagon uses the single repository concept. It means that a single repository is used to store
and retrieve all types of aggregates, which eliminates the necessity to write a separate repository
for each aggregate type. The repository is made asynchronous to avoid i/o blocks when multiple
applications store and retrieve aggregates simultaneously.


Application
-----------

**Applications are asynchronous**.
Each time you add a new use case to your application and implement event handlers, you should use
'async/await' syntax.


Projection
----------

**Projections are asynchronous**. You should use `async/await` syntax when implementing query
and event handlers. Also, you should use asynchronous clients for the reporting database you use,
for example, `AsyncDriver` for `Neo4j` database or `aiosqlite` module for asynchronous access to
`sqlite` database.


Subscriptions
-------------

**Subscriptions are asynchronous**. They await for new events without blocking the main thread,
which allows for other components to run without interruptions.


Concurrency between applications and projections
------------------------------------------------

One important thing to note here is how concurrency is organized between applications and projections.
If you have a simple system with only one application, then it will work as a synchronous system -
the application will consume events one by one and execute business logic sequentially
when processing events.

But in most cases, you will have more than just one application. As a minimum, you will have an
application and projection. In this case, each application and projection still processes events
sequentially, but, when one application awaits for database response, the other application or
projection will have time to run its processes.