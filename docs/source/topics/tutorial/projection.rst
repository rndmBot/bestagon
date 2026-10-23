Projection
==========

So far you should have a basic understanding of how to implement a domain model using a set of event-sourced aggregates
and execute use cases using applications. Use cases make your aggregates work, and aggregates provide you with domain events that
are the truth and persisted in the event store. You can react to the events, but events alone are not enough for the
system to be useful - you need all kinds of information about the current state of the system, for example:

- A table to show which customers are registered in the system.
- A graph that shows how different customers are related to each other.
- A table to show the history of transactions of a specific customer.

It is very difficult, and sometimes even impossible, to query such information from the stream of events. The good
news is that you do not need to, because event-sourced systems provide a very powerful mechanism to turn these
events into read models - a projection.

Projection takes the stream of events and populates a read model by replaying events one by one. The read model can be
in any form you can imagine - a table, graph, JSON file, and so on. It is populated in near
real-time and can be easily dropped if not needed anymore, because the source of truth - events - are kept intact and can be
used again to build a new projection.


Creating projection
-------------------

The projection in Bestagon is represented by the ``Projection`` class.

.. code-block:: python

    from bestagon.core.event_processor import Projection


    class BondsProjection(Projection):
        pass

Before we move forward, it is good practice to define an abstract interface of your projection.
It will make your life much easier if you decide to switch the data storage used by the projection, for example to migrate
from SQLite to Neo4j.

.. code-block:: python

    from bestagon.core.event_processor import Projection

    class BondsProjection(Projection):
        # Add / Set methods are used to populate the read model, they return nothing
        @abstractmethod
        async def add_bond(self, isin: str, issue_date: str, maturity_date: str, coupon: float) -> None:
            raise NotImplementedError

        @abstractmethod
        async def set_term_to_maturity(self, isin: str, ttm: int) -> None:
            raise NotImplementedError

        @abstractmethod
        async def set_matured(self, isin: str, matured: bool) -> None:
            raise NotImplementedError

        # Query handler - you get information using this method
        @abstractmethod
        async def get_bonds(self, query: GetBondsQuery) -> List[dict]:
            raise NotImplementedError


The projection defines two types of methods:

- Setters - these methods take arbitrary input parameters, change the data in the database, and return nothing. You use these
  methods to add something new to your database or change existing data.
- Query handlers - these methods take an instance of the ``Query`` class as input and return the data for this query.
  You use these methods to retrieve data from the projection.

We will return to these methods later. Now it is time to create a concrete implementation of the projection.
For this tutorial, we will use a Neo4j graph database to store our data, but you can use any storage technology you want
depending on your needs and technology stack.

.. code-block:: python

    class Neo4JBondsProjection(BondsProjection):
        def __init__(checkpoint_store: CheckpointStore, driver: AsyncDriver):
            super().__init__(checkpoint_store)
            self.driver = driver
            self.database_name = 'bonds-projection'

The projection takes several parameters as input. The first one is a checkpoint store instance.
This parameter is mandatory because, similar to ``Application``, ``Projection`` needs to track which events
have already been processed. All other parameters are related to the database you use to store your read model.
In this example, it is a Neo4j driver that gives us access to the database.
Remember to always call ``__init__`` of the superclass.

In most cases, you will need to perform additional initialization asynchronously, for example to configure the database and/or
create a database, indexes, and constraints if they do not exist. For this purpose you can reimplement the ``initialize`` method
that takes no parameters and allows you to run arbitrary code when your projection starts.

.. code-block:: python

    class Neo4JBondsProjection(BondsProjection):

        async def initialize(self) -> None:
            cypher = '''
            CREATE DATABASE $database_name IF NOT EXISTS
            '''
            async with self.driver.session() as sess:
                await sess.run(cypher, database_name=self.database_name)

Also, similar to the ``Application`` class, you need to implement the ``get_name`` method to provide the name of the projection:

.. code-block:: python

    class Neo4JBondsProjection(BondsProjection):

        def get_name(self) -> str:
            return 'bonds_projection'


Populating projection
---------------------

Let's return to the methods we defined inside our abstract interface - ``add_bond``, ``set_term_to_maturity``,
and ``set_matured``. The ``add_bond`` method adds a new record in your database that contains the basic information about the newly
issued bond, while ``set_term_to_maturity`` and ``set_matured`` only change specific values for an already added bond.

They have one thing in common - they contain the code that directly communicates with the database and changes the
stored data.

.. code-block:: python

    class Neo4JBondsProjection(BondsProjection):

        async def add_bond(self, isin: str, issue_date: str, maturity_date: str, coupon: float) -> None:
            cypher = '''
            MERGE (b:Bond {isin: $isin})
            ON CREATE
                SET b += $props
            '''
            props = {
                'issue_date': date.fromisoformat(issue_date),
                'maturity_date': date.fromisoformat(maturity_date),
                'coupon': coupon
            }
            async with self.driver.session(default_access_mode=neo4j.WRITE_ACCESS, database=self.database_name) as sess:
                await sess.run(cypher, isin=isin, props=props)

        async def set_term_to_maturity(self, isin: str, ttm: int) -> None:
            cypher = '''
            MATCH (b:Bond {isin: $isin})
            SET b.ttm = $ttm
            '''
            async with self.driver.session(default_access_mode=neo4j.WRITE_ACCESS, database=self.database_name) as sess:
                await sess.run(cypher, isin=isin, ttm=ttm)

        async def set_matured(self, isin: str, matured: bool) -> None:
            cypher = '''
            MATCH (b:Bond {isin: $isin})
            SET b.matured = $matured
            '''
            async with self.driver.session(default_access_mode=neo4j.WRITE_ACCESS, database=self.database_name) as sess:
                await sess.run(cypher, isin=isin, matured=matured)


Event handlers
--------------

Now that we have our setters, the question is how we call them. You definitely should not call them manually;
these methods should be called automatically as a reaction to the events that have happened in your domain model.

To make it real, you need to let the projection know which events you want to react to and how they should be called. The
first step is to define event handlers. These are simple methods that take an instance of ``DomainMessage`` as an
input parameter. Inside the handler, you extract the event and use the values from the event to call the necessary setter.

.. code-block:: python

    class Neo4JBondsProjection(BondsProjection):

        async def _when_bond_issued(self, message: DomainMessage) -> None:
            event = cast(Bond.Issued, message.event)
            await self.add_bond(isin=event.isin, issue_date=event.issue_date, maturity_date=event.maturity_date, coupon=event.coupon)

        async def _when_ttm_changed(self, message: DomainMessage) -> None:
            event = cast(Bond.TermToMaturityChanged, message.event)
            await self.set_term_to_maturity(isin=event.isin, ttm=event.new_ttm)

        async def _when_matured(self, message: DomainMessage) -> None:
            event = cast(Bond.Matured, message.event)
            await self.set_matured(isin=event.isin, matured=True)


After you have defined your event handlers, you need to reimplement the ``get_event_routing`` method to tie everything together.
The ``get_event_routing`` method should return the dictionary with domain event types as keys and event handlers as values:

.. code-block:: python

    class Neo4JBondsProjection(BondsProjection):

        def get_event_routing(self) -> dict:
            routing = {
                Bond.Issued: self._when_bond_issued,
                Bond.TermToMaturityChanged: self._when_ttm_changed,
                Bond.Matured: self._when_matured
            }
            return routing


Query handlers
--------------

The final thing to do is to implement query handlers. To recap - the query handlers take an instance of the ``Query`` class
as an input parameter and return an arbitrary value, usually a dictionary or a list of dictionaries.

First, let's figure out what ``Query`` is. The ``Query`` is a base class for each query that your system handles.
You use it to create a concrete implementation that is named according to your business domain and contains all the
necessary filters to retrieve the necessary data from the database.

.. code-block:: python

    @dataclass(frozen=True)
    class GetBondsQuery(Query):
        matured: bool | None
        # Additional filters according to your requirements

.. note::

    There are cases when you do not need to provide any filters to retrieve the data; in such a case, you still need to
    provide the query, but it will be empty, without any filters.

It is time to finally implement a query handler. This method is much more complex, because it contains logic to build a
query, execute it and return the data. In this example we prepare a Cypher query with the necessary filters and
retrieve data from the database as a list of dictionaries. This data can be easily passed through the network as a
JSON file and converted to a table or any other visual representation:

.. code-block:: python

    class Neo4JBondsProjection(BondsProjection):

        async def get_bonds(self, query: GetBondsQuery) -> List[dict]:
            filters = [
            'b.matured = $matured' if query.matured is not None else '',
            ]
            filters = [f for f in filters if f]
            if filters:
                filters = ' AND '.join(filters)
                filter_string = f'WHERE {filters}'
            else:
                filter_string = ''

            cypher = f'''
            MATCH (b:Bond)
            {filter_string}
            WITH {{
                isin: b.isin,
                issue_date: toString(b.issue_date),
                maturity_date: toString(b.maturity_date),
                coupon: b.coupon,
                ttm: b.ttm,
                matured: coalesce(b.matured, false)
            }} AS datum
            RETURN datum
            '''
            async with self.driver.session(default_access_mode=neo4j.READ_ACCESS, database=self.database_name) as sess:
                result = await sess.run(cypher, matured=query.matured)
                data = await result.value()
            return data


Dropping projection
-------------------

One of the very powerful features of projections is that they are disposable. You can easily drop it when you decide
that the read model is not useful anymore or it contains mistakes that need to be fixed. To make it real you need to reimplement
the ``drop`` method for the projection. It should delete all the data from your database regarding this projection so it could be rebuilt
by the framework.

.. code-block:: python

    class Neo4JBondsProjection(BondsProjection):

        async def drop(self) -> None:
            cypher = 'CREATE OR REPLACE DATABASE $database_name'
            async with self.driver.session() as sess:
                await sess.run(cypher, database_name=self.database_name)


.. note::

    You do not need to call ``drop`` manually; this method is supposed to be called by the framework internally
    when you execute the ``rebuild_projection`` method inside the ``EventSourcedSystem`` class.

Now your projection is complete. Remember - you can define any number of projections you want using any
storage that is available for you. Projections can be easily dropped and rebuilt again without the fear of
losing important data, because they are built from the source of truth - events.

In the next tutorial, you will learn how to tie everything together and create an event-sourced system.