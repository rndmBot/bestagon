Serving with FastAPI
====================

When your system is ready, it can be easily used locally as is, for example to run in Jupyter Notebook or using a command line.
But in most cases you want to be able to serve your system across the network so users and other systems could communicate with it.
One way to achieve this is by implementing a REST API, and in this tutorial you will see how you can do it using FastAPI.

System configuration
--------------------

You start with the configuration of your system. Here we define an event store, checkpoint store,
connection to the Neo4j database and use it to initialize the system.

.. code-block:: python

    neo_driver = neo4j.AsyncGraphDatabase.driver(uri=your_uri, auth=(username, password))
    event_store = KurrentDBEventStore(client=AsyncKurrentDBClient(uri=your_uri_here))
    checkpoint_store = Neo4JCheckpointStore(driver=neo_driver)

    system = BondsSystem(
        event_store=event_store,
        checkpoint_store=checkpoint_store,
        neo_driver=neo_driver
    )

FastAPI instance and system initialization
------------------------------------------

Next, you need to define a FastAPI application. There is one tricky part here - our system is asynchronous, it should be
initialized before usage and shut down gracefully when the work is finished. To make it happen, FastAPI provides a very
convenient mechanism using the ``lifespan`` coroutine, and this is a perfect place where we can do the system initialization and shutdown.

.. code-block:: python

    @asynccontextmanager
    async def lifespan(fast_api_app: FastAPI):
        await system.initialize()
        yield
        await system.shutdown()

    app = FastAPI(lifespan=lifespan)


Defining endpoints
------------------

It is time to write endpoints. You can split them into two categories - endpoints that change the state of your
system and return nothing, and endpoints that retrieve data from your system, but do not execute business logic.
Before we start, we define all necessary DTO objects. Bodies are used as input parameters to the endpoints.
DTOs are used as an output format for endpoints that return data.

.. note::

    For simplicity this example uses dataclasses as DTO objects, but it is highly advisable to use pydantic dataclasses
    for validation - the data you receive from endpoints can often be messy and you need an extra layer of defense to
    be sure that the input is valid.

.. code-block:: python

    @dataclass(frozen=True)
    class IssueBondBody:
        isin: str
        issue_date: str
        maturity_date: str
        coupon: float

    @dataclass(frozen=True)
    class UpdateTTMBody:
        isin: str

    @dataclass(frozen=True)
    class GetBondsBody:
        matured: bool | None

    @dataclass(frozen=True)
    class BondDTO:
        isin: str
        issue_date: str
        maturity_date: str
        coupon: float
        ttm: int
        matured: float


Next we write our endpoints. The defined ``BondsSystem`` provides the only way to communicate with the outside world - by executing
commands and queries. In your endpoints you only need to do three things:

1. Validate the input.
2. Construct the Command or Query.
3. Execute it and return the result.

This allows us to keep endpoints as thin as possible and provides a clear separation of concerns - the system is responsible
for the execution of business logic, and REST API is responsible for the communication with the outside world.

.. code-block:: python

    @app.post('/issue_bond')
    async def issue_bond(body: IssueBondBody) -> None:
        command = IssueBond(
            isin=body.isin,
            issue_date=body.issue_date,
            maturity_date=body.maturity_date,
            coupon=body.coupon
        )
        await system.execute_command(command=command, tracing_metadata=None)

    @app.post('/update_ttm')
    async def update_ttm(body: UpdateTTMBody) -> None:
        command = UpdateTermToMaturity(isin=body.isin)
        await system.execute_command(command=command, tracing_metadata=None)

    @app.post('/get_bonds')
    async def get_bonds(body: GetBondsBody) -> List[BondDTO]:
        query = GetBondsQuery(matured=body.matured)
        data = await system.execute_query(query)
        dto_list = [BondDTO(**datum) for datum in data]
        return dto_list


One thing to note here - the ``execute_command`` method takes two input parameters - the actual command and tracing metadata.
The tracing metadata is an instance of the ``TracingMetadata`` class that contains correlation and causation ids that allow you
to link your domain events to the process that triggered them. In such a case, the endpoint's input parameters should also include
the correlation and causation ids that can be propagated to the system.
If you pass None as tracing metadata, then the framework will automatically create new metadata with correlation and
causation ids.


Serving the system
------------------

In the final step, you can configure your server. Here we use uvicorn for this purpose.
After running the script, your system will be accessible across the network.

.. code-block:: python

    if __name__ == "__main__":
        uvicorn.run(app, host='0.0.0.0', port=55666)
