Serving with FastAPI
====================

This is the final part of the tutorial. Here you will learn how to serve your system across the network with FastAPI.

When your system is ready, it can be easily used locally as is, but in most cases you need the ability to communicate
with it across the network, send signals to other services or receive them and react in a specific way. One way to do that is through REST API.

System configuration
--------------------

You start with the configuration of your system. Here we define an event store, checkpoint store,
connection to the neo4j database and use it to initialize our system.

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

Next, you need to define a FastAPI application. Take a look at the `lifespan` co-routine.
FastAPI uses it to make the necessary initializations on startup (before the yield statement) and
to shut down gracefully (after the yield statement). We can use this mechanism to initialize and shutdown our system.

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
Before we start, we define body and DTO objects. Bodies are used as input parameters to the endpoints.
DTOs are used as an output format for endpoints that return data.

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


Bodies give you an extra layer of validation. FastAPI works closely with pydantic, and you can use that to validate
input parameters before they get to the system. If they are invalid, then the call to the endpoint should fail as soon as possible.

Next we write our endpoints. They should be thin - you need to define a command, or query and execute them with the
system and your endpoints should **never contain any business logic**. One thing to mention here - you must not open
endpoints for all your use cases, only create endpoints for use cases that should be triggered externally and protect them well.

.. code-block:: python

    @app.post('/issue_bond')
    async def issue_bond(body: IssueBondBody) -> None:
        command = IssueBond(
            isin=command.isin,
            issue_date=command.issue_date,
            maturity_date=command.maturity_date,
            coupon=comand.coupon
        )
        await system.execute_command(command)

    @app.post('/update_ttm')
    async def update_ttm(body: UpdateTTMBody) -> None:
        command = UpdateTermToMaturity(isin=command.isin)
        await system.execute_command(command)

    @app.post('/get_bonds')
    async def get_bonds(body: GetBondsBody) -> List[BondDTO]:
        query = GetBondsQuery(matured=body.matured)
        data = await system.execute_query(query)
        dto_list = [BondDTO(**datum) for datum in data]
        return dto_list


Serving the system
------------------

In the final step, you can configure your server. Here we use uvicorn for this purpose.
After running the script, your system will be accessible across the network.

.. code-block:: python

    if __name__ == "__main__":
        uvicorn.run(app, host='0.0.0.0', port=55666)
