=========
Aggregate
=========

Aggregate is a central part of your domain model. It represents an entity or collection of entities that exists in your
domain, for example Customer, Merchant, Transaction. Each aggregate is unique and cannot be substituted with another one and, most important,
aggregate serves as a source of domain events, which are persisted in the event store.

This tutorial will give you a step by step instructions how to write event-sourced aggregates using Bestagon framework.

..

    There is also one commonly overooked fact - there are no transactions in event-sourced systems like in database management systems,
    this function is delegated to the aggregate, exactly this component represents a transactional boundary in your system.


The Aggregate class
-------------------

In Bestagon Aggregates represented by the :class:`~bestagon.core.aggregate.Aggregate` that have to be subclassed for each
aggregate type you have in your domain model. This is an abstract class and it contains several abstract methods that
should be reimplemented or you will not be able to create an instance of your
aggregate - `create_id` and `get_event_handler_routing` (will be covered later in the tutorial).

.. code-block:: python

    from bestagon.core.aggregate import Aggregate


    @register_aggregate_type('bond')
    class Bond(Aggregate):
        pass


Additionally, you need to register aggregate type for each aggregate you have in your domain which can be done
using `@register_aggregate_type(aggregate_type)` decorator. Aggregate type is a simple string, which should be unique
for each aggregate in yuor domain model. Aggregate type is used by framework internally for the serialization of events
and allows to decouple the actual class from it's type.


Domain events
-------------

Domain events are one of the most important parts of the aggregate. They are the result of execution of business rulles
and they tell what have happened in your domain that can be interesting for the business people. They are usually discovered
first using workshops like event storming or event modelling and after that implemented in code.
It is a good practice to add them to the body of the aggregate, it will help to keep things
in order, especially when you have a rich domain model with many aggregates and events.

There are two types of domain events - `Aggregate.AggregateCreated` and `Aggregate.AggrgateEvent`. They are both frozen data classes and
should contain only python primitives like str, int, float, bool etc. so they can be easily serialized.

So let's return back to event types. `Aggregate.AggregateCreated` is a special type of event from which aggregate starts
it's lifecycle. Think about what is happening in your domain, what event tells you explicitly that specific entity have been created?
As an example, if Customer starts it's lifecycle after registering on a web site, then there should be a `Registered` event
inside the `Customer` aggregate.
Please note, that each aggregate must have one `AggregateCreated` event.

After defining the `AggregateCreated` event, each subsequent event must subclass `Aggregate.AggrgateEvent` class.

Similar to the `Aggregate` each event type should be registered using `@register_event_type(event_type)` decorator. The specified
event type will be used by framework internally during the events persistence step.

.. code-block:: python

    @register_aggregate_type('bond')
    class Bond(Aggregate):

        @dataclass(frozen=True)
        @register_event_type('BondIssued')
        class Issued(Aggregate.AggregateCreated):
            isin: str
            issue_date: str
            maturity_date: str
            coupon: float

        @dataclass(frozen=True)
        @register_event_type('BondTTMChanged')
        class TermToMaturityChanged(Aggregate.AggrgateEvent):
            isin: str
            old_ttm: int
            new_ttm: int

        @dataclass(frozen=True)
        @register_event_type('BondMatured')
        class Matured(Aggregate.AggrgateEvent):
            isin: str

::

    **WARNING** - it is highly advisable not to change aggregate and event types after their definition. If, for some
    reason, you need to rename them, then you need to manually register new types in addition to old ones.
    More details about that in the chapter about persistence mechanism.


Aggregate initialization
------------------------

When you have finished with your events, it is time to define `__init__` method for your aggregate. Constructor takes
only one parameter - an event from which an aggregate starts its lifecycle, and it must be a subclass of
`Aggregate.AggregateCreated` class. Here you define properties of your aggregate that can be changed within the aggregate
lifecycle. To simplify the example here, we use primitives as property types, but it is highly advisable to use value
objects here, especially if these properties are a part of your invariants:

.. code-block:: python

    def __init__(self, event: Issued):
        super().__init__(event)
        self.isin = event.isin
        self.issue_date = date.fomisoformat(event.issue_date)
        self.maturity_date = date.fromisoformat(event.maturity_date)
        self.coupon = event.coupon

        self.ttm = None
        self.matured = False


Aggregate ID
------------

Another crucial part of the aggregate is an ID. It is a string that should be globally unique across your system.
Bestagon uses aggregate ID to store and retrieve events from the event store. Each aggregate will have its own event
stream named by aggregate ID.

To create an aggregate ID you need to reimplement the abstract static method `create_id`. It is good practice to use
UUID5 for this purpose, but you can use any method you like until the value is globally unique:

.. code-block:: python

    @staticmethod
    def create_id(isin: str) -> str:
        aggregate_uid = uuid5(NAMESPACE_URL, f'/bond/{isin}')
        return str(aggregate_uid)

**WARNING** - after defining aggregate ID you must not change it, because it will corrupt your application and make it
impossible to retrieve events for already saved aggregates.

Aggregate creation
------------------

When you create a new instance of an aggregate, you do not initialize it directly; instead you should use a factory
method that returns an aggregate. The name of the factory method should reflect your business domain. In this example,
the bond is `Issued`, so there should be a factory method named `issue` inside the aggregate.

.. code-block:: python

    @classmethod
    def issue(cls, command: IssueBond) -> Bond:
        # Write your validation logic here, if necessary

        # Prepare metadata
        metadata = DomainEventMetadata(
            timestamp=DomainEventMetadata.create_timestamp(),
            aggregate_id=cls.create_id(isin=command.isin),
            aggregate_version=cls.INITIAL_VERSION,
            aggregate_type=cls.get_aggregate_type()
        )

        # Create event that should be a subclass of `Aggregate.AggregateCreated` class
        event = cls.Issued(
            metadata=metadata,
            isin=command.isin,
            issue_date=command.issue_date,
            maturity_date=command.maturity_date,
            coupon=command.coupon
        )

        # Initialize the aggregate and return it
        obj = cls._create(event)
        return obj

Let's dissect it step by step.

The factory method takes a command as a parameter. The command is a frozen data class that inherits `Command` class
and contains all the necessary data to create an aggregate. You can also use a standard approach when you pass
parameters directly, but it is recommended to use commands for such cases because it will help you to keep things in
order and to avoid signature duplication.

.. code-block:: python

    @dataclass(frozen=True)
    class IssueBond(Command):
        isin: str
        issue_date: str
        maturity_date: str
        coupon: float


After you pass the command to the factory method, you can make the necessary validations if it is necessary.

The next step is a `DomainEventMetadata`. Bestagon uses metadata to store information that is not related to your
business domain, but can be useful for other components. Metadata should also contain mandatory information that is
used by the framework during the persistence step:

- Timestamp as a string that shows when the event was created.
- A globally unique aggregate ID.
- Aggregate version - when aggregate is just created it is equal to 0 and increased for every subsequent event
- Aggregate type - a globally unique string.

When metadata is prepared, you need to create an `Issued` event and populate it with necessary data from the command.
After that, you should use the private method `_create` to initialize the aggregate and return it.


Adding business logic
---------------------

The last thing that is left is to write your business logic. This process consists of 3 steps:

- Add a command handler that implements business logic and triggers an event
- Ensure that the event is added to `apply_event` method
- Implement an event handler, that changes the state of the aggregate.

There are two types of method worth mentioning - comand handlers and event handlers.

Command handlers are public methods that receive a command, execute business logic and trigger a corresponding event
without changing the aggregate state.

AggrgateEvent handlers are usually private methods that do not contain business logic. They receive an event as a parameter and
change the aggregate state. **IMPORTANT** - your event handlers **must not contain any business logic**, it should be
implemented inside command handlers.

Let's learn it by example. Here we have a simple invariant - a bond is matured when its term to maturity reaches zero:

.. code-block:: python

    def set_matured(self, command: SetMatured) -> None:
        # You implement invariants  BEFORE triggering the event
        if self.ttm > 0:
            raise DomainException('The bond can only mature if its TTM is equal to 0')

        # When business logic executed prepare metadata and event
        metadata = DomainEventMetadata(
            timestamp=DomainEventMetadata.create_timestamp(),
            aggregate_id=self.aggregate_id,
            aggregate_version=self.next_version,
            aggregate_type=self.aggregate_type,
        )

        # AggrgateEvent captures what have happened
        event = self.Matured(
            metadata=metadata,
            isin=self.isin
        )

        # Trigger event to apply changes for the aggregate
        self.trigger_event(event)

Let's dive into the details. The `set_matured` method receives a command as a parameter. You define your business logic
right after the method definition. Here you validate your business rules and make the necessary calculations. When your
business rules are executed, you prepare metadata and event which describes what exactly has happened and trigger it
using `trigger_event` method.

After an event has been triggered, it should be routed to the responsible event handler. To route the event to the event
handler, you need to reimplement `apply_event` method on the aggregate:

.. code-block:: python

    # Reimplement `apply_event` method to route event to the responsible event handler
    def apply_event(self, event: DomainEvent):
        event_routing = {
            self.TermToMaturityChanged: self._when_term_to_maturity_changed,
            self.Matured: self._when_matured,
        }
        event_handler = routing.get(type(event))
        if event_handler is not None:
            event_handler(event)
        else:
            super().apply_event(event)

    def _when_term_to_maturity_changed(event: TermToMaturityCahnged) -> None:
        self.ttm = event.new_ttm

    def _when_matured(event: Matured) -> None:
        self.matured = True

Here inside `apply_event` method we implement `event_routing` which is a dictionary where keys are event types and
values are event handlers. When an event is triggered using `trigger_event` method, under the hood aggregate calls
`apply_event` method, which looks for the event handler and calls it to change the state of an aggregate.

The private methods `_when_term_to_maturity_changed` and `_when_matured` are event handlers. They receive events and change the state of
the aggregate.

This concludes the aggregate tutorial. On the next step, you will learn how to implement
use-cases and how to react to events that are generated by your domain model.
