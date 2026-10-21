=========
Aggregate
=========

An aggregate is a central part of your domain model. It represents an entity or collection of entities that exists in your
domain, for example ``Customer``, ``Merchant`` or ``Transaction``. Each aggregate is unique and cannot be substituted by another one and,
most importantly, an aggregate serves as a source of domain events, which are persisted in the event store.

This tutorial will give you step-by-step instructions on how to write event-sourced aggregates using the Bestagon framework.

.. note::

    There is also one commonly overlooked fact: there are no transactions in event-sourced systems like in database management systems.
    This function is delegated to the aggregate, and exactly this component represents a transactional boundary in your system.

.. note::

    For brevity, the code snippets in this tutorial omit imports.


The Aggregate class
-------------------

In Bestagon, aggregates are represented by the :class:`~bestagon.core.aggregate.Aggregate` class, which has to be subclassed for each
aggregate type you have in your domain model. This is an abstract class and it contains several abstract methods that
should be implemented: ``create_id`` and ``get_event_handler_routing`` (both are covered later in the tutorial).

.. code-block:: python

    from bestagon.core.aggregate import Aggregate


    @register_aggregate_type('bond')
    class Bond(Aggregate):
        pass


Additionally, you need to register an aggregate type for each aggregate you have in your domain, which can be done
using the ``@register_aggregate_type(aggregate_type)`` decorator. An aggregate type is a simple string, which should be unique
for each aggregate in your domain model. The aggregate type is used by the framework internally for the serialization of events
and allows you to decouple the actual class from its type.


Domain events
-------------

Domain events are one of the most important parts of the aggregate. They tell what has happened in your domain that can
be interesting for the business people, and they are usually discovered using workshops like event storming or event modeling.
When you define domain events, it is good practice to add them to the body of the aggregate. This helps to keep things
in order, especially when you have a rich domain model with many aggregates and events.

There are two types of domain events: ``Aggregate.AggregateCreated`` and ``Aggregate.AggregateEvent``. They are both frozen dataclasses and
should contain only Python primitives like ``str``, ``int``, ``float``, ``bool`` etc., so that they can be easily serialized.

``Aggregate.AggregateCreated`` is a special type of event from which an aggregate starts its lifecycle.
Think about what is happening in your domain: which event tells you explicitly that a specific entity has been created?
As an example, if a ``Customer`` starts its lifecycle after registering on a website, then there should be a ``Registered`` event
inside the ``Customer`` aggregate. Please note that each aggregate must have exactly one ``AggregateCreated`` event.

After defining the ``AggregateCreated`` event, each subsequent event must subclass the ``Aggregate.AggregateEvent`` class.

Similar to ``Aggregate``, each event type should be registered using the ``@register_event_type(event_type)`` decorator. The specified
event type will be used by the framework internally during the event persistence step.

.. code-block:: python

    @register_aggregate_type('bond')
    class Bond(Aggregate):

        @dataclass(frozen=True)
        @register_event_type('BondIssued')
        class Issued(Aggregate.AggregateCreated):
            isin: str
            issue_date: str
            maturity_date: str
            maturity_segment: int
            issue_amount: float
            coupon: float
            coupon_frequency: int
            day_count_convention: str

        @dataclass(frozen=True)
        @register_event_type('BondDaysToMaturityChanged')
        class DaysToMaturityChanged(Aggregate.AggregateEvent):
            isin: str
            old_days_to_maturity: int
            new_days_to_maturity: int

        @dataclass(frozen=True)
        @register_event_type('BondMatured')
        class Matured(Aggregate.AggregateEvent):
            isin: str

.. warning::

    It is highly advisable not to change aggregate and event types after their definition. If, for some
    reason, you need to rename them, then you need to manually register new types in addition to old ones using ``EventTypeRegistry``
    and ``AggregateTypeRegistry``. More details about that can be found in the chapter about the persistence mechanism.


Aggregate initialization
------------------------

When you have finished defining events, it is time to implement the ``__init__`` method for your aggregate. The method takes
several parameters as input: an ``AggregateCreated`` event from which an aggregate starts its lifecycle, the ID of the aggregate
and the initial version of the aggregate.

The aggregate ID and version are used internally, and you need to pass these values to the superclass's constructor.

The ``AggregateCreated`` event is used to set the initial state of the aggregate. This is what you need to do manually:
you use the fields in the event to set the initial values of the properties in your aggregate. Here you define the properties
of your aggregate that can be changed within the aggregate lifecycle. In the example below we use primitives as property types
for simplicity, but it is highly advisable to use value objects, especially if these properties are part of your invariants.

.. code-block:: python

    @register_aggregate_type('bond')
    class Bond(Aggregate):

        def __init__(self, event: Issued, aggregate_id: str, aggregate_version: int):
            super().__init__(event=event, aggregate_id=aggregate_id, aggregate_version=aggregate_version)

            # It is good practice to make attributes private to protect them from accidental change
            self._isin = event.isin
            self._issue_date = date.fromisoformat(event.issue_date)
            self._maturity_date = date.fromisoformat(event.maturity_date)
            self._maturity_segment = event.maturity_segment
            self._issue_amount = event.issue_amount
            self._coupon = event.coupon
            self._coupon_frequency = event.coupon_frequency
            self._day_count_convention = event.day_count_convention

            self._days_to_maturity = None
            self._matured = False

        @property
        def coupon(self) -> float:
            return self._coupon

        @property
        def coupon_frequency(self) -> int:
            return self._coupon_frequency

        @property
        def day_count_convention(self) -> str:
            return self._day_count_convention

        @property
        def days_to_maturity(self) -> int:
            return self._days_to_maturity

        @property
        def is_ready_to_mature(self) -> bool:
            return self.days_to_maturity == 0

        @property
        def isin(self) -> str:
            return self._isin

        @property
        def issue_amount(self) -> float:
            return self._issue_amount

        @property
        def issue_date(self) -> date:
            return self._issue_date

        @property
        def matured(self) -> bool:
            return self._matured

        @property
        def maturity_date(self) -> date:
            return self._maturity_date

        @property
        def maturity_segment(self) -> int:
            return self._maturity_segment


.. note::

    Please note that you never initialize an aggregate yourself using the ``__init__`` method; it is used internally by the framework.
    Always use the factory method to create a new aggregate instance, or a repository to retrieve an existing one.


Aggregate ID
------------

Another crucial part of the aggregate is its ID. It is a string that should be globally unique across your system.
Bestagon uses the aggregate ID to store and retrieve events from the event store, and each aggregate will have its own event
stream named by the aggregate ID.

To create the aggregate ID, you need to implement the abstract static method ``create_id``. It is good practice to use
UUIDv5 for this purpose, but you can use any method you like as long as the value is globally unique:

.. code-block:: python

    @staticmethod
    def create_id(isin: str) -> str:
        aggregate_uid = uuid5(NAMESPACE_URL, f'/bond/{isin}')
        return str(aggregate_uid)

.. warning::

    After defining the ``create_id`` method, be very careful when changing it.
    If your changes lead to the creation of a different aggregate ID with the same parameters, then you
    will not be able to reconstruct your previously saved aggregate from the event store.


Aggregate factory
-----------------

To create a new instance of the aggregate, you define a factory method. It takes all the parameters
that are necessary to create an ``AggregateCreated`` event, and it is named according to your domain. For example, if a ``Customer``
is registering on a website, then the factory method should be named ``register``.

We are going to continue our previous example with the ``Bond`` aggregate. The bond is issued, so our factory method will be
named ``issue``. The factory method consists of several steps:

- Validate the input data - you can validate whether the input parameters are complete and valid.
- Execute the business logic - if necessary, you can execute the business logic according to the rules that exist in your domain.
- Create the aggregate ID using the ``create_id`` method.
- Create the ``AggregateCreated`` event - when the data is validated and the business logic is executed, you create the ``AggregateCreated``
  event and populate it with the necessary fields.
- Create the aggregate instance using the ``create_aggregate`` method - ``create_aggregate`` is how you actually
  instantiate the aggregate. It takes the ``AggregateCreated`` event and the aggregate ID as input and returns the instance
  of the aggregate.
- Return the aggregate instance - the final step.


.. code-block:: python

    @register_aggregate_type('bond')
    class Bond(Aggregate):

        # Code omitted

        @classmethod
        def issue(
            cls,
            isin: str,
            issue_date: str,
            maturity_date: str,
            maturity_segment: int,
            issue_amount: float,
            coupon: float,
            coupon_frequency: int,
            day_count_convention: str,
        ) -> 'Bond':
            # 1 - Validate input (optional)
            # 2 - Execute business logic (optional)

            # 3 - Create aggregate_id
            aggregate_id = cls.create_id(isin=isin)

            # 4 - Create `AggregateCreated` event
            event = cls.Issued(
                isin=isin,
                issue_date=issue_date,
                maturity_date=maturity_date,
                maturity_segment=maturity_segment,
                issue_amount=issue_amount,
                coupon=coupon,
                coupon_frequency=coupon_frequency,
                day_count_convention=day_count_convention,
            )

            # 5 - Create the new instance of the aggregate
            obj = cls.create_aggregate(event=event, aggregate_id=aggregate_id)

            # 6 - Return the aggregate instance
            return obj


Implementing business logic
---------------------------

The next step is to implement methods that actually execute business logic and lead to the creation of domain events
and changes of state of the aggregate. Before we begin, there are two types of methods to distinguish between: command handlers
and event handlers.

The command handlers are methods where business logic lives:

- They are named according to your domain. For example, the ``Bond`` matures after a predefined amount of time, so the ``Bond``
  aggregate will have a ``set_matured`` method.
- Command handlers take all the necessary parameters to succeed. You can provide parameters, commands, events, or even whole aggregates
  as input if this is necessary.
- Command handlers validate the input and execute the business logic.
- When the business logic is executed, command handlers capture the changes as a corresponding domain event.
- The newly created event is then triggered using the ``trigger_event`` method.


.. code-block:: python

    @register_aggregate_type('bond')
    class Bond(Aggregate):

        # Code omitted

        def change_days_to_maturity(self, new_days_to_maturity: int) -> None:
            # 1 - Validate input (optional)

            # 2 - Execute business logic
            if new_days_to_maturity == self.days_to_maturity:
                return

            # 3 - Capture changes as a corresponding domain event
            event = self.DaysToMaturityChanged(
                isin=self.isin,
                old_days_to_maturity=self.days_to_maturity,
                new_days_to_maturity=new_days_to_maturity,
            )

            # 4 - Trigger the new event
            self.trigger_event(event)

        def set_matured(self) -> None:
            # 1 - Validate input (if necessary)

            # 2 - Execute business logic
            if self.matured:
                return

            if self.days_to_maturity != 0:
                raise DomainException('Only bonds with days to maturity of 0 can be matured')

            # 3 - Capture changes as a corresponding domain event
            event = self.Matured(
                isin=self.isin
            )

            # 4 - Trigger the new event
            self.trigger_event(event)


Event handlers are the opposite of command handlers: they never execute business logic and never create domain events.
Their only purpose is to take a domain event as an input parameter and change the aggregate's state by applying the values
in the event to the aggregate's attributes.

.. code-block:: python

    @register_aggregate_type('bond')
    class Bond(Aggregate):

        # Code omitted

        def _when_days_to_maturity_changed(self, event: DaysToMaturityChanged) -> None:
            self._days_to_maturity = event.new_days_to_maturity

        def _when_matured(self, event: Matured) -> None:
            self._matured = True


Such separation is necessary because event handlers are used during the reconstruction step, when events are consecutively
applied to the aggregate to reconstruct its final state.

.. note::

    It is good practice to make event handlers private, because they are supposed to be used internally by the framework
    and should not be a part of the public interface.


Event handler registration
--------------------------

The last thing that is left is to let the framework know which event handlers should be called for each domain event.
To do so, you need to implement the abstract ``get_event_handler_routing`` method. It should return a simple dictionary
with domain events as keys and event handlers as values:

.. code-block:: python

    @register_aggregate_type('bond')
    class Bond(Aggregate):

        # Code omitted

        def get_event_handler_routing(self) -> Dict[Type['Aggregate.AggregateEvent'], Callable]:
            routing = {
                self.DaysToMaturityChanged: self._when_days_to_maturity_changed,
                self.Matured: self._when_matured,
            }
            return routing


After that, the aggregate is complete and can be used in your applications.

This concludes the aggregate tutorial. In the next step, you will learn how to implement
use cases and how to react to events that are generated by your domain model.