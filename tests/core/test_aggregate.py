from dataclasses import FrozenInstanceError, dataclass
from typing import Callable, Dict, Type

import pytest

from bestagon.core.aggregate import Aggregate, PendingEvent
from bestagon.core.exceptions import AggregateTypeNotRegisteredError, EventHandlerNotRegisteredError
from bestagon.core.message import DomainEvent, DomainEventMetadata, DomainMessage
from bestagon.core.registry import register_aggregate_type, register_event_type

# The type registries are process wide singletons, so every type registered here is prefixed to avoid
# colliding with the types registered by the rest of the suite.


@register_aggregate_type('test-counter')
class Counter(Aggregate):
    @dataclass(frozen=True)
    @register_event_type('test-CounterOpened')
    class Opened(Aggregate.AggregateCreated):
        start: int = 0

    @dataclass(frozen=True)
    @register_event_type('test-CounterIncremented')
    class Incremented(Aggregate.AggrgateEvent):
        amount: int = 1

    @dataclass(frozen=True)
    class Unrouted(Aggregate.AggrgateEvent):
        """An event that is deliberately left out of `get_event_handler_routing`."""
        pass

    def __init__(self, event: 'Counter.Opened', aggregate_id: str, aggregate_version: int):
        super().__init__(event=event, aggregate_id=aggregate_id, aggregate_version=aggregate_version)
        self._value = event.start

    @property
    def value(self) -> int:
        return self._value

    @staticmethod
    def create_id(name: str) -> str:
        return f'counter-{name}'

    @classmethod
    def open(cls, name: str = 'a', start: int = 0) -> 'Counter':
        event = cls.Opened(start=start)
        return cls.create_aggregate(event=event, aggregate_id=cls.create_id(name=name))

    def get_event_handler_routing(self) -> Dict[Type[Aggregate.AggrgateEvent], Callable]:
        return {self.Incremented: self._when_incremented}

    def _when_incremented(self, event: 'Counter.Incremented') -> None:
        self._value += event.amount


def message(event: DomainEvent, aggregate_version: int, aggregate_id: str = 'counter-a') -> DomainMessage:
    metadata = DomainEventMetadata(
        timestamp='2026-10-13T12:00:00Z',
        event_id='event-1',
        event_type='test-CounterIncremented',
        aggregate_id=aggregate_id,
        aggregate_version=aggregate_version,
        aggregate_type='test-counter',
        correlation_id='correlation-1',
        causation_id='causation-1'
    )
    return DomainMessage(event=event, metadata=metadata)


class TestCreateAggregate:
    def test_creates_an_aggregate_at_the_initial_version(self):
        counter = Counter.open(name='a', start=5)

        assert counter.aggregate_id == 'counter-a'
        assert counter.aggregate_version == Counter.INITIAL_VERSION
        assert counter.value == 5

    def test_records_the_creation_event_as_the_only_pending_event(self):
        counter = Counter.open(start=5)

        assert counter.pending_events == (
            PendingEvent(aggregate_version=Counter.INITIAL_VERSION, domain_event=Counter.Opened(start=5)),
        )

    def test_leaves_the_original_version_unset_so_the_stream_is_written_as_new(self):
        counter = Counter.open()

        assert counter.original_version is None

    def test_does_not_apply_the_creation_event_through_the_event_handler_routing(self):
        # The creation event is consumed by __init__, not by a `_when_...` handler, so no routing entry
        # is needed for it.
        counter = Counter.open(start=5)

        assert Counter.Opened not in counter.get_event_handler_routing()

    def test_rejects_an_event_that_is_not_an_aggregate_created_event(self):
        with pytest.raises(TypeError, match='must be a subclass of <Aggregate.AggregateCreated>'):
            Counter.create_aggregate(event=Counter.Incremented(amount=1), aggregate_id='counter-a')

    def test_rejects_a_bare_domain_event(self):
        with pytest.raises(TypeError, match='must be a subclass of <Aggregate.AggregateCreated>'):
            Counter.create_aggregate(event=DomainEvent(), aggregate_id='counter-a')


class TestTriggerEvent:
    def test_applies_the_event_to_the_aggregate_state(self):
        counter = Counter.open(start=5)

        counter.trigger_event(Counter.Incremented(amount=3))

        assert counter.value == 8

    def test_bumps_the_aggregate_version(self):
        counter = Counter.open()

        counter.trigger_event(Counter.Incremented())

        assert counter.aggregate_version == Counter.INITIAL_VERSION + 1

    def test_records_the_pending_event_at_the_version_the_event_produced(self):
        counter = Counter.open()

        counter.trigger_event(Counter.Incremented(amount=3))

        assert counter.pending_events[-1] == PendingEvent(
            aggregate_version=Counter.INITIAL_VERSION + 1,
            domain_event=Counter.Incremented(amount=3)
        )

    def test_accumulates_pending_events_in_order(self):
        counter = Counter.open()

        counter.trigger_event(Counter.Incremented(amount=1))
        counter.trigger_event(Counter.Incremented(amount=2))

        versions = [pending.aggregate_version for pending in counter.pending_events]
        amounts = [pending.domain_event.amount for pending in counter.pending_events[1:]]
        assert versions == [0, 1, 2]
        assert amounts == [1, 2]

    def test_leaves_the_original_version_untouched(self):
        counter = Counter.open()
        counter.mutate(message(event=Counter.Incremented(), aggregate_version=5))

        counter.trigger_event(Counter.Incremented())

        assert counter.aggregate_version == 6
        assert counter.original_version == 5

    def test_rejects_an_aggregate_created_event(self):
        counter = Counter.open()

        with pytest.raises(TypeError, match='must be a subclass of <Aggregate.AggrgateEvent>'):
            counter.trigger_event(Counter.Opened())

    def test_rejects_a_bare_domain_event(self):
        counter = Counter.open()

        with pytest.raises(TypeError, match='must be a subclass of <Aggregate.AggrgateEvent>'):
            counter.trigger_event(DomainEvent())

    def test_an_unrouted_event_leaves_the_aggregate_unchanged(self):
        counter = Counter.open(start=5)

        with pytest.raises(EventHandlerNotRegisteredError):
            counter.trigger_event(Counter.Unrouted())

        assert counter.value == 5
        assert counter.aggregate_version == Counter.INITIAL_VERSION
        assert len(counter.pending_events) == 1


class TestApplyEvent:
    def test_routes_the_event_to_its_handler(self):
        counter = Counter.open(start=5)

        counter.apply_event(Counter.Incremented(amount=3))

        assert counter.value == 8

    def test_does_not_touch_the_version_or_the_pending_events(self):
        counter = Counter.open()
        pending_before = counter.pending_events

        counter.apply_event(Counter.Incremented())

        assert counter.aggregate_version == Counter.INITIAL_VERSION
        assert counter.pending_events == pending_before

    def test_raises_when_no_handler_is_routed_for_the_event(self):
        counter = Counter.open()

        with pytest.raises(EventHandlerNotRegisteredError, match='no event handler provided for the event'):
            counter.apply_event(Counter.Unrouted())


class TestMutate:
    def test_applies_the_event_to_the_aggregate_state(self):
        counter = Counter.open(start=5)

        counter.mutate(message(event=Counter.Incremented(amount=3), aggregate_version=1))

        assert counter.value == 8

    def test_takes_the_version_from_the_metadata_rather_than_incrementing(self):
        counter = Counter.open()

        counter.mutate(message(event=Counter.Incremented(), aggregate_version=7))

        assert counter.aggregate_version == 7

    def test_moves_the_original_version_so_a_replayed_aggregate_is_not_seen_as_new(self):
        counter = Counter.open()

        counter.mutate(message(event=Counter.Incremented(), aggregate_version=7))

        assert counter.original_version == 7

    def test_does_not_record_a_pending_event(self):
        # Replayed events are already persisted, re-recording them would write duplicates on the next save.
        counter = Counter.open()
        counter.clear_pending_events()

        counter.mutate(message(event=Counter.Incremented(), aggregate_version=1))

        assert counter.pending_events == ()

    def test_rejects_an_aggregate_created_event(self):
        counter = Counter.open()

        with pytest.raises(TypeError, match='must be a subclass of <Aggregate.AggrgateEvent>'):
            counter.mutate(message(event=Counter.Opened(), aggregate_version=1))

    def test_replays_a_sequence_of_events(self):
        counter = Counter.open(start=0)
        counter.clear_pending_events()

        for version, amount in enumerate([1, 2, 3], start=1):
            counter.mutate(message(event=Counter.Incremented(amount=amount), aggregate_version=version))

        assert counter.value == 6
        assert counter.aggregate_version == 3
        assert counter.original_version == 3
        assert counter.pending_events == ()


class TestPendingEvents:
    def test_returns_a_snapshot_detached_from_the_aggregate(self):
        counter = Counter.open()
        snapshot = counter.pending_events

        counter.trigger_event(Counter.Incremented())

        assert len(snapshot) == 1
        assert len(counter.pending_events) == 2

    def test_clearing_keeps_the_aggregate_version(self):
        counter = Counter.open()
        counter.trigger_event(Counter.Incremented())

        counter.clear_pending_events()

        assert counter.pending_events == ()
        assert counter.aggregate_version == Counter.INITIAL_VERSION + 1

    def test_clearing_an_aggregate_without_pending_events_is_a_no_op(self):
        counter = Counter.open()
        counter.clear_pending_events()

        counter.clear_pending_events()

        assert counter.pending_events == ()

    def test_a_pending_event_is_immutable(self):
        counter = Counter.open()

        with pytest.raises(FrozenInstanceError):
            counter.pending_events[0].aggregate_version = 99


class TestAggregateType:
    def test_returns_the_registered_aggregate_type(self):
        counter = Counter.open()

        assert counter.aggregate_type == 'test-counter'

    def test_a_subclass_does_not_inherit_the_aggregate_type_of_its_parent(self):
        # __init_subclass__ resets _AGGREGATE_TYPE so an unregistered child fails loudly instead of
        # being persisted under its parent's type.
        class UnregisteredCounter(Counter):
            pass

        counter = UnregisteredCounter.open()

        with pytest.raises(AggregateTypeNotRegisteredError, match='No aggregate type is registered'):
            counter.aggregate_type

    def test_a_subclass_reports_its_own_registered_aggregate_type(self):
        @register_aggregate_type('test-counter-child')
        class ChildCounter(Counter):
            pass

        counter = ChildCounter.open()

        assert counter.aggregate_type == 'test-counter-child'
        assert Counter.open().aggregate_type == 'test-counter'


class TestAbstractContract:
    def test_the_base_aggregate_cannot_be_instantiated(self):
        with pytest.raises(TypeError, match='abstract'):
            Aggregate(event=Aggregate.AggregateCreated(), aggregate_id='a', aggregate_version=0)

    def test_an_aggregate_without_an_event_handler_routing_cannot_be_instantiated(self):
        class NoRouting(Aggregate):
            @staticmethod
            def create_id(*args, **kwargs) -> str:
                return 'id'

        with pytest.raises(TypeError, match='get_event_handler_routing'):
            NoRouting(event=Aggregate.AggregateCreated(), aggregate_id='a', aggregate_version=0)

    def test_an_aggregate_without_an_id_factory_cannot_be_instantiated(self):
        class NoIdFactory(Aggregate):
            def get_event_handler_routing(self) -> Dict[Type[Aggregate.AggrgateEvent], Callable]:
                return {}

        with pytest.raises(TypeError, match='create_id'):
            NoIdFactory(event=Aggregate.AggregateCreated(), aggregate_id='a', aggregate_version=0)

    def test_the_concrete_id_factory_is_reachable_from_the_class(self):
        assert Counter.create_id(name='a') == 'counter-a'
