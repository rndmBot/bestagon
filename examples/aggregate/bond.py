from dataclasses import dataclass
from datetime import date
from uuid import uuid5, NAMESPACE_URL

from bestagon.core.aggregate import Aggregate, DomainEvent, DomainEventMetadata
from bestagon.core.exceptions import DomainException
from bestagon.core.mapper import register_aggregate_type, register_event_type


@register_aggregate_type()
class Bond(Aggregate):

    # Events are defined as nested classes to keep things in order
    @dataclass(frozen=True)
    @register_event_type('BondIssued')
    class Issued(Aggregate.Created):
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
    class DaysToMaturityChanged(Aggregate.Event):
        isin: str
        old_days_to_maturity: int
        new_days_to_maturity: int

    @dataclass(frozen=True)
    @register_event_type('BondMatured')
    class Matured(Aggregate.Event):
        isin: str

    def __init__(self, event: Issued):
        super().__init__(event)

        # It is a good practice to make attributes private to protect them from occasional change
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

    # Attributes can be accessible through getters or properties.
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
        if self.days_to_maturity == 0:
            return True
        return False

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

    # Private methots, that start with `_when` are event handlers, they are responsible for changing the state of the aggregate
    def _when_days_to_maturity_changed(self, event: DaysToMaturityChanged) -> None:
        self._days_to_maturity = event.new_days_to_maturity

    def _when_matured(self, event: Matured) -> None:
        self._matured = True

    def apply_event(self, event: DomainEvent) -> None:
        event_routing = {
            self.DaysToMaturityChanged: self._when_days_to_maturity_changed,
            self.Matured: self._when_matured,
        }
        event_handler = event_routing.get(type(event))
        if event_handler is not None:
            event_handler(event)
        else:
            super().apply_event(event)

    @staticmethod
    def create_id(isin: str) -> str:
        aggregate_uid = uuid5(NAMESPACE_URL, isin)
        return str(aggregate_uid)

    @staticmethod
    def get_aggregate_type() -> str:
        return 'bond'

    def set_matured(self) -> None:
        if self.matured:
            return

        if self.days_to_maturity != 0:
            raise DomainException('Only bonds with days to maturity of 0 can be matured')

        metadata = DomainEventMetadata(
            timestamp=DomainEventMetadata.create_timestamp(),
            aggregate_id=self.aggregate_id,
            aggregate_version=self.next_version,
            aggregate_type=self.get_aggregate_type()
        )
        event = self.Matured(
            metadata=metadata,
            isin=str(self.isin)
        )
        self.trigger_event(event)

    def update_days_to_maturity(self) -> None:
        if self.matured:
            return

        today = date.today()
        days_to_maturity = (self.maturity_date - today).days
        new_days_to_maturity = max([0, days_to_maturity])

        if self.days_to_maturity == new_days_to_maturity:
            return

        metadata = DomainEventMetadata(
            timestamp=DomainEventMetadata.create_timestamp(),
            aggregate_id=self.aggregate_id,
            aggregate_version=self.next_version,
            aggregate_type=self.get_aggregate_type()
        )
        event = self.DaysToMaturityChanged(
            metadata=metadata,
            isin=str(self.isin),
            old_days_to_maturity=self.days_to_maturity,
            new_days_to_maturity=new_days_to_maturity
        )
        self.trigger_event(event)