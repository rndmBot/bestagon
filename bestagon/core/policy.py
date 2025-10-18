from abc import ABC, abstractmethod
from dataclasses import dataclass


class StreamNamePolicy(ABC):
    @abstractmethod
    def create_stream_name(self, aggregate_id: str) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class ConventionStreamName:
    """Convention for stream name {SystemName}.{ApplicationName}.{AggregateType}-{AggregateId}"""
    system_name: str
    application_name: str
    aggregate_type: str
    aggregate_id: str

    def __eq__(self, other):
        if isinstance(other, ConventionStreamName):
            return self.to_string() == other.to_string()
        return NotImplemented

    def __post_init__(self):
        if not self.system_name:
            raise ValueError('System name cannot be empty.')
        if not self.application_name:
            raise ValueError('Application name cannot be empty.')
        if not self.aggregate_type:
            raise ValueError('Aggregate type cannot be empty.')
        if not self.aggregate_id:
            raise ValueError('Aggregate ID cannot be empty.')

    def __str__(self):
        return self.to_string()

    def to_string(self) -> str:
        return f'{self.system_name}.{self.application_name}.{self.aggregate_type}-{self.aggregate_id}'

    @classmethod
    def from_string(cls, s: str) -> 'ConventionStreamName':
        names, aggregate_id = s.split('-')
        system_name, application_name, aggregate_type = names.split('.')
        obj = cls(
            system_name=system_name,
            application_name=application_name,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id
        )
        return obj


class ConventionStreamNamePolicy(StreamNamePolicy):
    def __init__(self, application_name: ConventionApplicationName, aggregate_type: str):
        self._application_name = application_name
        self._aggregate_type = aggregate_type

    @property
    def aggregate_type(self) -> str:
        return self._aggregate_type

    @property
    def application_name(self) -> ConventionApplicationName:
        return self._application_name

    def create_stream_name(self, aggregate_id: str) -> str:
        stream_name = ConventionStreamName(
            system_name=self.application_name.system_name,
            application_name=self.application_name.application_name,
            aggregate_type=self.aggregate_type,
            aggregate_id=aggregate_id
        )
        return stream_name.to_string()
