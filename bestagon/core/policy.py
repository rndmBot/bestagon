from abc import ABC, abstractmethod
from dataclasses import dataclass

from bestagon.domain.application import Application


class StreamNamePolicy(ABC):
    @abstractmethod
    def create_stream_name(self, aggregate_type: str, aggregate_id: str) -> str:
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


class SimpleStreamNamePolicy(StreamNamePolicy):
    """Returns stream name in form {AggregateType}-{AggregateId}"""
    def create_stream_name(self, aggregate_type: str, aggregate_id: str) -> str:
        return f'{aggregate_type}-{aggregate_id}'


class ApplicationStreamNamePolicy(StreamNamePolicy):
    """Returns steam name in form: {application_name}.{aggregate_type}-{aggregate_id}"""
    def __init__(self, application: Application):
        self._application = application

    @property
    def application(self) -> Application:
        return self._application

    def create_stream_name(self, aggregate_type: str, aggregate_id: str) -> str:
        # TODO - validation, aggregate type and id should not be empty
        return f'{self.application.name}.{aggregate_type}-{aggregate_id}'


class SystemStreamNamePolicy(StreamNamePolicy):
    """Returns steam name in form: {system_name}.{application_name}.{aggregate_type}-{aggregate_id}"""
    def __init__(self, application: Application):
        self._application = application

    @property
    def application(self) -> Application:
        return self._application

    def create_stream_name(self, aggregate_type: str, aggregate_id: str) -> str:
        # TODO - validation, aggregate type and id should not be empty
        return f'{self.application.system.name}.{self.application.name}.{aggregate_type}-{aggregate_id}'
