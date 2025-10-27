from abc import ABC, abstractmethod


class StreamNamePolicy(ABC):
    @abstractmethod
    def create_stream_name(self, aggregate_type: str, aggregate_id: str) -> str:
        raise NotImplementedError


class ApplicationStreamNamePolicy(StreamNamePolicy):
    """Returns steam name in form: {application_name}.{aggregate_type}-{aggregate_id}"""
    def __init__(self, application_name: str):
        self._application_name = application_name

    @property
    def application_name(self) -> str:
        return self._application_name

    def create_stream_name(self, aggregate_type: str, aggregate_id: str) -> str:
        assert self.application_name
        assert aggregate_type
        assert aggregate_id

        return f'{self.application_name}.{aggregate_type}-{aggregate_id}'
