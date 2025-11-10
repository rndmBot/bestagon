from abc import ABC, abstractmethod


class StreamNamePolicy(ABC):
    # TODO - ORLY - get rid of it???

    @abstractmethod
    def create_stream_name(self, aggregate_type: str, aggregate_id: str) -> str:
        raise NotImplementedError


class SimpleStreamNamePolicy(StreamNamePolicy):
    def create_stream_name(self, aggregate_type: str, aggregate_id: str) -> str:
        return f'{aggregate_type}-{aggregate_id}'
