import logging
from typing import Tuple, Dict

from bestagon.core.event_store import EventStore
from bestagon.domain.application import Projection, Application
from bestagon.exceptions import ApplicationError


logger = logging.getLogger(__name__)


class EventSourcedSystem:
    # TODO - application graph
    # TODO - execution policy
    # TODO - split workflow into two parts - application workflow (write side) and projection workflow (read side)

    def __init__(self, name: str):
        self._name = name
        self._applications: Dict[str, Application] = dict()
        self._projections: Dict[str, Projection] = dict()

    @property
    def applications(self) -> Tuple[Application, ...]:
        return tuple(self._applications.values())

    @property
    def projections(self) -> Tuple[Projection, ...]:
        return tuple(self._projections.values())

    def add_application(self, app: Application) -> None:
        if not isinstance(app, Application):
            raise TypeError(f'Invalid application type {type(app)}')

        if app.name in self.applications:
            return

        self._applications[app.name] = app

    def add_projection(self, projection: Projection) -> None:
        if not isinstance(projection, Projection):
            raise TypeError(f'Invalid projection type {type(projection)}')

        if projection in self.projections:
            return

        self._projections[projection.name] = projection

    def get_application(self, name: str) -> Application:
        app = self._applications.get(name)
        if app is None:
            raise ApplicationError(f'No application with name {name} found.')
        return app

    def get_projection(self, name: str) -> Projection:
        proj = self._projections.get(name)
        if proj is None:
            raise ApplicationError(f'No projection with name {name} found.')
        return proj

    def process_application(self, name: str) -> None:
        # TODO - zapili
        raise NotImplementedError

    def process_projection(self, name: str) -> None:
        # TODO - zapili
        raise NotImplementedError

    def rebuild_projection(self, name: str) -> None:
        # TODO - zapili
        raise NotImplementedError
