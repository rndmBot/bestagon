from typing import Tuple, Dict

from bestagon.core.checkpoint_store import CheckpointStore
from bestagon.domain.application import Projection, Application, Follower
from bestagon.exceptions import ApplicationError


class EventSourcedSystem:
    # TODO - application graph
    # TODO - split workflow into two parts - application workflow (write side) and projection workflow (read side)

    def __init__(self, name: str, checkpoint_store: CheckpointStore):
        self._name = name
        self._applications: Dict[str, Application] = dict()
        self._projections: Dict[str, Projection] = dict()
        self._checkpoint_store = checkpoint_store

    @property
    def applications(self) -> Tuple[Application, ...]:
        return tuple(self._applications.values())

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    @property
    def name(self) -> str:
        return self._name

    @property
    def projections(self) -> Tuple[Projection, ...]:
        return tuple(self._projections.values())

    def _process_follower(self, follower: Follower) -> None:
        for leader_name in follower.leaders:
            leader = self.get_application(name=leader_name)
            checkpoint_name = f'{follower.name}-FOLLOWS-{leader_name}'  # TODO - ORLY???

            while True:
                last_checkpoint = self.checkpoint_store.get_checkpoint(name=checkpoint_name)
                events = leader.get_events(start_position=last_checkpoint, limit=100)  # TODO - make limit changeable
                events.pop(0)  # First event already processed

                if not events:
                    break

                for event, commit_postition in events:
                    follower.process_event(event)
                    self.checkpoint_store.set_checkpoint(name=checkpoint_name, value=commit_postition)

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
        app = self.get_application(name=name)
        self._process_follower(follower=app)

    def process_projection(self, name: str) -> None:
        proj = self.get_projection(name=name)
        self._process_follower(follower=proj)

    def rebuild_projection(self, name: str) -> None:
        # TODO - zapili
        # TODO - should reset checkpoint
        raise NotImplementedError
