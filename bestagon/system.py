import logging
from typing import TYPE_CHECKING, List, Tuple

from bestagon import utils
from bestagon.adapters.event_store import CheckpointStore, CheckpointName
from bestagon.exceptions import ApplicationError

if TYPE_CHECKING:
    from bestagon.component import Application


logger = logging.getLogger(__name__)


# TODO - delegate checkpoint management and event polling to Dispatcher
# TODO - applications and projections should have consumers, that constantly check if there are new events in subscriptions


class EventSourcedSystem:
    # TODO - application graph
    # TODO - execution policy
    # TODO - split workflow into two parts - application workflow (write side) and projection workflow (read side)

    def __init__(self, checkpoint_store: CheckpointStore):
        self._checkpoint_store = checkpoint_store
        self._applications: List['Application'] = list()

    @property
    def applications(self) -> Tuple['Application', ...]:
        return tuple(self._applications)

    @property
    def checkpoint_store(self) -> CheckpointStore:
        return self._checkpoint_store

    def add_application(self, app: 'Application') -> None:
        if app in self._applications:
            raise ApplicationError(f'Application with name {app.name} already exists.')
        self._applications.append(app)

    def get_application(self, name: str) -> 'Application':
        for app in self.applications:
            if app.name == name:
                return app
        raise ApplicationError(f'No application with name {name} found.')

    def process_application(self, application: 'Application') -> None:
        logger.info(f'Processing application {application.name}')
        leaders = [self.get_application(leader_name) for leader_name in application.leaders]

        # Do nothing if no leaders specified
        if not leaders:
            logger.info(f'No leaders specified for application {application.name}')
            return

        # For each leader
        for leader in leaders:
            logger.info(f'Consumer events from {leader.name} applicaiton sequence')
            checkpoint_name = CheckpointName(
                leader=leader.name,
                follower=application.name
            )

            # Consume all events from leader's application sequence
            while True:
                checkpoint = self.checkpoint_store.get_checkpoint(name=checkpoint_name)  # TODO - what if no checkpoint
                logger.info(f'Last checkpoint found: {checkpoint_name} - {checkpoint}')

                application_events = leader.get_application_events(start_position=checkpoint, limit=100)  # TODO - make limit adjustable

                # First event already processed and sould be removed
                if checkpoint and application_events:
                    first_event = application_events.pop(0)
                    assert first_event.sequence_position == checkpoint

                if not application_events:
                    logger.info(f'No application events loaded.')
                    break

                logger.info(f'Processing {len(application_events)} application events.')
                for application_event in application_events:
                    domain_event = utils.convert_to_domain_event(application_event)
                    application.policy(event=domain_event)
                    self.checkpoint_store.set_checkpoint(name=checkpoint_name, value=application_event.sequence_position)
                logger.info('Application events processed.')

    def process_applications(self) -> None:
        # TODO - ACHTUNG - not reliable, what if there are cyclic dependencies???
        for application in self.applications:
            self.process_application(application)
