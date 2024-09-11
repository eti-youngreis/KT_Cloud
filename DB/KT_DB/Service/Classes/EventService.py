import functools
from DB.DataAccess.EventManager import EventManager
from Models.EventModel import EventModel
from typing import Dict
from datetime import datetime, timedelta
from Validation.Validation import check_required_params, check_extra_params, check_filters_validation


def log_event(func, event_service):
    @functools.wraps(func)
    def wrapper(**kwargs):
        # Find the 'id' argument in the arguments
        event_service.create(event_identifier=func.__name__, **kwargs)

        # Call the original function
        return func(**kwargs)

    return wrapper


class EventService():
    def __init__(self, event_dal: EventManager):
        self.event_dal = event_dal

    def create(self, event_identifier: str, resource_identifier: str):
        event = EventModel(event_identifier, resource_identifier)
        self.event_dal.create(metadata=event.to_dict())

    def get_all_events(self):
        return self.event_dal.get_all_events()

    def get_all_unprocessed_events(self):
        return self.event_dal.get_all_unprocessed_events()

    def get(self, event_id: str):
        return self.event_dal.get(event_id=event_id)

    def delete(self):
        pass

    def describe(self, **kwargs) -> Dict:
        """Describe events related to the DB instances."""
