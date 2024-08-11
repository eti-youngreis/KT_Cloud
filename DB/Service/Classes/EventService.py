import functools
from Models import EventModel
from typing import Dict
from DataAccess import DataAccessLayer
from datetime import datetime, timedelta
from Validation.Validation import check_required_params, check_extra_params, check_filters_validation


def log_event(func, event_service):
    @functools.wraps(func)
    def wrapper(**kwargs):
        # Find the 'id' argument in the arguments
        event_service.create(event_identifier=func.__name__,**kwargs)

        # Call the original function
        return func(**kwargs)
    
    return wrapper

class EventService():
    def __init__(self, dal: DataAccessLayer):
        self.dal = dal
        self.option_groups = {}

    def create(self, **kwargs):
        # Find the 'id' argument in the arguments
        event_identifier = kwargs['event_identifier']
        resource_identifier = self._find_first_id_in_kwargs(kwargs)
        event = EventModel(event_identifier, resource_identifier)
        self.dal.insert('Event', event.to_dict())


    def delete(self):
        pass

    def describe(self, **kwargs) -> Dict:
        """Describe events related to the DB instances."""

    
   




