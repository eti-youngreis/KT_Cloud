import functools
from Models import EventModel
from typing import Dict
from DataAccess import DataAccessLayer
from Abc import DBO
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

class EventService(DBO):
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
        
        # max num of events days to describe
        max_event_days = 14
        # params validation
        all_params = [ 'source_identifier','source_type','start_time','end_time','duration','event_categories','filters''max_records','marker']
        if not check_extra_params(all_params, kwargs):
            raise ParamValidationError(f'Unknown parameter in input, must be one of: {"".join(all_params)}')
        

        # put params to variables
        source_id = kwargs.get('source_identifier',None)
        src_type = kwargs.get('source_type',None)
        start_time = kwargs.get('start_time',None)
        end_time = kwargs.get('end_time',None)
        duration = kwargs.get('duration',None)
        event_categories = kwargs.get('event_categories',None)
        filters = kwargs.get('filters',None)
        max_records = kwargs.get('max_records',None)
        marker = kwargs.get('marker',None)

        # params validation
        if not not duration == start_time and end_time:
            raise ParamValidationError('can get only duration or only start time and end time')
        if source_id and not src_type:
            raise ParamValidationError('If source_identifier is supplied, source_type must also be provided.')
        if src_type and src_type not in ['db-instance','db-parameter-group','db-security-group','db-snapshot','db-cluster','db-cluster-snapshot','custom-engine-version','db-proxy','blue-green-deployment']:
            raise ParamValidationError('source type must be one of [db-instance,db-parameter-group,db-security-group,db-snapshot,db-cluster,db-cluster-snapshot,custom-engine-version,db-proxy,blue-green-deployment]')
        if filters:
            check_filters_validation(kwargs['filters'])
    
        # set start time and end time, by default 14 days ago and today
        if kwargs.get('duration'):
            if kwargs.get('start_time'):
                start_time = kwargs['start_time']
                end_time = start_time + timedelta(minutes=duration)
            elif kwargs.get('end_time'):
                start_time = kwargs['start_time']
                end_time = start_time + timedelta(minutes=duration)
            else:
                end_time = datetime.now()
                start_time = end_time - timedelta(minutes=duration)
        if not start_time:
            start_time = datetime.now() - timedelta(days=max_event_days)
        if not end_time:
            end_time = datetime.now()
        
        # select events     
        criteria = f'''type_object = 'Event' AND json_extract(metadata, '$.creation_time') between {start_time.strftime('%Y-%m-%d %H:%M:%S')} and {end_time.strftime('%Y-%m-%d %H:%M:%S')}'''
        selected_events = self.dal.select('Event', criteria)
    
    
        events_to_return = []
        for event in selected_events:
            event_id, resource_id, date = event['event_id'] ,event['resource_id'], event['created_time']
            event_data = get_event_by_func_name(True,event_id)
            event_to_return = {
                'source_identifier':resource_id,
                'source_type':event_data['resource_type'],
                'message': event_data['message'],
                'event_category': event_data['category'],
                'date': datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

            }
            events_to_return.append(event_to_return)
        if source_id:
            events_to_return = [event for event in events_to_return if event['source_identifier'] == source_id and event['source_type'] == src_type]
        if event_categories:
            events_to_return = [event for event in events_to_return if event['event_category'] in event_categories]
        return {'Events': events_to_return}


    def modify(self):
        pass
    
    def _find_first_id_in_kwargs(kwargs):
        for key, value in kwargs.items():
            if 'identifier' in key:
                return value
        return None  # If no key containing 'id' is found

class ParamValidationError(Exception):
    pass



