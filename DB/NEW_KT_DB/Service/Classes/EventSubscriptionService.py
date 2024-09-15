from typing import List, Tuple
import json
import os


from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription, SourceType, EventCategory
from DB.NEW_KT_DB.Service.Abc.DBO import DBO

PATH = 'server'


class EventSubscriptionService(DBO):

    def __init__(self, dal: EventSubscriptionManager):
        self.dal = dal

    def create(self, subscription_name: str, sources: List[Tuple[SourceType, str]], event_categories: List[EventCategory],
               sns_topic_arn: str, source_type: SourceType):
        '''Create a new EventSubscription.'''

        # validate input

        # create object in code using EventSubscriptionModel.init()- assign all attributes
        event_subscription = EventSubscription(subscription_name=subscription_name, sources=sources,
                                               event_categories=event_categories, sns_topic_arn=sns_topic_arn, source_type=source_type)

        # create physical object as described in task
        json_object = event_subscription.to_dict()
        file_name = PATH + '/' + subscription_name + '.json'
        with open(file_name, 'w') as f:
            json.dump(json_object, f)

        # save in memory using EventSubscriptionManager.createInMemoryEventSubscription() function
        self.dal.createInMemoryEventSubscription(
            event_subscription=event_subscription)

    def delete(self, subscription_name: str):
        '''Delete an existing EventSubscription.'''

        # validate input

        # delete physical object
        file_name = PATH + '/' + subscription_name + '.json'

        if os.path.exists(file_name):
            os.remove(file_name)
            print(f'Deleted {file_name}')

        else:
            print(f'{file_name} does not exist')

        # delete from memory using EventSubscriptionManager.deleteInMemoryEventSubscription() function- send criteria using self attributes
        self.dal.deleteInMemoryEventSubscription(subscription_name)

    def describe(self):
        '''Describe the details of EventSubscription.'''
        return self.dal.describeEventSubscription()

    def modify(self, subscription_name: str, **updates):
        '''Modify an existing EventSubscription.'''

        # Get the current subscription
        current_subscription = self.dal.describeEventSubscription()

        # Update the subscription object
        for key, value in updates.items():
            setattr(current_subscription, key, value)

        # Update in-memory representation
        self.dal.modifyEventSubscription(current_subscription)

        # Update physical JSON file
        json_data = current_subscription.to_dict()
        file_name = f"{subscription_name}.json"
        file_path = os.path.join(PATH, file_name)

        with open(file_path, 'w') as json_file:
            json.dump(json_data, json_file)

        print(f"EventSubscription '{
              subscription_name}' modified successfully.")

    def get(self, subscription_name: str, columns=['*']) -> EventSubscription:
        '''get code object.'''
        # return real time object
        return self.dal.describeEventSubscription(subscription_name=subscription_name, columns=columns)
