from typing import List, Tuple
import json
import os


from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription, SourceType, EventCategory
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class EventSubscriptionService(DBO):

    def __init__(self, dal: EventSubscriptionManager, storage_manager: StorageManager, directory: str):
        self.dal = dal
        self.storage_manager = storage_manager
        self.directory = directory
        if not self.storage_manager.is_directory_exist(directory):
            self.storage_manager.create_directory(directory)

    def get_file_path(self, subscription_name: str):
        return os.path.join(self.directory, subscription_name + '.json')

    def create(self, subscription_name: str, sources: List[Tuple[SourceType, str]], event_categories: List[EventCategory],
               sns_topic_arn: str, source_type: SourceType):
        '''Create a new EventSubscription.'''

        # validate input

        # create object in code using EventSubscriptionModel.init()- assign all attributes
        event_subscription = EventSubscription(subscription_name=subscription_name, sources=sources,
                                               event_categories=event_categories, sns_topic_arn=sns_topic_arn, source_type=source_type)

        # create physical object as described in task
        json_object = json.dumps(event_subscription.to_dict())
        file_path = self.get_file_path(subscription_name)
        self.storage_manager.create_file(
            file_path=file_path, content=json_object)

        # save in memory using EventSubscriptionManager.createInMemoryEventSubscription() function
        self.dal.createInMemoryEventSubscription(
            event_subscription=event_subscription)

    def delete(self, subscription_name: str):
        '''Delete an existing EventSubscription.'''

        # validate input

        # delete physical object
        file_path = self.get_file_path(subscription_name)
        self.storage_manager.delete_file(file_path=file_path)

        # delete from memory using EventSubscriptionManager.deleteInMemoryEventSubscription() function- send criteria using self attributes
        self.dal.deleteInMemoryEventSubscription(subscription_name)

    def describe(self, subscription_name: str, columns=['*']):
        '''Describe the details of EventSubscription.'''
        return self.dal.describeEventSubscription(subscription_name=subscription_name, columns=columns)

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
        file_path = self.get_file_path(subscription_name)
        self.storage_manager.update_file(file_path, json.dumps(json_data))

        self.storage_manager.delete_file(file_path)
        self.storage_manager.create_file(file_path, json.dumps(json_data))

    def get(self, subscription_name: str, columns=['*']) -> EventSubscription:
        '''get code object.'''
        # return real time object
        return self.dal.describeEventSubscription(subscription_name=subscription_name, columns=columns)
