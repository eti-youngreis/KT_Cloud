import json
from typing import List, Tuple
from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, EventSubscription, SourceType
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class EventSubscriptionService(DBO):

    def __init__(self, dal: EventSubscriptionManager, storage_manager: StorageManager, directory: str):
        self.dal = dal
        self.storage_manager = storage_manager
        self.directory = directory
        if not self.storage_manager.is_directory_exist(directory):
            self.storage_manager.create_directory(directory)

    def create(self, subscription_name: str, sources: List[Tuple[SourceType, str]],
               event_categories: List[EventCategory], sns_topic_arn: str, source_type: SourceType):

        event_subscription = EventSubscription(
            subscription_name, sources, event_categories, sns_topic_arn, source_type)

        self.dal.createInMemoryEventSubscription(event_subscription)

        self.storage_manager.create_file(self.get_file_path(
            subscription_name), json.dumps(event_subscription.to_dict()))

    def delete(self, subscription_name: str):
        self.dal.deleteInMemoryEventSubscription(subscription_name)
        self.storage_manager.delete_file(self.get_file_path(subscription_name))

    def modify(self, subscription_name: str, event_categories: List[EventCategory] = None, sns_topic_arn: str = None, source_type: SourceType = None):

        event_subscription = self.dal.describeEventSubscriptionById(
            subscription_name)

        if event_categories is not None:
            event_subscription.event_categories = event_categories
        if sns_topic_arn is not None:
            event_subscription.sns_topic_arn = sns_topic_arn
        if source_type is not None:
            event_subscription.source_type = source_type

        self.dal.modifyEventSubscription(
            event_subscription)

        self.storage_manager.write_to_file(self.get_file_path(
            subscription_name), json.dumps(event_subscription.to_dict()))

    def get_by_id(self, subscription_name: str):
        return self.dal.get_by_id(subscription_name)

    def describe(self, columns: List[str] = None, criteria: str = None):
        return self.dal.describeEventSubscriptionByCriteria(columns, criteria)

    def get(self, criteria: str = None):
        return self.dal.get(criteria)
    
    def get_file_path(self, subscription_name: str):
        return f'{self.directory}/{subscription_name}.json'
