import json
from typing import Any, Dict, List
from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, EventSubscription, SourceType
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class EventSubscriptionService(DBO):
    """
    A service class for managing event subscriptions.

    This class provides methods to create, delete, modify, and retrieve event subscriptions.
    It interacts with both an in-memory data access layer and a file storage system.

    Attributes:
        dal (EventSubscriptionManager): The data access layer for event subscriptions.
        storage_manager (StorageManager): The storage manager for file operations.
        directory (str): The directory path for storing event subscription files.
    """

    def __init__(self, dal: EventSubscriptionManager, storage_manager: StorageManager, directory: str):
        """
        Initialize the EventSubscriptionService.

        Args:
            dal (EventSubscriptionManager): The data access layer for event subscriptions.
            storage_manager (StorageManager): The storage manager for file operations.
            directory (str): The directory path for storing event subscription files.
        """
        self.dal = dal
        self.storage_manager = storage_manager
        self.directory = directory
        if not self.storage_manager.is_directory_exist(directory):
            self.storage_manager.create_directory(directory)

    def create(self, subscription_name: str, sources: List[str],
               event_categories: List[EventCategory], sns_topic: str, source_type: SourceType):
        """
        Create a new event subscription.

        Args:
            subscription_name (str): The name of the subscription.
            sources (List[str]): The list of sources for the subscription.
            event_categories (List[EventCategory]): The list of event categories.
            sns_topic (str): The ARN of the SNS topic.
            source_type (SourceType): The type of the source.
        """
        event_subscription = EventSubscription(
            subscription_name, sources, event_categories, sns_topic, source_type)

        self.dal.createInMemoryEventSubscription(event_subscription)

        self.storage_manager.create_file(self.get_file_path(
            subscription_name), json.dumps(event_subscription.to_dict()))

    def delete(self, subscription_name: str):
        """
        Delete an event subscription.

        Args:
            subscription_name (str): The name of the subscription to delete.
        """
        self.dal.deleteInMemoryEventSubscription(subscription_name)
        self.storage_manager.delete_file(self.get_file_path(subscription_name))

    def modify(self, subscription_name: str, event_categories: List[EventCategory] = None, sns_topic: str = None, source_type: SourceType = None):
        """
        Modify an existing event subscription.

        Args:
            subscription_name (str): The name of the subscription to modify.
            event_categories (List[EventCategory], optional): The new list of event categories.
            sns_topic (str, optional): The new ARN of the SNS topic.
            source_type (SourceType, optional): The new type of the source.
        """
        event_subscription = self.dal.get_by_id(
            subscription_name)

        if event_categories is not None:
            event_subscription.event_categories = event_categories
        if sns_topic is not None:
            event_subscription.sns_topic = sns_topic
        if source_type is not None:
            event_subscription.source_type = source_type

        self.dal.modifyEventSubscription(
            event_subscription)

        self.storage_manager.write_to_file(self.get_file_path(
            subscription_name), json.dumps(event_subscription.to_dict()))

    def get_by_id(self, subscription_name: str):
        """
        Get an event subscription by its name.

        Args:
            subscription_name (str): The name of the subscription to retrieve.

        Returns:
            EventSubscription: The event subscription with the given name.
        """
        return self.dal.get_by_id(subscription_name)

    def describe(self, columns: List[str] = None, criteria: Dict[str, Any] = None) -> List[Dict]:
        """
        Describe event subscriptions based on given criteria.

        Args:
            columns (List[str], optional): The columns to include in the description.
            criteria (Dict[str, Any], optional): The criteria to filter the subscriptions.

        Returns:
            List[Dict]: A list of dictionaries describing the matching event subscriptions.
        """
        return self.dal.describeEventSubscriptionByCriteria(columns, criteria)

    def describe_by_id(self, subscription_name: str) -> Dict:
        """
        Describe an event subscription by its name.

        Args:
            subscription_name (str): The name of the subscription to describe.

        Returns:
            Dict: A dictionary describing the event subscription.
        """
        return self.dal.describeEventSubscriptionById(subscription_name)

    def get(self, criteria: Dict[str, Any] = None):
        """
        Get event subscriptions based on given criteria.

        Args:
            criteria (Dict[str, Any], optional): The criteria to filter the subscriptions.

        Returns:
            List[EventSubscription]: A list of event subscriptions matching the criteria.
        """
        return self.dal.get(criteria)

    def get_file_path(self, subscription_name: str):
        """
        Get the file path for a given subscription name.

        Args:
            subscription_name (str): The name of the subscription.

        Returns:
            str: The file path for the subscription.
        """
        return f'{self.directory}/{subscription_name}.json'
