<<<<<<< HEAD
from typing import List, Tuple
import json
import os


from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription, SourceType, EventCategory
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from DB.NEW_KT_DB.Validation.EventSubscriptionValidations import validate_subscription_name_exist
=======
import json
from typing import Any, Dict, List, Tuple
from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, EventSubscription, SourceType
from DB.NEW_KT_DB.Service.Abc.DBO import DBO
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class EventSubscriptionService(DBO):
<<<<<<< HEAD

    def __init__(self, dal: EventSubscriptionManager, storage_manager: StorageManager, directory: str):
=======
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
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
        self.dal = dal
        self.storage_manager = storage_manager
        self.directory = directory
        if not self.storage_manager.is_directory_exist(directory):
            self.storage_manager.create_directory(directory)

<<<<<<< HEAD
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
        validate_subscription_name_exist(self.dal, subscription_name)
        # return real time object
        return self.dal.describeEventSubscriptionById(subscription_name=subscription_name, columns=columns)
=======
    def create(self, subscription_name: str, sources: List[Tuple[SourceType, str]],
               event_categories: List[EventCategory], sns_topic: str, source_type: SourceType):
        """
        Create a new event subscription.

        Args:
            subscription_name (str): The name of the subscription.
            sources (List[Tuple[SourceType, str]]): The list of sources for the subscription.
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
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
