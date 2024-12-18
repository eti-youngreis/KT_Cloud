from typing import Dict, Any, List, Optional, Tuple
import json
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, EventSubscription, SourceType


class EventSubscriptionManager:
    """
    Manages event subscriptions in the database.
    """

    def __init__(self, db_file: str):
        """
        Initialize EventSubscriptionManager with the database connection.

        Args:
            db_file (str): Path to the SQLite database file.
        """
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(
            EventSubscription.get_object_name(), EventSubscription.table_structure, pk_column_data_type='TEXT')

    def createInMemoryEventSubscription(self, event_subscription: EventSubscription) -> None:
        """
        Create a new event subscription in memory.

        Args:
            event_subscription (EventSubscription): The event subscription to create.
        """
        self.object_manager.save_in_memory(
            event_subscription.get_object_name(), event_subscription.to_sql())

    def deleteInMemoryEventSubscription(self, subscription_name: str) -> None:
        """
        Delete an event subscription from memory.

        Args:
            subscription_name (str): The name of the subscription to delete.
        """
        self.object_manager.delete_from_memory_by_pk(
            EventSubscription.get_object_name(), EventSubscription.pk_column, subscription_name)

    def describeEventSubscriptionById(self, subscription_name: str) -> Dict:
        """
        Retrieve an event subscription by its ID (name).

        Args:
            subscription_name (str): The name of the subscription to retrieve.

        Returns:
            Dict: A dictionary representation of the event subscription, or None if not found.
        """
        event_subscription = self.object_manager.get_from_memory(
            EventSubscription.get_object_name(),
            criteria=f'{EventSubscription.pk_column} = "{subscription_name}"'
        )
        if not event_subscription:
            return None
        return EventSubscription.values_to_dict(*event_subscription[0])

    def modifyEventSubscription(self, event_subscription: EventSubscription) -> None:
        """
        Modify an existing event subscription in memory.

        Args:
            event_subscription (EventSubscription): The updated event subscription.
        """
        subscription_dict = event_subscription.to_dict()
        updates = []
        for key, value in subscription_dict.items():
            if isinstance(value, (dict, list)):
                updates.append(f"{key} = '{json.dumps(value)}'")
            elif isinstance(value, str):
                updates.append(f"{key} = '{value}'")
            else:
                updates.append(f"{key} = {repr(value)}")

        updates = ", ".join(updates)
        self.object_manager.update_in_memory(
            EventSubscription.get_object_name(),
            updates,
            f'{EventSubscription.pk_column} = "{event_subscription.pk_value}"'
        )

    def describeEventSubscriptionByCriteria(self, columns: Optional[List[str]] = '*', criteria: Dict[str, Any] = None) -> List[Dict]:
        """
        Retrieve event subscriptions based on specified criteria.

        Args:
            columns (Optional[List[str]]): List of columns to retrieve. Defaults to all columns.
            criteria (Dict[str, Any]): Criteria for filtering event subscriptions.

        Returns:
            List[Dict]: A list of dictionaries representing the matching event subscriptions.
        """
        if criteria:
            key, value = next(iter(criteria.items()))
            criteria = f'{key} = "{value}"'
        event_subscription_data = self.object_manager.get_from_memory(
            EventSubscription.get_object_name(), columns, criteria
        )
        return [EventSubscription.values_to_dict(*event_subscription) for event_subscription in event_subscription_data]

    def get(self, criteria: Dict[str, Any] = None) -> List[EventSubscription]:
        """
        Retrieve event subscriptions based on specified criteria.

        Args:
            criteria (Dict[str, Any]): Criteria for filtering event subscriptions.

        Returns:
            List[EventSubscription]: A list of EventSubscription objects matching the criteria.
        """
        if criteria:
            key, value = criteria.popitem()
            criteria = f'{key} = "{value}"'
        event_subscriptions_data = self.object_manager.get_from_memory(
            EventSubscription.get_object_name(), criteria=criteria)

        return [EventSubscriptionManager.sql_to_object(event_subscription) for event_subscription in event_subscriptions_data]

    def get_by_id(self, subscription_name: str) -> EventSubscription:
        """
        Retrieve an event subscription by its ID (name).

        Args:
            subscription_name (str): The name of the subscription to retrieve.

        Returns:
            EventSubscription: The EventSubscription object, or None if not found.
        """
        event_subscriptions_data = self.object_manager.get_from_memory(
            EventSubscription.get_object_name(), criteria=f'{EventSubscription.pk_column} = "{subscription_name}"')

        if not event_subscriptions_data:
            return None

        return EventSubscriptionManager.sql_to_object(event_subscriptions_data[0])


    @staticmethod
    def sql_to_object(sql_subscription: Tuple[str]) -> EventSubscription:
        """
        Convert SQL data to an EventSubscription object.

        Args:
            sql_subscription (Tuple[str]): SQL data representing an event subscription.

        Returns:
            EventSubscription: The converted EventSubscription object.
        """
        subscription_name, sources, source_type, event_categories, sns_topic = sql_subscription
        sources = json.loads(sources)
        event_categories = json.loads(event_categories)

        sources_list = [(SourceType(source_type), source_id) for source_type,
                        source_ids in sources.items() for source_id in source_ids]

        event_categories_list = [EventCategory(
            category) for category in event_categories]

        event_subscription = EventSubscription(
            subscription_name=subscription_name,
            sources=sources_list,
            event_categories=event_categories_list,
            sns_topic=sns_topic,
            source_type=SourceType(source_type)
        )

        return event_subscription
