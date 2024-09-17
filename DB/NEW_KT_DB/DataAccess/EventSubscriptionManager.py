import json
from typing import Any, Dict, List, Optional
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class EventSubscriptionManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(
            EventSubscription.get_object_name(), EventSubscription.table_schema)

    def createInMemoryEventSubscription(self, event_subscription: EventSubscription) -> None:
        self.object_manager.save_in_memory(event_subscription.get_object_name(), event_subscription.to_sql(
        ))

    def deleteInMemoryEventSubscription(self, subscription_name: str) -> None:
        self.object_manager.delete_from_memory_by_pk(
            EventSubscription.get_object_name(), EventSubscription.pk_column, subscription_name)

    def describeEventSubscription(self, subscription_name: str, columns: Optional[List[str]] = None, criteria: Optional[str] = None) -> EventSubscription:
        event_subscription_dict = self.object_manager.get_all_objects_from_memory(
            EventSubscription.get_object_name())
        event_subscription = EventSubscription(
            **json.loads(event_subscription_dict))
        return event_subscription

    def modifyEventSubscription(self, subscription_name: str, updates: Dict[str, Any]) -> None:
        self.object_manager.update_in_memory(EventSubscription.get_object_name(),
                                             updates, f'"{EventSubscription.pk_column}" = "{subscription_name}"')
