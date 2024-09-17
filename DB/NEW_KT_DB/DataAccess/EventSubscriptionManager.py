import json
from typing import Any, Dict
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class EventSubscriptionManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(
            EventSubscription.get_object_name(), EventSubscription.table_schema)

    def createInMemoryEventSubscription(self, event_subscription: EventSubscription):
        self.object_manager.save_in_memory(event_subscription.get_object_name(), event_subscription.to_sql(
        ))

    def deleteInMemoryEventSubscription(self, subscription_name: str):
        self.object_manager.delete_from_memory_by_pk(
            EventSubscription.get_object_name(), EventSubscription.pk_column, subscription_name)

    def describeEventSubscription(self, subscription_name: str, columns=['*']) -> EventSubscription:
        event_subscription_dict = self.object_manager.get_from_memory(
            EventSubscription.get_object_name(), columns, f'"{EventSubscription.pk_column} = "{subscription_name}"')
        event_subscription = EventSubscription(**event_subscription_dict)
        # for key, value in updates.items():
        #     setattr(current_subscription, key, value)
        return event_subscription

    def modifyEventSubscription(self, subscription_name: str, updates: Dict[str, Any]):
        self.object_manager.update_in_memory(object_name=self.table_name,
                                             updates=updates, object_id=subscription_name)
