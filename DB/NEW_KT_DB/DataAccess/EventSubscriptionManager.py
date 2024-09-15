import json
from typing import Any, Dict
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class EventSubscriptionManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.type_object = 'event_subscriptions'
        # self.create_table()

    def createInMemoryEventSubscription(self, event_subscription: EventSubscription):
        self.object_manager.save_in_memory(type_object=self.type_object, metadata=json.dumps(event_subscription.to_dict(
        )), object_id=event_subscription.subscription_name)

    def deleteInMemoryEventSubscription(self, subscription_name: str):
        self.object_manager.delete_from_memory(object_id=subscription_name)

    def describeEventSubscription(self, subscription_name: str, columns=['*']) -> EventSubscription:
        event_subscription_dict = self.object_manager.get_from_memory(
            type_object=self.type_object, columns=columns, object_id=subscription_name)
        event_subscription = EventSubscription(**event_subscription_dict)
        return event_subscription

    def modifyEventSubscription(self, subscription_name: str, updates: Dict[str, Any]):
        self.object_manager.update_in_memory(type_object=self.type_object,
                                             updates=updates, object_id=subscription_name)
