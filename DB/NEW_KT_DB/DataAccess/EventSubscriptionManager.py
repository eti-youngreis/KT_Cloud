import json
from typing import Any, Dict
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager


class EventSubscriptionManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.object_name = __class__.__name__.removesuffix('Manager')

    def createInMemoryEventSubscription(self, event_subscription: EventSubscription):
        self.object_manager.save_in_memory(object_name=self.object_name, metadata=json.dumps(event_subscription.to_dict(
        )), object_id=event_subscription.subscription_name)

    def deleteInMemoryEventSubscription(self, subscription_name: str):
        self.object_manager.delete_from_memory(
            object_name=self.object_name, object_id=subscription_name)

    def describeEventSubscription(self, subscription_name: str, columns=['*']) -> EventSubscription:
        event_subscription_dict = self.object_manager.get_from_memory(
            object_name=self.object_name, columns=columns, object_id=subscription_name)
        event_subscription = EventSubscription(**event_subscription_dict)
        # for key, value in updates.items():
        #     setattr(current_subscription, key, value)
        return event_subscription

    def modifyEventSubscription(self, subscription_name: str, updates: Dict[str, Any]):
        self.object_manager.update_in_memory(object_name=self.object_name,
                                             updates=updates, object_id=subscription_name)
