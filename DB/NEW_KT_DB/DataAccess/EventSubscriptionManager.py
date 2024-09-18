from typing import Dict, Any, List, Optional
import json
import sqlite3
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, EventSubscription, SourceType


class EventSubscriptionManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(
            EventSubscription.get_object_name(), EventSubscription.table_schema, 'TEXT')

    def createInMemoryEventSubscription(self, event_subscription: EventSubscription) -> None:
        self.object_manager.save_in_memory(
            event_subscription.get_object_name(), event_subscription.to_sql())

    def deleteInMemoryEventSubscription(self, subscription_name: str) -> None:
        self.object_manager.delete_from_memory_by_pk(
            EventSubscription.get_object_name(), EventSubscription.pk_column, subscription_name)

    def describeEventSubscriptionById(self, subscription_name: str) -> EventSubscription:
        event_subscription = self.describeEventSubscriptionByCriteria(
            criteria=f'{EventSubscription.pk_column} = "{subscription_name}"')

        return event_subscription[0] if len(event_subscription) > 0 else None

    def modifyEventSubscription(self, event_subscription: EventSubscription) -> None:
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
        
    def describeEventSubscriptionByCriteria(self, columns: Optional[List[str]] = '*', criteria: Optional[str] = None) -> List[EventSubscription]:
        event_subscription_data = self.object_manager.get_from_memory(
            EventSubscription.get_object_name(), columns, criteria)

        event_subscriptions = []
        
        for data in event_subscription_data:
            subscription_name, source_type, sources, event_categories, sns_topic_arn = data
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
                sns_topic_arn=sns_topic_arn,
                source_type=SourceType(source_type)
            )
            event_subscriptions.append(event_subscription)

        return event_subscriptions

    def get(self, criteria: str = None) -> List[EventSubscription]:
        return self.describeEventSubscriptionByCriteria(criteria=criteria)

    def get_by_id(self, subscription_name: str):
        return self.describeEventSubscriptionById(subscription_name)
