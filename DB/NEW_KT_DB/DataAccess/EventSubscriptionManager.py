import json
from typing import Any, Dict, List, Optional
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, SourceType


class EventSubscriptionManager:
    def __init__(self, db_file: str):
        '''Initialize ObjectManager with the database connection.'''
        self.object_manager = ObjectManager(db_file)
        self.object_manager.create_management_table(
            EventSubscription.get_object_name(), EventSubscription.table_schema, 'TEXT')

    def createInMemoryEventSubscription(self, event_subscription: EventSubscription) -> None:
        self.object_manager.save_in_memory(event_subscription.get_object_name(), event_subscription.to_sql(
        ))

    def deleteInMemoryEventSubscription(self, subscription_name: str) -> None:
        self.object_manager.delete_from_memory_by_pk(
            EventSubscription.get_object_name(), EventSubscription.pk_column, subscription_name)

    def describeEventSubscriptionByCriteria(self, columns: Optional[List[str]] = None, criteria: Optional[str] = None) -> List[EventSubscription]:
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

    def describeEventSubscriptionById(self, subscription_name: str, columns=None) -> EventSubscription:
        return self.describeEventSubscriptionByCriteria(columns='*', criteria=f'"{EventSubscription.pk_column}" = "{subscription_name}"')[0]

    def modifyEventSubscription(self, subscription_name: str, updates: Dict[str, Any]) -> None:
        self.object_manager.update_in_memory(EventSubscription.get_object_name(),
                                             updates, f'"{EventSubscription.pk_column}" = "{subscription_name}"')
