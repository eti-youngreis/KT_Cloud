from typing import List
from DB.Models.EventCategory import EventCategory
from Models.SourceType import SourceType
from Service.Classes.EventSubscriptionService import EventSubscriptionService


class EventSubscriptionController:
    def __init__(self, service: EventSubscriptionService) -> None:
        self.service = service

    def create_event_subscription(self, subscription_name: str, sources: List[(SourceType, str)],
                                  event_categories: List[EventCategory], sns_topic_arn: str, source_type: SourceType = SourceType.All):

        self.service.create(subscription_name=subscription_name, sources=sources,
                            event_categories=event_categories, sns_topic_arn=sns_topic_arn, source_type=source_type)

    def delete_event_subscription(self, subscription_name: str):

        self.service.delete(subscription_name=subscription_name)

    def describe_event_subscriptions(self, marker: str, max_records: int = 100, subscription_name: str = ''):

        self.service.describe(marker=marker, max_records=max_records,
                              subscription_name=subscription_name)

    def modify_event_subscription(self, subscription_name: str, event_categories: List[EventCategory], sns_topic_arn: str, source_type: SourceType = SourceType.ALL):

        self.service.modify(subscription_name=subscription_name,
                            event_categories=event_categories, sns_topic_arn=sns_topic_arn, source_type=source_type)
