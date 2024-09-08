from enum import Enum
from typing import Dict, List
from DB.Models.EventCategory import EventCategory
from Models.SourceType import SourceType


class EventSubscriptionProps(Enum):
    SUBSCRIPTION_NAME = 'subscription_name'
    SOURCES = 'sources'
    EVENT_CATEGORIES = 'event_categories'
    SNS_TOPIC_ARN = 'sns_topic_arn'
    SOURCE_TYPE = 'source_type'


class EventSubscriptionModel:

    def __init__(
        self,
        subscription_name: str,
        sources: List[(SourceType, str)],
        event_categories: List[EventCategory],
        sns_topic_arn: str,
        source_type: SourceType
    ) -> None:
        self.subscription_name = subscription_name
        self.source_type = source_type
        self.sources = {source_type.value: set() for source_type in SourceType}

        for source_type, source_id in sources:
            self.sources[source_type.value].add(source_id)

        self.event_categories = event_categories
        self.sns_topic_arn = sns_topic_arn

    def to_dict(self):
        return {
            EventSubscriptionProps.SUBSCRIPTION_NAME.value: self.subscription_name,
            EventSubscriptionProps.SOURCE_TYPE.value: self.source_type,
            EventSubscriptionProps.SOURCES.value: self.sources,
            EventSubscriptionProps.EVENT_CATEGORIES.value: self.event_categories,
            EventSubscriptionProps.SNS_TOPIC_ARN.value: self.sns_topic_arn
        }
