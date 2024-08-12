from typing import List
from Models.SourceType import SourceType


class EventSubscriptionModel:

    def __init__(
        self,
        subscription_name,
        sources: List[SourceType, str],
        event_categories: List[str],
        sns_topic_arn: str,
        source_type: str
    ) -> None:
        self.subscription_name = subscription_name
        self.source_type = source_type
        self.sources = {source_type.value: {} for source_type in SourceType}

        for source_type, source_id in sources:
            self.sources[source_type].add(source_id)
        self.event_categories = event_categories
        self.sns_topic_arn = sns_topic_arn

    def to_dict(self):
        return {
            'subscription_name': self.subscription_name,
            'source_type': self.source_type,
            'sources': self.sources,
            'event_categories': self.event_categories,
            'sns_topic_arn': self.sns_topic_arn
        }
 