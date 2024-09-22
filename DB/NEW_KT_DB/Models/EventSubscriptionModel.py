from typing import List
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB import SourceType, EventCategory


class EventSubscription:
    def __init__(self, subscription_name: str, sources: List[(SourceType, str)], event_categories: List[EventCategory], sns_topic_arn: str, source_type: SourceType):
        self.subscription_name = subscription_name
        self.sources = {source_type.value: set() for source_type in SourceType}
        for source_type, source_id in sources:
            self.sources[source_type.value].add(source_id)
        self.event_categories = event_categories
        self.sns_topic_arn = sns_topic_arn
        self.source_type = source_type
        self.pk_column = 'subscription_name'
        self.pk_value = None

    def to_dict(self):
        '''Retrieve the data of the event subscription as a dictionary.'''
        return ObjectManager.convert_object_attributes_to_dictionary(
            subscription_name=self.subscription_name,
            sources=self.sources,
            event_categories=self.event_categories,
            sns_topic_arn=self.sns_topic_arn,
            source_type=self.source_type,
            pk_column=self.pk_column,
            pk_value=self.pk_value
        )
