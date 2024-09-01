from typing import List
from DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.Validation.EventSubscriptionValidation import validate_subscription_props, subscription_table_name
from DB.Models.EventCategory import EventCategory
from Models.EventSubscriptionModel import EventSubscriptionModel, EventSubscriptionProps
from Models.SourceType import SourceType
from Service.Abc.DBO import DBO


class EventSubscriptionService(DBO):

    def __init__(self, dal: EventSubscriptionManager):
        self.dal = dal

    def create(self, subscription_name: str, sources: List[(SourceType, str)], event_categories: List[EventCategory],
               sns_topic_arn: str, source_type: SourceType):

        validate_subscription_props(self.dal, subscription_name=subscription_name, sources=sources,
                                    event_categories=event_categories, sns_topic_arn=sns_topic_arn, source_type=source_type)

        event_subscription = EventSubscriptionModel(
            subscription_name, sources, event_categories, sns_topic_arn, source_type)

        self.dal.create(**event_subscription.to_dict())

    def delete(self, subscription_name: str):

        validate_subscription_props(
            self.dal, subscription_name=subscription_name)

        self.dal.delete(subscription_name)

    def describe(self, marker: str, max_records: int, subscription_name: str = None):

        # TODO: Implement functionality for 'marker' and 'max_records' once they are available

        validate_subscription_props(
            self.dal, subscription_name=subscription_name)

        self.dal.get(subscription_name or '%')

    def modify(self, subscription_name: str, sns_topic_arn: str, event_categories: List[EventCategory] = [], source_type: SourceType = SourceType.ALL):

        validate_subscription_props(
            self.dal, subscription_name=subscription_name, sns_topic_arn=sns_topic_arn,
            event_categories=event_categories, source_type=source_type)

        event_subscription = self.dal.get(subscription_name)

        updated_subscription = {**event_subscription, EventSubscriptionProps.SNS_TOPIC_ARN: sns_topic_arn,
                                EventSubscriptionProps.EVENT_CATEGORIES: event_categories, EventSubscriptionProps.SOURCE_TYPE: source_type}

        self.dal.update(**updated_subscription)
