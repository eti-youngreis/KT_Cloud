from typing import List
from DB.Validation.EventSubscriptionValidation import validate_subscription_props, subscription_table_name
from DataAccess.DataAccessLayer import DataAccessLayer
from Models.EventSubscriptionModel import EventSubscriptionModel
from Models.SourceType import SourceType
from Service.Abc.DBO import DBO


class EventSubscriptionService(DBO):

    def __init__(self, dal: DataAccessLayer):
        self.dal = dal

    def create(self, subscription_name: str, sources: List[SourceType, str], event_categories: List[str],
               sns_topic_arn: str, source_type: SourceType):

        validate_subscription_props(self.dal, subscription_name=subscription_name, sources=sources,
                                    event_categories=event_categories, sns_topic_arn=sns_topic_arn, source_type=source_type)

        event_subscription = EventSubscriptionModel(
            subscription_name, sources, event_categories, sns_topic_arn, source_type)

        self.dal.insert(subscription_table_name, event_subscription.to_dict())

    def delete(self, subscription_name: str):

        validate_subscription_props(
            self.dal, subscription_name=subscription_name)

        self.dal.delete(subscription_table_name, subscription_name)

    def describe(self, marker: str, max_records: int, subscription_name: str = None):

        validate_subscription_props(
            self.dal, subscription_name=subscription_name)

        self.dal.select(subscription_table_name, subscription_name or '%')

    def modify(self, subscription_name: str, **kwargs):

        if 'sources' in kwargs:
            raise ValueError('Modifying sources is not allowed in the modify function')
        
        validate_subscription_props(
            self.dal, subscription_name=subscription_name)

        event_subscription = self.dal.select(
            subscription_table_name, subscription_name)

        updated_subscription = {**event_subscription, **kwargs}

        validate_subscription_props(self.dal, updated_subscription)
