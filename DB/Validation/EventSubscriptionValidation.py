from typing import Dict, List

from DB.DataAccess.DataAccessLayer import DataAccessLayer
from DB.Models.SourceType import SourceType


subscription_table_name = 'EventSubscription'
source_table_name = 'Sources'


def validate_subscription_props(dal: DataAccessLayer, **kwargs):

    for key, value in kwargs:

        if key not in validations:
            raise ValueError('Invalid parameter', key)

        validations[key](dal, **value)


def validate_subscription_name(dal: DataAccessLayer, subscription_name: str, exists: bool):

    event_subscriptions = dal.select(subscription_table_name, '%')

    if subscription_name in event_subscriptions or not exists:
        raise ValueError(f'Subscription name {
                         'already exists' if exists else 'doesn\'t exist'}')


def validate_sources(dal: DataAccessLayer, sources: Dict[SourceType, str]):
    return True


def validate_event_categories(dal: DataAccessLayer, event_categories: List[str]):
    return True


def validate_sns_topic_arn(dal: DataAccessLayer, sns_topic_arn: str):
    return True


def validate_source_type(source_type: SourceType):
    return True


validations = {
    'subscription_name': validate_subscription_name,
    'sources': validate_sources,
    'event_categories': validate_event_categories,
    'sns_topic_arn': validate_sns_topic_arn,
    'source_type': validate_source_type
}
