from typing import List

from DB.DataAccess.DataAccessLayer import DataAccessLayer
from DB.Models.SourceType import SourceType
from KT_Cloud.DB.Models.EventCategory import EventCategory
from KT_Cloud.DB.Models.EventSubscriptionModel import EventSubscriptionProps


subscription_table_name = 'EventSubscription'
source_table_name = 'Sources'
sns_topics_arns_table_name = 'SNS_Topics_ARNs'


def validate_subscription_props(dal: DataAccessLayer, **kwargs):

    for key, value in kwargs.items():

        if key not in validations:
            raise ValueError('Invalid parameter', key)

        validations[key](dal, **value)


def validate_subscription_name(dal: DataAccessLayer, subscription_name: str, exists: bool):

    event_subscriptions = dal.select(subscription_table_name, '%')

    if subscription_name in event_subscriptions != exists:
        raise ValueError(f'Subscription name {
                         'already exists' if exists else 'doesn\'t exist'}')


def validate_sources(dal: DataAccessLayer, sources: List[(SourceType, str)]):

    source_ids = dal.select(source_table_name, '%')

    for source_type, source_id in sources:

        if source_id not in source_ids:
            raise ValueError('Source id doesn\'t exist', source_id)

        # TODO: Verify how to access 'type' in source_ids
        if source_ids[source_id]['type'] != source_type:
            raise ValueError(f"Source ID '{
                             source_id}' does not match the expected source type '{source_type}'")


def validate_event_categories(dal: DataAccessLayer, event_categories: List[EventCategory]):
    return True


def validate_sns_topic_arn(dal: DataAccessLayer, sns_topic_arn: str):

    sns_topic_arns = dal.select(sns_topics_arns_table_name, '%')

    if sns_topic_arn not in sns_topic_arns:
        raise ValueError(f"SNS topic ARN '{sns_topic_arn}' doesn't exist.")


def validate_source_type(source_type: SourceType):
    return True


validations = {
    EventSubscriptionProps.SUBSCRIPTION_NAME: validate_subscription_name,
    EventSubscriptionProps.SOURCES: validate_sources,
    EventSubscriptionProps.EVENT_CATEGORIES: validate_event_categories,
    EventSubscriptionProps.SNS_TOPIC_ARN: validate_sns_topic_arn,
    EventSubscriptionProps.SOURCE_TYPE: validate_source_type
}
