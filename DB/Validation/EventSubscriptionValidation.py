from typing import List
from DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.Models.SourceType import SourceType
from DB.Models.EventCategory import EventCategory
from DB.Models.EventSubscriptionModel import EventSubscriptionProps


subscription_table_name = 'EventSubscription'
source_table_name = 'Sources'
sns_topics_arns_table_name = 'SNS_Topics_ARNs'


def validate_subscription_props(dal: EventSubscriptionManager, **kwargs):

    for key, value in kwargs.items():

        if key not in validations:
            raise ValueError('Invalid parameter', key)

        validations[key](dal, **value)


def validate_subscription_name(dal: EventSubscriptionManager, subscription_name: str, exists: bool):

    event_subscriptions = dal.get('%')

    if subscription_name in event_subscriptions != exists:
        raise ValueError(f'Subscription name {
                         'already exists' if exists else 'doesn\'t exist'}')


def validate_sources(dal: EventSubscriptionManager, sources: List[(SourceType, str)]):

    source_ids = dal.get_sources(source_table_name, '%')

    for source_type, source_id in sources:

        if source_id not in source_ids:
            raise ValueError('Source id doesn\'t exist', source_id)

        # TODO: Verify how to access 'type' in source_ids
        if source_ids[source_id]['type'] != source_type:
            raise ValueError(f"Source ID '{
                             source_id}' does not match the expected source type '{source_type}'")


def validate_event_categories(dal: EventSubscriptionManager, event_categories: List[EventCategory]):
    return True


def validate_sns_topic_arn(dal: EventSubscriptionManager, sns_topic_arn: str):

    sns_topic_arns = dal.get(sns_topics_arns_table_name,'%')

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
