import pytest

from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, EventSubscription, SourceType
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


@pytest.fixture
def dal():
    return EventSubscriptionManager('test_db_file.db')


@pytest.fixture
def storage_manager():
    dir = 'test_storage_directory'
    manager = StorageManager(dir)
    yield manager
    manager.delete_directory(dir)


@pytest.fixture
def event_subscription_service(dal: EventSubscriptionManager, storage_manager: StorageManager):
    dir = 'test_directory'
    service = EventSubscriptionService(dal, storage_manager, dir)
    yield service
    storage_manager.delete_directory(dir)


@pytest.fixture
def event_subscription(event_subscription_service: EventSubscriptionService):
    sources = [
        (SourceType.DB_INSTANCE, 'test_instance'),
        (SourceType.DB_CLUSTER, 'db_cluster')
    ]
    event_subscription = EventSubscription(
        'test_subscription',
        sources,
        [EventCategory.CREATION, EventCategory.DELETION],
        'test_sns_topic_arn',
        SourceType.DB_INSTANCE
    )
    event_subscription_service.create(
        event_subscription.subscription_name,
        sources,  # Pass sources directly, not event_subscription.sources
        event_subscription.event_categories,
        event_subscription.sns_topic_arn,
        event_subscription.source_type
    )
    yield event_subscription
    event_subscription_service.delete(event_subscription.subscription_name)


def test_create(event_subscription_service: EventSubscriptionService, event_subscription: EventSubscription):
    # Assuming that __eq__ is implemented for EventSubscription
    assert event_subscription == event_subscription_service.get_by_id(
        event_subscription.subscription_name)

    assert event_subscription_service.storage_manager.is_file_exist(
        event_subscription_service.get_file_path(event_subscription.subscription_name))


def test_delete(event_subscription_service: EventSubscriptionService, event_subscription: EventSubscription):
    event_subscription_service.delete(event_subscription.subscription_name)

    assert not event_subscription_service.get_by_id(
        event_subscription.subscription_name)

    assert not event_subscription_service.storage_manager.is_file_exist(
        event_subscription_service.get_file_path(event_subscription.subscription_name))


def test_modify(event_subscription_service: EventSubscriptionService, event_subscription: EventSubscription):

    updated_event_subscription = EventSubscription('test_subscription', [(SourceType.DB_INSTANCE, 'test_instance'), (
        SourceType.DB_CLUSTER, 'db_cluster')], [EventCategory.BACKUP, EventCategory.DELETION, EventCategory.CREATION], 'test_sns_topic_arn', SourceType.DB_INSTANCE)
    event_subscription_service.modify(
        event_subscription.subscription_name, event_categories=updated_event_subscription.event_categories)

    # Assuming that __eq__ is implemented for EventSubscription
    assert updated_event_subscription == event_subscription_service.get_by_id(
        event_subscription.subscription_name)

    updated_event_subscription.sns_topic_arn = 'new_sns_topic_arn'
    event_subscription_service.modify(
        event_subscription.subscription_name, sns_topic_arn=updated_event_subscription.sns_topic_arn)
    # Assuming that __eq__ is implemented for EventSubscription
    assert updated_event_subscription == event_subscription_service.get_by_id(
        event_subscription.subscription_name)

    updated_event_subscription.source_type = SourceType.DB_CLUSTER
    event_subscription_service.modify(
        event_subscription.subscription_name, source_type=updated_event_subscription.source_type)

    # Assuming that __eq__ is implemented for EventSubscription
    assert updated_event_subscription == event_subscription_service.get_by_id(
        event_subscription.subscription_name)


def test_describe_by_id(event_subscription_service: EventSubscriptionService, event_subscription: EventSubscription):
    assert event_subscription.to_dict() == event_subscription_service.describe_by_id(
        event_subscription.subscription_name)


def test_describe(event_subscription_service: EventSubscriptionService, event_subscription: EventSubscription):
    assert [event_subscription.to_dict()] == event_subscription_service.describe(
        criteria={'subscription_name': event_subscription.subscription_name})


def test_get(event_subscription_service: EventSubscriptionService, event_subscription: EventSubscription):
    # Assuming that __eq__ is implemented for EventSubscription
    assert event_subscription == event_subscription_service.get(
        {'subscription_name': event_subscription.subscription_name})[0]


def test_get_by_id(event_subscription_service: EventSubscriptionService, event_subscription: EventSubscription):
    # Assuming that __eq__ is implemented for EventSubscription
    assert event_subscription == event_subscription_service.get_by_id(
        event_subscription.subscription_name)
