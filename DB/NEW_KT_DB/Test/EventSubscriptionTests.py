<<<<<<< HEAD
import json
from unittest.mock import Mock
import pytest

from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Controller.EventSubscriptionController import EventSubscriptionController
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription, SourceType, EventCategory
from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
=======
import pytest

from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, EventSubscription, SourceType
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


@pytest.fixture
<<<<<<< HEAD
def mock_object_manager():
    return Mock(spec=ObjectManager)


@pytest.fixture
def event_subscription_manager():
    event_subscription_manager = EventSubscriptionManager('test_db.db')
    return event_subscription_manager
=======
def dal():
    return EventSubscriptionManager('test_db_file.db')
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e


@pytest.fixture
def storage_manager():
<<<<<<< HEAD
    return StorageManager('test_storage_directory')


@pytest.fixture
def event_subscription_service(storage_manager: StorageManager, event_subscription_manager):
    return EventSubscriptionService(
        storage_manager=storage_manager,
        dal=event_subscription_manager,
        directory='test_directory'
    )


@pytest.fixture
def event_subscription(event_subscription_service):
    event_subscription = EventSubscription('test_subscription', [(SourceType.DB_INSTANCE, 'test_db_instance'), (SourceType.DB_CLUSTER, 'test_db_cluster')], [
        EventCategory.CREATION, EventCategory.DELETION], 'test_sns_topic_arn', SourceType.DB_INSTANCE)
    return event_subscription


@pytest.fixture(autouse=True)
def cleanup_event_subscription(event_subscription_service: EventSubscriptionService):
    '''Fixture to ensure that the event_subscription is always deleted.'''
    yield
    event_subscription_service.delete('test_subscription')


def test_delete(event_subscription_service: EventSubscriptionService, storage_manager: StorageManager, event_subscription: EventSubscription):
    event_subscription_service.delete(event_subscription.subscription_name)
    with pytest.raises(ValueError):
        event_subscription_service.get(event_subscription.subscription_name)

    assert not storage_manager.is_file_exist(
        event_subscription_service.get_file_path(event_subscription.subscription_name))


def test_create_and_get(event_subscription_service: EventSubscriptionService, storage_manager: StorageManager):
    '''Test the create method.'''

    # create event subscription
    event_subscription_service.create('test_subscription', [(SourceType.DB_INSTANCE, 'test_db_instance'), (SourceType.DB_CLUSTER, 'test_db_cluster')], [
        EventCategory.CREATION, EventCategory.DELETION], 'test_sns_topic_arn', SourceType.DB_INSTANCE)

    # check that the EventSubscription was created
    event_subscription_res = event_subscription_service.get(
        'test_subscription')

    assert event_subscription_res.subscription_name == 'test_subscription'
    assert event_subscription_res.source_type == SourceType.DB_INSTANCE
    assert event_subscription_res.event_categories == [
        EventCategory.CREATION, EventCategory.DELETION]
    assert event_subscription_res.sns_topic_arn == 'test_sns_topic_arn'
    assert event_subscription_res.sources[SourceType.DB_INSTANCE.value] == {
        'test_db_instance'}
    assert event_subscription_res.sources[SourceType.DB_CLUSTER.value] == {
        'test_db_cluster'}

    assert storage_manager.is_file_exist(
        event_subscription_service.get_file_path('test_subscription'))
=======
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
        'test_sns_topic',
        SourceType.DB_INSTANCE
    )
    event_subscription_service.create(
        event_subscription.subscription_name,
        sources,  # Pass sources directly, not event_subscription.sources
        event_subscription.event_categories,
        event_subscription.sns_topic,
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

    with pytest.raises(Exception):
        event_subscription_service.get_by_id(
            event_subscription.subscription_name)

    assert not event_subscription_service.storage_manager.is_file_exist(
        event_subscription_service.get_file_path(event_subscription.subscription_name))


def test_modify(event_subscription_service: EventSubscriptionService, event_subscription: EventSubscription):

    updated_event_subscription = EventSubscription('test_subscription', [(SourceType.DB_INSTANCE, 'test_instance'), (
        SourceType.DB_CLUSTER, 'db_cluster')], [EventCategory.BACKUP, EventCategory.DELETION, EventCategory.CREATION], 'test_sns_topic', SourceType.DB_INSTANCE)
    event_subscription_service.modify(
        event_subscription.subscription_name, event_categories=updated_event_subscription.event_categories)

    # Assuming that __eq__ is implemented for EventSubscription
    assert updated_event_subscription == event_subscription_service.get_by_id(
        event_subscription.subscription_name)

    updated_event_subscription.sns_topic = 'new_sns_topic'
    event_subscription_service.modify(
        event_subscription.subscription_name, sns_topic=updated_event_subscription.sns_topic)
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
>>>>>>> fe49bffeff811509c9dbc52c0399d1d6a288665e
