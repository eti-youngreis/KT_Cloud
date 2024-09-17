from unittest.mock import Mock
import pytest

from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Controller.EventSubscriptionController import EventSubscriptionController
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventSubscription, SourceType, EventCategory
from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
from DB.NEW_KT_DB.Test.GeneralTests import test_file_exists
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


@pytest.fixture
def mock_object_manager():
    return Mock(spec=ObjectManager)


@pytest.fixture
def event_subscription_manager():
    event_subscription_manager = EventSubscriptionManager('test_db.db')
    return event_subscription_manager


@pytest.fixture
def event_subscription_service(event_subscription_manager):
    return EventSubscriptionService(
        dal=event_subscription_manager,
        storage_manager=StorageManager('test_storage'),
        directory='test_directory'
    )


@pytest.fixture
def event_subscription(event_subscription_service):
    event_subscription = EventSubscription('test_subscription', [(SourceType.DB_INSTANCE, 'test_db_instance'), (SourceType.DB_CLUSTER, 'test_db_cluster')], [
                                           EventCategory.CREATION, EventCategory.DELETION], 'test_sns_topic_arn', SourceType.DB_INSTANCE)
    return event_subscription


def test_create_with_valid_input(event_subscription:EventSubscription, event_subscription_service: EventSubscriptionService):
    '''Test the create method.'''

    # create event subscription
    event_subscription_service.create(event_subscription)

    # check that the EventSubscription was created
    event_subscription_res = event_subscription_service.get('test_subscription')

    assert event_subscription_res == event_subscription

    assert test_file_exists(event_subscription_service.get_file_path(event_subscription.subscription_name))
