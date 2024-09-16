import pytest

from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Controller.EventSubscriptionController import EventSubscriptionController
from DB.NEW_KT_DB.Models.EventSubscriptionModel import SourceType, EventCategory
from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


@pytest.fixture
def mock_event_subscription_manager(mocker):
    return mocker.Mock()


@pytest.fixture
def event_subscription_service(mocker_event_subscription_manager):
    return EventSubscriptionService(
        dal=mock_event_subscription_manager,
        storage_manager=StorageManager('test_storage'),
        directory='test_directory'
    )


def test_create_with_valid_input(event_subscription_service, mock_event_subscription_manager):
    '''Test the create method.'''

    # Set up the mock to return a predefined response for the create method

    event_subscription_controller = EventSubscriptionController(
        event_subscription_service
    )

    event_subscription_controller.create_event_subscription(
        subscription_name='test_subscription',
        sources=[
            (SourceType.DB_INSTANCE, 'test_db_instance'),
            (SourceType.DB_CLUSTER, 'test_db_cluster')
        ],
        event_categories=[EventCategory.CREATION, EventCategory.DELETION],
        sns_topic_arn='test_sns_topic_arn',
        source_type=SourceType.DB_INSTANCE
    )
    
    # check that the EventSubscription was created
    event_subscription = event_subscription_controller.get(
        subscription_name='test_subscription')

    assert event_subscription.subscription_name == 'test_subscription'
