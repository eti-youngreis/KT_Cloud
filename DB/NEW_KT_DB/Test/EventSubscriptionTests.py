from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Controller.EventSubscriptionController import EventSubscriptionController
from DB.NEW_KT_DB.Models.EventSubscriptionModel import SourceType, EventCategory
from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


def test_create_with_valid_input():
    '''Test the create method.'''
    # create a new EventSubscription
    event_subscription_controller = EventSubscriptionController(
        service=EventSubscriptionService(
            dal=EventSubscriptionManager(db_file='test_db.db'), storage_manager=StorageManager(base_directory='test_storage'), directory='test_directory'))

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
