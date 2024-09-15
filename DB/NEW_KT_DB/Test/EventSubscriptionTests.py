from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
from DB.NEW_KT_DB.Models.EventSubscriptionModel import SourceType, EventCategory

def test_create_with_valid_input():
    '''Test the create method.'''
    # create a new EventSubscription
    event_subscription_service = EventSubscriptionService(
        dal=EventSubscriptionManager('test_db.db'))
    event_subscription_service.create(
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
    event_subscription = event_subscription_service.get(
        subscription_name='test_subscription')
    
    assert event_subscription.subscription_name == 'test_subscription'
