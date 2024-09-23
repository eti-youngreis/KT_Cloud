from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, SourceType, EventSubscription
from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Controller.EventSubscriptionController import EventSubscriptionController
from datetime import datetime


def create_event_subscription_controller() -> EventSubscriptionController:
    db_file = 'DB/NEW_KT_DB/DBs/mainDB.db'
    storage_directory = 'Storage/KT_Storage/DataAccess/example_storage'
    event_subscription_directory = 'example_event_subscription'

    event_subscription_manager = EventSubscriptionManager(db_file)
    storage_manager = StorageManager(storage_directory)
    event_subscription_service = EventSubscriptionService(
        event_subscription_manager, storage_manager, event_subscription_directory)
    event_subscription_controller = EventSubscriptionController(
        event_subscription_service)

    return event_subscription_controller


current_date_time = datetime.now()

# Initialize the event_subscription_controller
event_subscription_controller = create_event_subscription_controller()

print('''---------------------Start Of Session----------------------''')

print(f'''{current_date_time}
      demonstration of Event Subscription start''')

# Create
print(f'''{current_date_time}
          going to create event subscription named "example_subscription"''')
event_subscription_controller.create_event_subscription(
    subscription_name="example_subscription",
    sources=[(SourceType.DB_INSTANCE, "example_instance"),
             (SourceType.DB_CLUSTER, "example_cluster")],
    event_categories=[EventCategory.CREATION, EventCategory.DELETION],
    sns_topic="example-topic",
    source_type=SourceType.DB_INSTANCE
)
print('verifying event subscription "example_subscription" creation''')
try:
    event_subscription_controller.get_event_subscription(
        subscription_name="example_subscription")
    print('event subscription "example_subscription" created successfully')
except:
    print('event subscription "example_subscription" creation failed')

# Describe
print(f'''{current_date_time}
          describing event subscription "example_subscription"''')
print(event_subscription_controller.describe_event_subscriptions(
    criteria={"subscription_name": "example_subscription"}))

# Modify
print(f'''{current_date_time}
      modifying event subscription "example_subscription"''')
event_subscription_controller.modify_event_subscription(
    subscription_name="example_subscription",
    event_categories=[EventCategory.CREATION,
                      EventCategory.DELETION, EventCategory.FAILURE],
    sns_topic="example-topic-2",
    source_type=SourceType.DB_INSTANCE
)
print('verifying event subscription "example_subscription" modification')
print(event_subscription_controller.describe_event_subscription(
    subscription_name="example_subscription"))
print('event subscription "example_subscription" modified successfully')

# Delete
print(f'''{current_date_time}
          going to delete event subscription "example_subscription"''')
event_subscription_controller.delete_event_subscription(
    "example_subscription")
print('verify event subscription "example_subscription" deletion')
try:
    event_subscription_controller.get_event_subscription(
        subscription_name="example_subscription")
    print('event subscription "example_subscription" deletion failed')
except:
    print('event subscription "example_subscription" deleted successfully')


print(f'''{current_date_time}
      deonstration of object EventSubscription ended successfully''')
print(current_date_time)
print('''---------------------End Of session----------------------''')
