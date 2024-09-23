from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from DB.NEW_KT_DB.Service.Classes.EventSubscriptionService import EventSubscriptionService
from DB.NEW_KT_DB.Models.EventSubscriptionModel import EventCategory, SourceType
from DB.NEW_KT_DB.DataAccess.EventSubscriptionManager import EventSubscriptionManager
from DB.NEW_KT_DB.Controller.EventSubscriptionController import EventSubscriptionController
from datetime import datetime
from colorama import init, Fore, Back, Style

init(autoreset=True)


def print_colored(text, color=Fore.WHITE, bg_color=Back.BLACK, style=Style.NORMAL):
    print(f"{style}{bg_color}{color}{text}{Style.RESET_ALL}")


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
event_subscription_controller = create_event_subscription_controller()

print_colored(
    '''---------------------Start Of Session----------------------''', Fore.GREEN)
print_colored(
    f"{current_date_time} Event Subscription Functionality Demonstration", Fore.CYAN)

print_colored('''
Introduction to Event Subscriptions:
Event Subscriptions are a crucial component in our system that allow users to monitor and respond to specific events 
occurring within the database environment. They provide a way to receive notifications about important changes or 
incidents, enabling proactive management and timely responses to critical situations.

Key features of Event Subscriptions:
1. Customizable event categories (e.g., creation, deletion, failure)
2. Multiple source types (e.g., database instances, clusters)
3. Integration with SNS (Simple Notification Service) for efficient notification delivery
4. Flexible management options (create, modify, delete, describe)

In this demonstration, we'll walk through the entire lifecycle of an Event Subscription, showcasing its functionality 
and how it can be used to enhance database monitoring and management.
''', Fore.YELLOW)

print_colored('''
Summary of operations to be performed:
1. Create a new event subscription
2. Retrieve and describe the created subscription
3. Modify the existing subscription
4. Delete the subscription
5. Attempt to retrieve the deleted subscription (error handling)
''', Fore.YELLOW)
# Create
print_colored(
    f"\n{current_date_time} Creating event subscription 'example_subscription'", Fore.MAGENTA)
event_subscription_controller.create_event_subscription(
    subscription_name="example_subscription",
    sources=[(SourceType.DB_INSTANCE, "example_instance"),
             (SourceType.DB_CLUSTER, "example_cluster")],
    event_categories=[EventCategory.CREATION, EventCategory.DELETION],
    sns_topic="example-topic",
    source_type=SourceType.DB_INSTANCE
)
print_colored(
    "Event subscription 'example_subscription' created successfully", Fore.GREEN)

# Describe
print_colored(
    f"\n{current_date_time} Describing event subscription 'example_subscription'", Fore.MAGENTA)
description = event_subscription_controller.describe_event_subscription(
    "example_subscription")
print_colored(f"Description: {description}", Fore.CYAN)

# Modify
print_colored(
    f"\n{current_date_time} Modifying event subscription 'example_subscription'", Fore.MAGENTA)
event_subscription_controller.modify_event_subscription(
    subscription_name="example_subscription",
    event_categories=[EventCategory.CREATION,
                      EventCategory.DELETION, EventCategory.FAILURE],
    sns_topic="example-topic-2",
    source_type=SourceType.DB_INSTANCE
)
print_colored(
    "Event subscription 'example_subscription' modified successfully", Fore.GREEN)

# Describe after modification
print_colored(
    f"\n{current_date_time} Describing modified event subscription 'example_subscription'", Fore.MAGENTA)
modified_description = event_subscription_controller.describe_event_subscription(
    "example_subscription")
print_colored(f"Modified Description: {modified_description}", Fore.CYAN)

# Delete
print_colored(
    f"\n{current_date_time} Deleting event subscription 'example_subscription'", Fore.MAGENTA)
event_subscription_controller.delete_event_subscription("example_subscription")

# Attempt to retrieve deleted subscription (error handling)
print_colored(
    f"\n{current_date_time} Attempting to retrieve deleted subscription 'example_subscription'", Fore.MAGENTA)
try:
    event_subscription_controller.get_event_subscription(
        "example_subscription")
    print_colored(
        "Unexpected: Subscription still exists after deletion attempt", Fore.RED)
except Exception as e:
    print_colored("Event subscription 'example_subscription' deleted successfully", Fore.GREEN)
    print_colored("Attempted retrieval failed as expected. The subscription no longer exists in the system.", Fore.YELLOW)

print_colored(
    f"\n{current_date_time} Event Subscription demonstration completed successfully", Fore.GREEN)
print_colored(
    '''---------------------End Of Session----------------------''', Fore.GREEN)
