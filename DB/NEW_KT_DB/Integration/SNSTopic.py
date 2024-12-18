from SNS.Controller.SNSController import SNSTopicController
from SNS.Model.SNSModel import Protocol
from SNS.Service.SNSService import SNSTopicService

from SNS.DataAccess.SNSManager import SNSTopicManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

from datetime import datetime
from colorama import init, Fore, Back, Style


init(autoreset=True)


def print_colored(text, color=Fore.WHITE, bg_color=Back.BLACK, style=Style.NORMAL):
    print(f"{style}{bg_color}{color}{text}{Style.RESET_ALL}")


def create_sns_topic_controller():
    db_file = 'DB/NEW_KT_DB/DBs/mainDB.db'
    storage_directory = 'Storage/KT_Storage/DataAccess/example_storage'
    sns_directory = 'example_sns_directory'

    sns_topic_manager = SNSTopicManager(db_file)
    storage_manager = StorageManager(storage_directory)
    sns_service = SNSTopicService(
        sns_topic_manager, storage_manager, sns_directory)
    sns_topic_controller = SNSTopicController(sns_service)
    return sns_topic_controller


current_date_time = datetime.now()
sns_topic_controller = create_sns_topic_controller()

print_colored(
    '''---------------------Start Of Session----------------------''', Fore.GREEN)
print_colored(
    f"{current_date_time} SNS (Simple Notification Service) Functionality Demonstration", Fore.CYAN)

print_colored('''
Introduction to SNS (Simple Notification Service):
SNS is a service that sends messages to different subscribers using topics. 
It helps applications send notifications efficiently. 

Key features of SNS:
1. Create and manage topics
2. Add or remove subscribers (currently limited to email addresses)
3. Send messages to all subscribers of a topic simultaneously
4. Manage subscriptions

This demonstration will show how to create a topic, add a subscriber, 
send a notification, unsubscribe from the topic and delete the topic.
''', Fore.YELLOW)

print_colored('''
Summary of operations to be performed:
1. Create a new SNS topic
2. Subscribe to the topic
3. Publish a notification to the topic
4. Unsubscribe from the topic
5. Delete the topic
6. Attempt to retrieve the deleted topic (error handling)
''', Fore.YELLOW)

# Create Topic
print_colored(
    f"\n{current_date_time} Creating SNS topic 'example_topic'", Fore.MAGENTA)
sns_topic_controller.create_topic(topic_name="example_topic")
print_colored("SNS topic 'example_topic' created successfully", Fore.GREEN)

# Subscribe to Topic
print_colored(
    f"\n{current_date_time} Subscribing to SNS topic 'example_topic' with protocol EMAIL", Fore.MAGENTA)
sns_topic_controller.subscribe(
    topic_name="example_topic",
    protocol=Protocol.EMAIL,
    notification_endpoint="example@example.com"
)
sns_topic_controller.subscribe(
    topic_name="example_topic",
    protocol=Protocol.EMAIL,
    notification_endpoint="example2@example.com"
)
print_colored(
    " 'example@example.com', 'example2@example.com' Subscribed to SNS topic 'example_topic' successfully", Fore.GREEN)

# Notify
print_colored(
    f"\n{current_date_time} Publishing notification to SNS topic 'example_topic'", Fore.MAGENTA)
sns_topic_controller.notify(
    topic_name="example_topic",
    message="This is a test notification from our SNS demonstration"
)
print_colored(
    "Notification sent to SNS topic 'example_topic' successfully", Fore.GREEN)

# Unsubscribe from Topic
print_colored(
    f"\n{current_date_time} Unsubscribing from SNS topic 'example_topic'", Fore.MAGENTA)
sns_topic_controller.unsubscribe(
    topic_name="example_topic",
    protocol=Protocol.EMAIL,
    notification_endpoint="example@example.com"
)
print_colored(
    "Unsubscribed from SNS topic 'example_topic' successfully", Fore.GREEN)

# Delete Topic
print_colored(
    f"\n{current_date_time} Deleting SNS topic 'example_topic'", Fore.MAGENTA)
sns_topic_controller.delete_topic(topic_name="example_topic")

# Attempt to retrieve deleted topic (error handling)
print_colored(
    f"\n{current_date_time} Attempting to retrieve deleted topic 'example_topic'", Fore.MAGENTA)
try:
    sns_topic_controller.get_topic(topic_name="example_topic")
    print_colored(
        "Unexpected: Topic still exists after deletion attempt", Fore.RED)
except Exception as e:
    print_colored("SNS topic 'example_topic' deleted successfully", Fore.GREEN)
    print_colored(
        "Attempted retrieval failed as expected. The topic no longer exists in the system.", Fore.YELLOW)

print_colored(
    f"\n{current_date_time} SNS demonstration completed successfully", Fore.GREEN)
print_colored(
    '''---------------------End Of Session----------------------''', Fore.GREEN)
