import sys
import os
from datetime import datetime

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the tag management class
from Controller.TagObjectController import TagObjectController


def log_message(message):
    """Log a message with the current timestamp and return the current time."""
    current_time = datetime.now()
    print(f"{current_time.strftime('%H:%M:%S.%f')[:-3]} {message}")
    return current_time


def calculate_duration_time(start, end):
    """Calculate the duration between two timestamps."""
    return end - start


def main():
    tag_controller = TagObjectController()

    # Log the start of the session
    log_message("--------------------- Start Of Session ----------------------")
    log_message("Demonstration of object tag start")

    # Create a new tag
    log_message('Going to create tag with key "example_key"')

    start = log_message("Creating tag 'example_key'")
    tag_controller.create_tag("example_key", "example_value")

    # Verify the tag has been created
    log_message('Verifying tag "example_key" creation')
    try:
        log_message(f'The tag object: {tag_controller.get_tag("example_key")}')
    except KeyError as e:
        log_message(f"Error: {e}")

    end = log_message('Tag "example_key" created successfully')
    print(f"Duration time: {calculate_duration_time(start, end)}")

    # Modify the tag
    start = log_message('Going to modify tag with key "example_key" to "modify_key"')
    tag_controller.modify_tag("example_key", "modify_key", "example_value")

    # Verify that the old tag no longer exists and the new tag exists
    log_message('Verifying that tag "example_key" no longer exists and "modify_key" exists')
    try:
        log_message(f'The tag object: {tag_controller.get_tag("example_key")}')
    except KeyError as e:
        log_message(f"Error: {e}")

    log_message('Tag "modify_key" created successfully')
    end = log_message(f'The tag object: {tag_controller.get_tag("modify_key")}')
    print(f"Duration time: {calculate_duration_time(start, end)}")

    # Delete the tag
    start = log_message('Going to delete tag "modify_key"')
    tag_controller.delete_tag("modify_key")

    # Verify that the tag has been deleted
    log_message('Verifying tag "modify_key" deletion')
    try:
        tag_controller.get_tag("modify_key")
    except KeyError as e:
        log_message(f"Error: {e}")

    end = log_message("After deletion verification")
    print(f"Duration time: {calculate_duration_time(start, end)}")

    # Create additional tags
    log_message("Going to create 5 additional tags")
    for i in range(1, 6):
        tag_controller.create_tag(f"example_key{i}", f"example_value{i}")

    # Describe the created tags
    start = log_message("Describing tags")
    for tag in tag_controller.describe_tags():
        log_message(tag)
    end = log_message("Finished describing tags")
    print(f"Duration time: {calculate_duration_time(start, end)}")

    # Log the end of the session
    log_message("Demonstration of object Tag ended successfully")
    log_message("--------------------- End Of Session ----------------------")

