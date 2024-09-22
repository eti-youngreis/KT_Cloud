import sys
import os
from datetime import datetime

# Add the parent directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the tag management class
from Controller.TagObjectController import TagObjectController

def log_message(message):
    """Log a message with the current timestamp."""
    print(f"{datetime.now().strftime('%H:%M:%S.%f')[:-3]} {message}")

def main():
    # Create an instance of the tag management class
    tag_controller = TagObjectController()

    # Log the start of the session
    log_message('--------------------- Start Of Session ----------------------')
    log_message('Demonstration of object tag start')

    # Create a new tag
    log_message('Going to create tag with key "example_key"')
    tag_controller.create_tag('example_key', "example_value")

    # Verify that the tag has been created
    log_message('Verifying tag "example_key" created by checking if it exists')
    try:
        log_message(f'The tag object: {tag_controller.get_tag("example_key")}')
    except KeyError as e:
        log_message(f'Error: {e}')

    log_message('Tag "example_key" created successfully')

    # Modify the tag
    log_message('Going to modify tag with key "example_key" to "modify_key"')
    tag_controller.modify_tag('example_key', 'modify_key', "example_value")

    # Verify that the old tag no longer exists and the new tag exists
    log_message('Verifying that tag "example_key" no longer exists and "modify_key" exists')
    try:
        log_message(f'The tag object: {tag_controller.get_tag("example_key")}')
    except KeyError as e:
        log_message(f'Error: {e}')

    log_message('Tag "modify_key" created successfully')
    log_message(f'The tag object: {tag_controller.get_tag("modify_key")}')

    # Delete the tag
    log_message('Going to delete tag "modify_key"')
    tag_controller.delete_tag('modify_key')

    # Verify that the tag has been deleted
    log_message('Verifying tag "modify_key" deleted by checking if it exists')
    try:
        tag_controller.get_tag('modify_key')
    except KeyError as e:
        log_message(f'Error: {e}')

    # Create additional tags
    log_message('Going to create 5 tags')
    for i in range(1, 6):
        tag_controller.create_tag(f'example_key{i}', f'example_value{i}')
    
    log_message('Tags created successfully')

    # Describe the created tags
    log_message('Describing tags')
    for tag in tag_controller.describe_tags():
        log_message(tag)

    # Log the end of the session
    log_message('Demonstration of object Tag ended successfully')
    log_message('--------------------- End Of Session ----------------------')
