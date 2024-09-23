import datetime
import os
from Storage.NEW_KT_Storage.Controller.VersionController import VersionController
from Storage.NEW_KT_Storage.Service.Classes.VersionService import VersionService
from Storage.NEW_KT_Storage.DataAccess.VersionManager import VersionManager

def print_separator(char='-', length=70):
    print(char * length)

def print_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log_message(message):
    """Log a message with the current timestamp and return the current time."""
    current_time = datetime.datetime.now()
    print(f"{current_time.strftime('%H:%M:%S.%f')[:-3]} {message}")
    return current_time

def calculate_duration_time(start, end):
    """Calculate the duration between two timestamps."""
    return end - start


def main():
    print_separator('=')
    log_message("--------------------- Start Of Session ----------------------")
    log_message("Version Demonstration")
    print_separator('=')

    # Initialize VersionController
    version_service = VersionService(VersionManager())
    version_controller = VersionController(version_service)

    # Test parameters
    bucket_name = "demo-bucket"
    object_key = "sample-object"
    content = "version2.txt"

    try:
        # 1. Create Version
        log_message("Going to create version")
        start = log_message("Creating a new version")

        create_result = version_controller.create_version(
            bucket_name=bucket_name,
            key=object_key,
            content=content,
            is_latest=True,
            last_modified=datetime.datetime.now(),
            etag={"1":"e123456789"},
            size=len(content),
            storage_class="STANDARD"
        )
        #print(f"Creation Result: {create_result}")
        print_separator('-')

        # Verify creation
        log_message('Verifying version creation')
        version_id = create_result["version_id"]  # Assuming create_result returns the created version_id
        try:
            get_result = version_controller.get_version(bucket_name, object_key, version_id)
            log_message(f"Retrieved Version after creation: {get_result}")
            print_separator('-')

        except KeyError as e:
            log_message(f"Error: {e}")

        end = log_message(f'version {version_id} created successfully')
        print(f"Duration time: {calculate_duration_time(start, end)}")
        print_separator()

        # 2. Get Version
        log_message("Going to get version")
        start = log_message("Get exist version")
        log_message("Retrieving the exist version")
        #version_id = "v25a2a223-e20f-4660-89bc-6cddb05ff430"
        get_result = version_controller.get_version(bucket_name, object_key, version_id)
        log_message(f"Retrieved Version: {get_result}")

        end = log_message(f'version {version_id} get successfully')
        print(f"Duration time: {calculate_duration_time(start, end)}")
        print_separator()
        print_separator('-')

        # 3. Analyze Version Changes
        start = log_message("Going to analyze version Changes")

        log_message("Analyzing version changes:")
        version_controller.analyze_version_changes(bucket_name, object_key, "vd1110ed5-76cb-4d4b-9510-a6765a06cbd6", "vaee36d8c-2295-4e13-8997-be7d80cb65c8")
        log_message("Version analysis completed.")
        print_separator('-')

        # 4. Visualize Version History
        start = log_message("Going to visualize versions History")

        log_message("Visualizing version history:")
        version_controller.visualize_version_history(bucket_name, object_key)
        log_message(f"Version history visualization saved as 'versions/{bucket_name}/{object_key}/version_history.png'")
        print_separator('-')

        # 5. Delete Version
        log_message("Going to delete version")
        start = log_message("Delete a exist version")

        delete_result = version_controller.delete_version(bucket_name, object_key, version_id)
        print(f"Deletion Result: {delete_result}")

        # Verify deletion
        log_message('Verifying version deletion')

        """Verify that the version object has been deleted from the system."""

        version_file_path = f"C:/s3_project/server/versions/{bucket_name}/{object_key}/{version_id}.json"

        if not os.path.exists(version_file_path):
             print(f"File is not exist")
        else:
            raise ValueError(
                f"Failed to delete version object {version_id}. File still exists at {version_file_path}.")
        end = log_message("After deletion verification")
        print(f"Duration time: {calculate_duration_time(start, end)}")
        print_separator('=')

    except KeyError as e:
        log_message(f"Error: {e}")
        # print_separator('=')

    # Log the end of the session
    log_message("Demonstration of Version ended successfully")
    log_message("--------------------- End Of Session ----------------------")

if __name__ == "__main__":
    main()