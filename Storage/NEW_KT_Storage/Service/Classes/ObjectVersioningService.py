import datetime
from Storage.NEW_KT_Storage.Models.ObjectVersioningModel import VersionObject
from Storage.NEW_KT_Storage.Service.Abc.STO import STO
from Storage.NEW_KT_Storage.Validation.GeneralValidations import *
from Storage.NEW_KT_Storage.Validation.ObjectVersioningValidition import  *
from Storage.NEW_KT_Storage.DataAccess.VersionObjectManager import VersionManager


class ObjectVersioningService(STO):
    def __init__(self, manager: VersionManager):
        self.dal = manager

    def create(self, bucket_name: str, key: str, is_latest: bool, content, last_modified, etag: str, size: int, version_id = None,
               storage_class="STANDARD", owner=None, metadata=None, delete_marker=False, checksum=None,
               encryption_status=None):
        '''Create a new VersionObject.'''

        # Perform validations
        required_params = ["bucket_name", "key"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters")

        # ObjectVersioningValidation.validate_is_latest(is_latest)
        # ObjectVersioningValidation.validate_size(size)
        # Create the VersionObject model
        version_object = VersionObject(
            pk_value = bucket_name+key,
            bucket_name = bucket_name,
            object_key=key,
            version_id="_"+"v"+datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
            content = content,
            is_latest=is_latest,
            last_modified=last_modified,
            etag=etag,
            size=size,
            storage_class=storage_class,
            owner=owner
        )

        # Save physical object using StorageManager (e.g., create file or folder for the versioned object)
        # Placeholder: call StorageManager.create_file(), create_directory(), etc.

        # Save the object in memory using ObjectManager
        version_manager = VersionManager()
        self.dal.create_in_memory_version_object(version_object, bucket_name, key)

        return {"status": "success", "message": "Version object created successfully"}

    def get(self, bucket_name: str, key: str, version_id: str) -> VersionObject:
        '''Get a VersionObject.'''

        # Validate parameters
        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for retrieving version.")

        version_manager = VersionManager()

        # Use ObjectManager to retrieve the version object
        version_object = self.dal.get_version_object(bucket_name=bucket_name, key=key, version_id=version_id)

        if not version_object:
            raise ValueError("Version object not found.")

        return version_object

    def delete(self, bucket_name: str, key: str, version_id: str):
        '''Delete an existing VersionObject.'''

        # Validate parameters
        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for deleting version.")

        # Delete physical object (e.g., delete versioned file from storage)
        # Placeholder: call StorageManager.delete_file()

        version_manager = VersionManager()

        # Delete from memory using ObjectManager
        self.dal.delete_version_object(bucket_name=bucket_name, key=key, version_id=version_id)

        return {"status": "success", "message": "Version object deleted successfully"}

    def describe(self, bucket_name: str, key: str, version_id: str) -> Dict:
        '''Describe the details of a VersionObject.'''

        # Validate parameters
        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for describing version.")

        # Use ObjectManager to retrieve the version object details
        version_object = self.dal.describeVersionObject(bucket_name=bucket_name, object_key=key, version_id=version_id)

        if not version_object:
            raise ValueError("Version object not found.")

        return version_object.to_dict()

    # def put(self, bucket_name: str, key: str, version_id: str, updates: Dict):
    #     '''Modify an existing VersionObject.'''
    #
    #     # Validate parameters
    #     required_params = ["bucket_name", "key", "version_id", "updates"]
    #     if not GeneralValidation.check_required_params(required_params, locals()):
    #         raise ValueError("Missing required parameters for updating version.")
    #
    #     if not updates:
    #         raise ValueError("Updates must be a non-empty dictionary.")
    #
    #     # Modify physical object (e.g., update file attributes or contents in storage)
    #     # Placeholder: call StorageManager.update_file()
    #
    #     # Update in memory using ObjectManager
    #     self.dal.putVersionObject(bucket_name=bucket_name, object_key=key, version_id=version_id, updates=updates)
    #
    #     return {"status": "success", "message": "Version object updated successfully"}



    def put(self, bucket_name: str, key: str, version_id: str, updates: Dict):
        '''Modify an existing VersionObject.'''
        # Validate parameters
        required_params = ["bucket_name", "key", "version_id", "updates"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for updating version.")

        if not updates:
            raise ValueError("Updates must be a non-empty dictionary.")

        # Modify physical object (if needed)
        # Placeholder: call StorageManager.update_file()

        # Update in memory using ObjectManager
        self.dal.put_version_object(bucket_name=bucket_name, object_key=key, version_id=version_id, updates=updates)

        return {"status": "success", "message": "Version object updated successfully"}