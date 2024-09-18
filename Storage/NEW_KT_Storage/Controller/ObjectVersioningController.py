from Storage.NEW_KT_Storage.Service.Classes.ObjectVersioningService import ObjectVersioningService
from Storage.NEW_KT_Storage.Validation.GeneralValidations import *
from Storage.NEW_KT_Storage.Validation.ObjectVersioningValidition import *
from Storage.NEW_KT_Storage.DataAccess.VersionObjectManager import VersionManager
from Storage.NEW_KT_Storage.Models.ObjectVersioningModel import VersionObject



class ObjectVersioningController:
    def __init__(self, service: ObjectVersioningService):
        print("hello")
        self.service = service

    def create_version_object(self, bucket_name, key, content  = " ", version_id = None, is_latest = None, last_modified = None, etag = None, size = None,
                              storage_class="STANDARD", owner=None, metadata=None, delete_marker=False,
                              checksum=None, encryption_status=None):
        # Checking that the required parameters exist
        required_params = ["bucket_name", "key"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters")

        # Validate parameters
        if not is_valid_engine_name(key):
            raise ValueError(f"Invalid object key: {key}")

        if not is_valid_engine_name(bucket_name):
            raise ValueError(f"Invalid bucket name: {bucket_name}")

        # if not is_valid_number(version_id, min=1):
        #     raise ValueError(f"Invalid version id: {version_id}")

        if not validate_tags(etag):
            raise ValueError("Invalid tags")

        # validate_is_latest(is_latest)
        # validate_size(size)

        # Call the create function with the validated parameters
        self.service.create(
            bucket_name=bucket_name,
            key=key,
            version_id=version_id,
            content = content,
            is_latest=is_latest,
            last_modified=last_modified,
            etag=etag,
            size=size,
            storage_class=storage_class,
            owner=owner,
            metadata=metadata,
            delete_marker=delete_marker,
            checksum=checksum,
            encryption_status=encryption_status
        )

        return {"status": "success", "message": "Version object created successfully"}

    def delete_version_object(self, bucket_name, key, version_id):
        # Checking that the required parameters exist
        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for deleting version.")

        # Validate parameters
        if not is_valid_engine_name(bucket_name):
            raise ValueError(f"Invalid bucket name: {bucket_name}")

        if not is_valid_engine_name(key):
            raise ValueError(f"Invalid object key: {key}")

        # if not is_valid_number(version_id, min=1):
        #     raise ValueError(f"Invalid version id: {version_id}")

        # Call to the service function to delete the version
        self.service.delete(bucket_name=bucket_name, key=key, version_id=version_id)
        return {"status": "success", "message": "Version object deleted successfully"}


    def get_version_object(self, bucket_name, key, version_id):
        # Checking that the required parameters exist
        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for retrieving version.")

        # Validate parameters
        if not is_valid_engine_name(bucket_name):
            raise ValueError(f"Invalid bucket name: {bucket_name}")

        if not is_valid_engine_name(key):
            raise ValueError(f"Invalid object key: {key}")

        #if not is_valid_number(version_id, min=1):
            #raise ValueError(f"Invalid version id: {version_id}")

        # Call to the service function to get the requested version
        version_object = self.service.get(bucket_name=bucket_name, key=key, version_id=version_id)
        if not version_object:
            raise ValueError("Version object not found.")

        return version_object



def main():
    manager = VersionManager()
    service = ObjectVersioningService(manager)
    controller = ObjectVersioningController(service)

    controller.create_version_object("books", "shabos")

    #v = controller.get_version_object("books", "shabos", "booksshabos_v20240918122201")
    #print(v)

    #controller.delete_version_object("books", "shabos", "booksshabos_v20240918122201")
if __name__ == "__main__":
    main()