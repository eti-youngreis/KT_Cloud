from Storage.NEW_KT_Storage.Service.Classes.VersionService import VersionService
from Storage.NEW_KT_Storage.Validation.VersionValidition import *

class VersionController:
    def __init__(self, service: VersionService):
        self.service = service


    def validate_parameters(self, required_params, bucket_name, key, version_id=None):
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters.")

        if not is_valid_engine_name(bucket_name):
            raise ValueError(f"Invalid bucket name: {bucket_name}")

        if not is_valid_engine_name(key):
            raise ValueError(f"Invalid object key: {key}")


    def create_version(self, bucket_name, key, content  = " ", version_id = None, is_latest = None, last_modified = None, etag = None, size = None,
                              storage_class="STANDARD", owner=None, metadata=None, delete_marker=False,
                              checksum=None, encryption_status=None):
        required_params = ["bucket_name", "key"]
        self.validate_parameters(required_params, bucket_name, key, version_id)

        if not validate_tags(etag):
            raise ValueError("Invalid tags")

        return self.service.create(
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


    def delete_version(self, bucket_name, key, version_id):

        required_params = ["bucket_name", "key", "version_id"]
        self.validate_parameters(required_params, bucket_name, key, version_id)

        self.service.delete(bucket_name=bucket_name, key=key, version_id=version_id)
        return {"status": "success", "message": "Version object deleted successfully"}


    def get_version(self, bucket_name, key, version_id):

        required_params = ["bucket_name", "key", "version_id"]
        self.validate_parameters(required_params, bucket_name, key, version_id)
        try:
            version = self.service.get(bucket_name=bucket_name, key=key, version_id=version_id)
            return version
        except FileNotFoundError:
            raise ValueError(
                f"Version object not found for bucket '{bucket_name}', key '{key}', version '{version_id}'.")

        except Exception as e:
            raise ValueError(f"An unexpected error occurred while retrieving the version: {str(e)}")

        return version


    def describe_version(self):
        return self.service.describe()


    def analyze_version_changes(self, bucket_name: str, object_key: str, version_id1: str, version_id2: str):
        self.service.analyze_version_changes(bucket_name, object_key, version_id1, version_id2)


    def visualize_version_history(self, bucket_name: str, object_key: str):
        self.service.visualize_version_history(bucket_name, object_key)