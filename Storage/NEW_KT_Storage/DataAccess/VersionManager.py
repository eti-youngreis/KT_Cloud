import datetime
import json
import os
from Storage.NEW_KT_Storage.Models.VersionModel import Version
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Validation.VersionValidition import *
class VersionManager:

    def __init__(self, db_file: str = "versionDB.db", base_directory: str = "versions"):
        '''Initialize VersionManager with the database connection and storage paths.'''
        self.base_directory = base_directory
        self.path_db = os.path.join(base_directory, db_file)
        self.storage_manager = StorageManager(base_directory)  # Use StorageManager
        self.object_manager = ObjectManager(db_file)
        self.object_manager.object_manager.create_management_table(Version.object_name, Version.table_structure)

    @staticmethod
    def serialize_object(obj):
        """
        Converts objects to a JSON-compatible format.

        Parameters:
        obj: The object to serialize (supports `Version` and `datetime.datetime`).

        Returns:
        dict/str: A dictionary for `Version` objects or ISO format string for `datetime`.

        Raises:
        TypeError: If the object is not serializable.
        """
        if isinstance(obj, Version):
            return obj.to_dict()
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def create_in_memory_version(self, version, bucket_name, key):

        version_dict = version.to_dict()
        version_directory = os.path.join(self.base_directory, bucket_name, key)
        self.storage_manager.create_directory(version_directory)  # Use StorageManager

        version_file_name = f"{bucket_name}{key}{version_dict['version_id']}.json"
        version_file_path = os.path.join(version_directory, version_file_name)

        with open(version_file_path, 'w', encoding='utf-8') as version_file:
            json.dump(version, version_file, indent=4, ensure_ascii=False, default=self.serialize_object)

        print(f"Version for '{key}' saved to '{version_file_path}'.")

        self.object_manager.save_in_memory(Version.object_name, version.to_sql())

    def get_object_path(self, bucket_name, key):
        '''Retrieve the object path for a given bucket and key.'''
        return os.path.join(self.base_directory, bucket_name, key)


    def get_version(self, bucket_name, key, version_id):
        '''Retrieve the version object for a given bucket, key, and version_id.'''

        version_directory = os.path.join(self.base_directory, bucket_name, key)

        if not self.storage_manager.is_directory_exist(version_directory):  # Use StorageManager
            raise FileNotFoundError(f"Directory '{version_directory}' does not exist.")

        print("Files in the directory:", self.storage_manager.list_files_in_directory(version_directory))  # Use StorageManager

        version_metadata_file_name = f'{version_id}.json'
        version_metadata_file_path = os.path.join(version_directory, version_metadata_file_name)
        if not self.storage_manager.is_file_exist(version_metadata_file_path):  # Use StorageManager
            raise FileNotFoundError(
                f"No version object found for '{key}' in bucket '{bucket_name}' with version_id '{version_id}'.")

        with open(version_metadata_file_path, 'r') as version_file:
            version = json.load(version_file)

        print(f"Version object for '{key}' with version_id '{version_id}' retrieved successfully.")
        pk_db = version_id
        try:
            version_DB = self.object_manager.get_from_memory(Version.object_name, criteria={"version_pk": pk_db})
        except Exception as e:
            print("version didn't found in data base")

        return version, version_DB

    def delete_version(self, bucket_name, key, version_id):
        '''Delete the version object for a given bucket, key, and version_id.'''

        required_params = ["bucket_name", "key", "version_id"]
        if not check_required_params(required_params, locals()):
            raise ValueError("Missing required parameters for deleting version.")

        version_directory = os.path.join(self.base_directory, bucket_name, key)

        if not self.storage_manager.is_directory_exist(version_directory):
            raise FileNotFoundError(f"Directory '{version_directory}' does not exist.")

        version_metadata_file_name = f'{version_id}.json'
        version_metadata_file_path = os.path.join(version_directory, version_metadata_file_name)

        if not self.storage_manager.is_file_exist(version_metadata_file_path):
            raise FileNotFoundError(
                f"No version object found for '{key}' in bucket '{bucket_name}' with version_id '{version_id}'.")

        self.storage_manager.delete_file(version_metadata_file_path)
        print(f"Version object file '{version_metadata_file_name}' deleted from '{version_directory}'.")

        pk_db = {"version_pk": version_id}

        where_clause = " AND ".join(
            f"{k} = '{json.dumps(v)}'" if isinstance(v, dict) or isinstance(v, list)
            else f"{k} = '{v}'" if isinstance(v, str)
            else f"{k} = '{str(v)}'"
            for k, v in pk_db.items()
        )

        self.object_manager.delete_from_memory_by_criteria(Version.object_name, criteria=where_clause)