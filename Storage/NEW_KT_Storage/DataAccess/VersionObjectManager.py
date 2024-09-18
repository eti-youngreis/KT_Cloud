import datetime
import json
import os
import sqlite3
from Storage.NEW_KT_Storage.Models.ObjectVersioningModel import VersionObject
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager

class VersionManager:

    def __init__(self, db_file: str = "C:\\Users\\OWNER\\Desktop\\server\\versionDB.db",
                 base_directory: str = "C:\\Users\\OWNER\\Desktop\\server\\versions"):
        '''Initialize VersionManager with the database connection and storage paths.'''
        self.base_directory = base_directory
        self.path_db = os.path.join(base_directory, db_file)
        self.storage_manager = StorageManager(base_directory)  # Use StorageManager
        self.object_manager = ObjectManager(db_file)
        self.object_name = "version"
        self.create_table()

    def create_table(self):
        table_columns = "pk TEXT PRIMARY KEY","bucket_name TEXT","content", "version_id TEXT", "object_key TEXT", "is_latest TEXT", "last_modified DateTime", "size INT", "owner TEXT"
        columns_str = ", ".join(table_columns)
        self.object_manager.object_manager.db_manager.create_table("mng_versions", columns_str)


    @staticmethod
    def serialize_object(obj):
        if isinstance(obj, VersionObject):
            return obj.to_dict()
        elif isinstance(obj, datetime.datetime):
            return obj.isoformat()  # Convert to ISO format string
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def create_in_memory_version_object(self, version_object, bucket_name, key):
        # Create the path for the versions directory
        version_object_dict = version_object.to_dict()
        version_directory = os.path.join(self.base_directory, bucket_name, key)
        self.storage_manager.create_directory(version_directory)  # Use StorageManager

        # Get the current object path
        current_object_path = self.get_object_path(bucket_name, key)
        print(key, version_object_dict['version_id'])
        # Create a new version file with a timestamp or version_id
        version_file_name = f"{bucket_name}{key}{version_object_dict['version_id']}.json"
        version_file_path = os.path.join(version_directory, version_file_name)

        # Save version object data in a JSON file
        with open(version_file_path, 'w', encoding='utf-8') as version_file:
            json.dump(version_object, version_file, indent=4, ensure_ascii=False, default=self.serialize_object)

        print(f"Version object for '{key}' saved to '{version_file_path}'.")

        # Save version metadata in the database
        self.object_manager.save_in_memory(self.object_name, version_object.to_sql())

    def get_object_path(self, bucket_name, key):
        '''Retrieve the object path for a given bucket and key.'''
        return os.path.join(self.base_directory, bucket_name, key)


    def get_version_object(self, bucket_name, key, version_id):
        '''Retrieve the version object for a given bucket, key, and version_id.'''

        # Build the directory path where the version is stored
        version_directory = os.path.join(self.base_directory, bucket_name, key)

        # Verify that the directory exists
        if not self.storage_manager.is_directory_exist(version_directory):  # Use StorageManager
            raise FileNotFoundError(f"Directory '{version_directory}' does not exist.")

        # Print all the file names in the version directory
        print("Files in the directory:", self.storage_manager.list_files_in_directory(version_directory))  # Use StorageManager

        # Path to the version metadata file
        version_metadata_file_name = f'{version_id}.json'
        version_metadata_file_path = os.path.join(version_directory, version_metadata_file_name)

        if not self.storage_manager.is_file_exist(version_metadata_file_path):  # Use StorageManager
            raise FileNotFoundError(
                f"No version object found for '{key}' in bucket '{bucket_name}' with version_id '{version_id}'.")

        # Read the version object from the file
        with open(version_metadata_file_path, 'r') as version_file:
            version_object = json.load(version_file)

        print(f"Version object for '{key}' with version_id '{version_id}' retrieved successfully.")
        pk_db = bucket_name + key +version_id
        version_object_DB = self.object_manager.get_from_memory(self.object_name, criteria={"pk": pk_db})

        return version_object, version_object_DB

    def delete_version_object(self, bucket_name, key, version_id):
        '''Delete the version object for a given bucket, key, and version_id.'''

        # Build the directory path where the version is stored
        version_directory = os.path.join(self.base_directory, bucket_name, key)

        # Verify that the directory exists
        if not self.storage_manager.is_directory_exist(version_directory):  # Use StorageManager
            raise FileNotFoundError(f"Directory '{version_directory}' does not exist.")

        # Path to the version metadata file
        version_metadata_file_name = f'{version_id}.json'
        version_metadata_file_path = os.path.join(version_directory, version_metadata_file_name)

        # Check if the version file exists
        if not self.storage_manager.is_file_exist(version_metadata_file_path):  # Use StorageManager
            raise FileNotFoundError(
                f"No version object found for '{key}' in bucket '{bucket_name}' with version_id '{version_id}'.")

        # Delete the version object file from the directory
        self.storage_manager.delete_file(version_metadata_file_path)  # Use StorageManager
        print(f"Version object file '{version_metadata_file_name}' deleted from '{version_directory}'.")

        # Remove the version metadata from the database
        pk_db = version_id
        pk_db = {"pk": pk_db}
        where_clause = self.build_delete_criteria(pk_db)

        # Pass the where_clause directly
        self.object_manager.delete_from_memory_by_criteria(self.object_name, criteria=where_clause)

    def build_delete_criteria(self, criteria):
        where_clause = " AND ".join(
            f"{k} = '{json.dumps(v)}'" if isinstance(v, dict) or isinstance(v, list)
            else f"{k} = '{v}'" if isinstance(v, str)
            else f"{k} = '{str(v)}'"
            for k, v in criteria.items()
        )
        return where_clause
