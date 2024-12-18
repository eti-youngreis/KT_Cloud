import datetime
import json
import os
from Storage.NEW_KT_Storage.Models.VersionModel import Version
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.Test.VersiontTests import version_id
from Storage.NEW_KT_Storage.Validation.VersionValidition import *

class VersionManager:

    def __init__(self, db_file: str = "DB\\NEW_KT_DB\\DBs\\mainDB.db", base_directory: str = "C:\\s3_project\\server\\versions"):

        '''Initialize VersionManager with the project root and database connection.'''

        project_root =  self.get_project_root()
        self.base_directory = os.path.join(project_root, base_directory)
        self.path_db = os.path.join(project_root, db_file)
        self.storage_manager = StorageManager(self.base_directory)
        self.object_manager = ObjectManager(self.path_db)
        self.object_manager.object_manager.create_management_table(Version.object_name, Version.table_structure)


    def get_project_root(self):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = current_dir
        while not os.path.exists(os.path.join(project_root, 'setup.py')) and project_root != os.path.dirname(
                project_root):
            project_root = os.path.dirname(project_root)
        while os.path.basename(current_dir) != "KT_Cloud":
            current_dir = os.path.dirname(current_dir)

        return current_dir


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

        version_file_name = f"{version_dict['version_id']}.json"
        version_file_path = os.path.join(version_directory, version_file_name)

        with open(version_file_path, 'w', encoding='utf-8') as version_file:
            json.dump(version, version_file, indent=4, ensure_ascii=False, default=self.serialize_object)

        print(f"Version for '{key}' saved to '{version_file_path}'.")
        self.object_manager.save_in_memory(Version.object_name, version.to_sql())
        return {"status": "success", "message": "Version object created successfully", "version_id": version_dict["version_id"]}


    def get_object_path(self, bucket_name, key):
        '''Retrieve the object path for a given bucket and key.'''
        return os.path.join(self.base_directory, bucket_name, key)


    def get_version(self, bucket_name, key, version_id):
        '''Retrieve the version object for a given bucket, key, and version_id.'''

        version_directory = os.path.join(self.base_directory, bucket_name, key)
        if not self.storage_manager.is_directory_exist(version_directory):  # Use StorageManager
            raise FileNotFoundError(f"Directory '{version_directory}' does not exist.")

        version_metadata_file_name = f'{version_id}.json'
        version_metadata_file_path = os.path.join(version_directory, version_metadata_file_name)
        if not self.storage_manager.is_file_exist(version_metadata_file_path):
            raise FileNotFoundError(
                f"No version object found for '{key}' in bucket '{bucket_name}' with version_id '{version_id}'.")

        with open(version_metadata_file_path, 'r') as version_file:
            version = json.load(version_file)

        print(f"Version object for '{key}' with version_id '{version_id}' retrieved successfully.")

        try:
            version_DB = self.object_manager.get_from_memory(Version.object_name, criteria = f"version_id='{version_id}' AND bucket_name='{bucket_name}' AND object_key='{key}'"
)
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

        pk_db = {"bucket_name": bucket_name, "object_key": key, "version_id": version_id}

        where_clause = " AND ".join(
            f"{k} = '{json.dumps(v)}'" if isinstance(v, dict) or isinstance(v, list)
            else f"{k} = '{v}'" if isinstance(v, str)
            else f"{k} = '{str(v)}'"
            for k, v in pk_db.items()
        )

        self.object_manager.delete_from_memory_by_criteria(Version.object_name, criteria=where_clause)