import json
import os

from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy
from datetime import datetime, date

class LifecyclePolicyManager:

    def __init__(self, object_name: str, path_policy_file: str = "Lifecycle.json",
                 path_db: str = "Lifecycle.db",
                 base_directory: str = None):
        '''Initialize ObjectManager with the database connection.'''
        if base_directory is None:
            # Use relative path based on current directory if no base_directory is provided
            base_directory = os.path.join(os.getcwd(), "server")
        self.storage_manager = StorageManager(base_directory)
        self.json_name = path_policy_file
        self.object_name = object_name
        self.object_manager = ObjectManager(path_db)
        self.object_manager.object_manager.create_management_table(self.object_name, LifecyclePolicy.table_structure)

    def create_table(self):
        table_columns = "policy_name TEXT PRIMARY KEY", "status TEXT", "prefix TEXT", "expiration_days INT", "transitions_days_GLACIER INT", "creation_date DATETIME"
        columns_str = ", ".join(table_columns)
        self.object_manager.object_manager.db_manager.create_table("mng_Lifecycles", columns_str)

    def create(self, lifecycle_policy: LifecyclePolicy):
        existing_policy = self.get(lifecycle_policy.policy_name)
        if existing_policy:
            print(f"Policy with name '{lifecycle_policy.policy_name}' already exists")
            raise ValueError(f"Policy with name '{lifecycle_policy.policy_name}' already exists")
        if self.storage_manager.is_file_exist(self.json_name):
            data = self.read_to_json_file()
            data[lifecycle_policy.policy_name] = lifecycle_policy.__dict__
            self.write_to_json_file(data)
        else:
            data = {lifecycle_policy.policy_name: lifecycle_policy.__dict__}
            self.write_to_json_file(data)
        # save in DB
        self.object_manager.save_in_memory(self.object_name, lifecycle_policy.to_sql())

    def get(self, policy_name):
        data = self.read_to_json_file()
        if policy_name in data:
            policy_data = data[policy_name]
            return LifecyclePolicy(**policy_data)
        return None

    def delete(self, policy_name):
        data = self.read_to_json_file()
        if policy_name in data:
            del data[policy_name]
            self.write_to_json_file(data)
        self.object_manager.delete_from_memory_by_pk(self.object_name, LifecyclePolicy.pk_column, policy_name)

    def update(self, policy_name, lifecycle_update: LifecyclePolicy):
        existing_policy = self.get(policy_name)
        if not existing_policy:
            raise ValueError(f"Policy with name '{policy_name}' does not exist")

        # Proceed with update if policy exists
        data = self.read_to_json_file()
        data[policy_name] = lifecycle_update.__dict__
        self.write_to_json_file(data)

        # Updating the database record
        update_statement = (
            f"prefix = '{json.dumps(lifecycle_update.prefix)}', "
            f"status = '{lifecycle_update.status}', "
            f"expiration_days = {lifecycle_update.expiration_days}, "
            f"transitions_days_GLACIER = {lifecycle_update.transitions_days_GLACIER} "
        )
        criteria = f"policy_name = '{policy_name}'"
        self.object_manager.update_in_memory(self.object_name, update_statement, criteria)

    def describe(self, policy_id):
        data = self.read_to_json_file()
        return data[policy_id]

    def read_to_json_file(self):
        if self.storage_manager.is_file_exist(self.json_name):
            return self.storage_manager.read_to_json_file(self.json_name)
        return {}

    def write_to_json_file(self, data):
        self.storage_manager.write_to_json_file(self.json_name, data, default_converter=self.default_converter)

    @staticmethod
    def default_converter(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
