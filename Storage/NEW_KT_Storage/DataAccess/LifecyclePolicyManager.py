import json
import os

from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy
from datetime import datetime, date


class LifecyclePolicyManager:

    def __init__(self, object_name: str, policy_file: str = "Lifecycle.json",
                 db_path: str = "KT_Cloud\\DB\\NEW_KT_DB\\DBs\\mainDB.db", base_dir: str = None,
                 path_policy_file="test_lifecycle.json", path_db=":memory:", base_directory="./test_data"):
        '''Initialize ObjectManager with the database connection and set up storage.'''
        if base_dir is None:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            while os.path.basename(current_dir) != "KT_Cloud":
                current_dir = os.path.dirname(current_dir)
            base_dir = os.path.dirname(current_dir)
        self.storage_manager = StorageManager(base_dir)
        self.policy_file = policy_file
        self.object_name = object_name
        self.object_manager = ObjectManager(os.path.join(base_dir, db_path))
        self.object_manager.object_manager.create_management_table(self.object_name, LifecyclePolicy.table_structure)

    def create_table(self):
        table_columns = "policy_name TEXT PRIMARY KEY", "status TEXT", "prefix TEXT", "expiration_days INT", "transitions_days_glacier INT", "creation_date DATETIME"
        columns_str = ", ".join(table_columns)
        self.object_manager.object_manager.db_manager.create_table("mng_Lifecycles", columns_str)

    def create(self, lifecycle_policy: LifecyclePolicy):
        existing_policy = self.get(lifecycle_policy.policy_name)
        if existing_policy:
            print(f"Policy with name '{lifecycle_policy.policy_name}' already exists")
            raise ValueError(f"Policy with name '{lifecycle_policy.policy_name}' already exists")
        if self.storage_manager.is_file_exist(self.policy_file):
            data = self.read_json_file()
            data[lifecycle_policy.policy_name] = lifecycle_policy.__dict__
            self.write_to_json_file(data)
        else:
            data = {lifecycle_policy.policy_name: lifecycle_policy.__dict__}
            self.write_to_json_file(data)
        self.object_manager.save_in_memory(self.object_name, lifecycle_policy.to_sql())

    def get(self, policy_name):
        data = self.read_json_file()
        if policy_name in data:
            policy_data = data[policy_name]
            return LifecyclePolicy(**policy_data)
        return None

    def delete(self, policy_name):
        data = self.read_json_file()
        if policy_name in data:
            del data[policy_name]
            self.write_to_json_file(data)
        self.object_manager.delete_from_memory_by_pk(self.object_name, LifecyclePolicy.pk_column, policy_name)

    def update(self, policy_name, lifecycle_update: LifecyclePolicy):
        existing_policy = self.get(policy_name)
        if not existing_policy:
            raise ValueError(f"Policy with name '{policy_name}' does not exist")

        # Proceed with update if policy exists
        data = self.read_json_file()
        data[policy_name] = lifecycle_update.__dict__
        self.write_to_json_file(data)

        # Updating the database record
        update_statement = (
            f"prefix = '{json.dumps(lifecycle_update.prefix)}', "
            f"bucket_name = '{lifecycle_update.bucket_name}', "
            f"status = '{lifecycle_update.status}', "
            f"expiration_days = {lifecycle_update.expiration_days}, "
            f"transitions_days_glacier = {lifecycle_update.transitions_days_glacier} "
        )
        criteria = f"policy_name = '{policy_name}'"
        self.object_manager.update_in_memory(self.object_name, update_statement, criteria)

    def describe(self, lifecycle: LifecyclePolicy) -> str:
        """
        Provide a detailed, human-readable description of a lifecycle policy by its name.
        """
        # Check if the policy exists

        # If the policy does not exist, raise an informative error
        if not lifecycle:
            raise ValueError(f"Policy with name '{lifecycle.policy_name}' not found")

        # Retrieve the policy data

        # Construct a human-readable description
        description = f"Lifecycle Policy '{lifecycle.policy_name}' Details:\n"
        description += f"- The policy is applied to the bucket: '{lifecycle.bucket_name}'.\n"
        description += f"- Current status of the policy: '{lifecycle.status}'.\n"

        if lifecycle.expiration_days:
            description += f"- Objects will expire after {lifecycle.expiration_days} days.\n"
        else:
            description += "- No expiration date is set for objects.\n"

        if lifecycle.transitions_days_glacier:
            description += f"- Objects will transition to Glacier storage after {lifecycle.transitions_days_glacier} days.\n"
        else:
            description += "- No transition to Glacier storage is set.\n"

        if lifecycle.prefix:
            description += f"- This policy applies to objects with the following prefix(es): {', '.join(lifecycle.prefix)}.\n"
        else:
            description += "- No specific prefixes are set for this policy.\n"

        description += f"- Policy creation date: {lifecycle.creation_date}.\n"

        return description

    def read_json_file(self):
        if self.storage_manager.is_file_exist(self.policy_file):
            return self.storage_manager.read_json_file(self.policy_file)
        return {}

    def write_to_json_file(self, data):
        self.storage_manager.write_to_json_file(self.policy_file, data, default_converter=self.default_converter)

    @staticmethod
    def default_converter(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
