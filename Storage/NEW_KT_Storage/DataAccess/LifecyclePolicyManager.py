import json
from Storage.NEW_KT_Storage.DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from Storage.NEW_KT_Storage.Models.LifecyclePolicyModel import LifecyclePolicy
from datetime import datetime, date

class LifecyclePolicyManager:

    def __init__(self, path_physical_object: str = "Lifecycle.json", path_db:str = "C:\\Users\\User\\Desktop\\database\\Lifecycle.db", base_directory: str = "C:\\Users\\User\\Desktop\\server"):
        '''Initialize ObjectManager with the database connection.'''
        self.storage_manager = StorageManager(base_directory)
        self.json_name = path_physical_object
        self.object_name = "Lifecycle"
        self.object_manager = ObjectManager(path_db)
        self.create_table()

    def create_table(self):
        table_columns = "policy_name TEXT PRIMARY KEY", "status TEXT", "prefix TEXT", "expiration_days INT", "transitions_days_GLACIER INT", "creation_date DATETIME"
        columns_str = ", ".join(table_columns)
        self.object_manager.object_manager.db_manager.create_table("mng_Lifecycles", columns_str)

    def create(self, lifecycle_policy: LifecyclePolicy):
        # save physical_object
        if self.storage_manager.is_file_exist(self.json_name):
            data = self._read_json()
            data[lifecycle_policy.policy_name] = lifecycle_policy.__dict__
            self._write_json(data)
        else:
            data = {lifecycle_policy.policy_name: lifecycle_policy.__dict__}
            self._write_json(data)
        # save in DB
        self.object_manager.save_in_memory(self.object_name, lifecycle_policy.to_sql())

    def get(self, policy_name):
        data = self._read_json()
        if policy_name in data:
            lifecycle_policy_info = data[policy_name]
            lifecycle_policy = LifecyclePolicy(
                    policy_name=lifecycle_policy_info['policy_name'],
                    status=lifecycle_policy_info['status'],
                    creation_date=lifecycle_policy_info['creation_date'],
                    expiration_days=lifecycle_policy_info['expiration_days'],
                    prefix=lifecycle_policy_info['prefix'],
                    transitions_days_GLACIER=lifecycle_policy_info['transitions_days_GLACIER'],
                    lifecycle_policy_id=lifecycle_policy_info['lifecycle_policy_id'])
            return lifecycle_policy
        return None

    def delete(self, policy_name):
        data = self._read_json()
        if policy_name in data:
            del data[policy_name]
            self._write_json(data)
        self.object_manager.delete_from_memory_by_pk(self.object_name, LifecyclePolicy.pk_column, policy_name)

    def update(self, policy_name, lifecycle_update: LifecyclePolicy):
        data = self._read_json()
        data[policy_name] = lifecycle_update.__dict__
        self._write_json(data)

        update_statement = f"prefix = '{lifecycle_update.prefix}', status = '{lifecycle_update.status}', " \
                           f"expiration_days = '{lifecycle_update.expiration_days}', " \
                           f"transitions_days_GLACIER = '{lifecycle_update.transitions_days_GLACIER}' "
        criteria = f"policy_name = '{policy_name}'"
        self.object_manager.update_in_memory(self.object_name, update_statement, criteria)

    def describe(self, policy_id):
        data = self._read_json()
        return data[policy_id]

    def _read_json(self):
        if self.storage_manager.is_file_exist(self.json_name):
            return self.storage_manager.read_json(self.json_name)
        return {}

    def _write_json(self, data):
        self.storage_manager.write_json(self.json_name, data, default_converter=self.default_converter)

    @staticmethod
    def default_converter(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")
