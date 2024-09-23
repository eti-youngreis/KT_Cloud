from DB.NEW_KT_DB.Service.Abc.DBO import DBO
from ...DataAccess.DBProxyManager import DBProxyManager
from DB.NEW_KT_DB.Models.DBProxyModel import DBProxy
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager
from ...Validation.GeneralValidations import validate_tags_structure
from ...Validation.DBProxyValidations import *
from ...Exceptions.DBProxyExceptions import *
from datetime import datetime
import json


class DBProxyService(DBO):
    """
    A service class responsible for handling operations related to DBProxy,
    including creating, modifying, deleting, and retrieving proxies.
    """

    def __init__(self, dal: DBProxyManager, storage_manager: StorageManager) -> None:
        """
        Initialize the service with a data access layer (DAL) and a storage manager.

        Args:
            dal (DBProxyManager): Data access layer for managing DBProxy-related operations.
            storage_manager (StorageManager): Manages storage operations like file creation and deletion.
        """
        self.dal = dal
        self.storage_manager = storage_manager

    def create(self, **attributes):
        """
        Create a new DBProxy instance. Validates the required parameters and creates a proxy.

        Args:
            **attributes: Key-value pairs representing the attributes of the DBProxy.

        Raises:
            TypeError: If required or invalid parameters are missing or extra.
            ValueError: If the DBProxy name or tags are invalid.
            DBProxyAlreadyExistsFault: If the DBProxy already exists.
        """
        required_params = ['db_proxy_name', 'engine_family', 'role_arn', 'auth', 'vpc_subnet_ids']
        all_params = ['vpc_security_group_ids', 'require_TLS', 'idle_client_timeout', 'debug_logging', 'tags']
        all_params.extend(required_params)

        # Validate the input parameters
        validate_init_db_proxy_params(attributes, required_params, all_params)
        validate_db_proxy_name(attributes.get('db_proxy_name'))

        # Validate tags if provided
        if 'tags' in attributes and not validate_tags_structure(attributes.get('tags')):
            raise ValueError("Tags not valid!")

        # Check if the proxy already exists
        if self.dal.is_db_proxy_exist(attributes.get('db_proxy_name')):
            raise DBProxyAlreadyExistsFault()

        # Create DBProxy and save it to storage
        proxy = DBProxy(**attributes)
        json_content = json.dumps(proxy.to_dict())
        path = proxy.db_proxy_name + ".json"
        self.storage_manager.create_file(path, json_content)

        # Insert the proxy data into the in-memory database
        self.dal.create_in_memory_DBProxy(proxy.to_sql())

    def delete(self, db_proxy_name):
        """
        Delete an existing DBProxy instance by its name.

        Args:
            db_proxy_name (str): The name of the DBProxy to delete.

        Raises:
            ValueError: If the DBProxy name is invalid.
            DBProxyNotFoundFault: If the DBProxy does not exist.
        """
        validate_db_proxy_name(db_proxy_name)

        # Check if the proxy exists
        if not self.dal.is_db_proxy_exist(db_proxy_name):
            raise DBProxyNotFoundFault()

        # Delete the proxy data from storage and memory
        self.storage_manager.delete_file(db_proxy_name + ".json")
        self.dal.delete_in_memory_DBProxy(object_id=db_proxy_name)

    def describe(self, db_proxy_name):
        """
        Describe a specific DBProxy instance by its name.

        Args:
            db_proxy_name (str): The name of the DBProxy to describe.

        Returns:
            dict: The details of the DBProxy.

        Raises:
            ValueError: If the DBProxy name is invalid.
            DBProxyNotFoundFault: If the DBProxy does not exist.
        """
        validate_db_proxy_name(db_proxy_name)

        # Check if the proxy exists
        if not self.dal.is_db_proxy_exist(db_proxy_name):
            raise DBProxyNotFoundFault()

        # Return the DBProxy details
        return self.dal.describe_DBProxy(db_proxy_name)[0]

    def modify(self, **updates):
        """
        Modify an existing DBProxy instance by updating its attributes.

        Args:
            **updates: Key-value pairs representing the updates to apply to the DBProxy.

        Raises:
            TypeError: If required or invalid parameters are missing or extra.
            ValueError: If the DBProxy name is invalid.
            DBProxyNotFoundFault: If the DBProxy does not exist.
        """
        required_params = ['db_proxy_name']
        all_params = ['engine_family', 'role_arn', 'auth', 'vpc_subnet_ids', 'vpc_security_group_ids', 'require_TLS',
                      'idle_client_timeout', 'debug_logging', 'tags']
        all_params.extend(required_params)

        # Validate the input parameters
        validate_init_db_proxy_params(updates, required_params, all_params)
        validate_db_proxy_name(updates.get('db_proxy_name'))

        # Check if the proxy exists
        if not self.dal.is_db_proxy_exist(updates.get('db_proxy_name')):
            raise DBProxyNotFoundFault()

        # Update the proxy's update_date
        updates['update_date'] = datetime.now().isoformat()

        # Load the existing data, update it, and save it back to storage
        path = updates.get('db_proxy_name') + '.json'
        old_data = self.storage_manager.get_file_content(path)
        old_data = json.loads(old_data)
        old_data.update(updates)
        new_data = json.dumps(old_data)
        self.storage_manager.write_to_file(updates.get('db_proxy_name') + '.json', new_data)

        # Update the in-memory database
        updates_in_sql = ', '.join([f"{key} = '{value}'" for key, value in updates.items()])
        self.dal.modify_DBProxy(updates.get('db_proxy_name'), updates_in_sql)

    def get(self, db_proxy_name):
        """
        Retrieve a DBProxy instance by its name from storage.

        Args:
            db_proxy_name (str): The name of the DBProxy to retrieve.

        Returns:
            DBProxy: A DBProxy object with the stored attributes.
        """
        path = db_proxy_name + '.json'
        content = self.storage_manager.get_file_content(path)
        dict_content = json.loads(content)
        return DBProxy(**dict_content)

