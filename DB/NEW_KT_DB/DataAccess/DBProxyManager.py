from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
from DB.NEW_KT_DB.Models.DBProxyModel import DBProxy
from .DBManager import EmptyResultsetError


class DBProxyManager:
    def __init__(self, object_manager: ObjectManager) -> None:
        """
        Initialize the DBProxyManager.

        Parameters:
        object_manager (ObjectManager): An instance of ObjectManager for managing DBProxy objects.
        """
        self.object_manager = object_manager
        # Create the management table for DBProxy
        self.object_manager.create_management_table(DBProxy.object_name, DBProxy.table_structure, 'TEXT')

    def create_in_memory_DBProxy(self, metadata):
        """
        Create a DBProxy object in memory.

        Parameters:
        metadata (dict): The metadata for the DBProxy to be saved.
        """
        self.object_manager.save_in_memory(DBProxy.object_name, metadata)

    def delete_in_memory_DBProxy(self, object_id):
        """
        Delete a DBProxy object from memory by its primary key.

        Parameters:
        object_id (str): The primary key of the DBProxy to delete.
        """
        self.object_manager.delete_from_memory_by_pk(DBProxy.object_name, DBProxy.pk_column, object_id)

    def describe_DBProxy(self, object_id):
        """
        Retrieve a DBProxy object by its primary key.

        Parameters:
        object_id (str): The primary key of the DBProxy to describe.

        Returns:
        dict: The DBProxy object metadata.
        """
        criteria = f"{DBProxy.pk_column} = '{object_id}'"
        return self.object_manager.get_from_memory(DBProxy.object_name, criteria=criteria)

    def modify_DBProxy(self, object_id, updates):
        """
        Modify an existing DBProxy object in memory.

        Parameters:
        object_id (str): The primary key of the DBProxy to modify.
        updates (dict): The updates to apply to the DBProxy.
        """
        criteria = f"{DBProxy.pk_column} = '{object_id}'"
        self.object_manager.update_in_memory(DBProxy.object_name, updates, criteria)

    def is_exist(self, object_id):
        """
        Check if a DBProxy object exists in memory.

        Parameters:
        object_id (str): The primary key of the DBProxy to check.

        Returns:
        bool: True if the DBProxy exists, False otherwise.
        """
        try:
            criteria = f"{DBProxy.pk_column} = '{object_id}'"
            res = self.object_manager.get_from_memory(DBProxy.object_name, criteria=criteria)
            return True if res else False

        except EmptyResultsetError:
            return False
        except Exception as e:
            return False

