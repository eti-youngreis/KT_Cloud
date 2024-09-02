import sqlite3  
from permissionModel import Action, Resource, Effect
# from DataAccess import DBManager  

class PermissionManager:
    """
    Manages permissions stored in a SQLite database. This class handles operations
    like creating, checking, deleting, and listing permissions using the DBManager
    for database interactions.
    """

    def __init__(self, db_file: str):
        """
        Initialize PermissionManager with a database connection.
        
        :param db_file: Path to the SQLite database file.
        """
        self.db_manager = DBManager(db_file)  
        self.table_name = 'permission_manager'  
        self.create_table()  

    def create_permission(self, permission):
        """
        Insert a new permission into the database.
        
        :param permission: A dictionary containing the permission data to be inserted.
        """
        self.db_manager.insert(self.table_name, permission)  

    def is_exist_permission(self, action, resource, effect):
        """
        Check if a permission exists in the database based on action, resource, and effect.
        
        :param action: Action associated with the permission (e.g., 'read', 'write').
        :param resource: Resource associated with the permission (e.g., 'bucket_name').
        :param effect: Effect of the permission (e.g., 'allow', 'deny').
        :return: True if the permission exists, otherwise False.
        """
        query = f'SELECT 1 FROM {self.table_name} WHERE metadata LIKE ? LIMIT 1'  
        search_pattern = f"%'action': '{action}'%'resource': '{resource}'%'effect': '{effect}'%" 
        
        try:
            c = self.db_manager.connection.cursor() 
            c.execute(query, (search_pattern,))  
            result = c.fetchone()  
            return result is not None  
        except sqlite3.OperationalError as e:
            raise Exception(f'Error checking for permission existence: {e}')

    def create_table(self):
        """
        Create the permissions table if it does not already exist in the database.
        
        The table includes:
        - object_id: Primary key (auto-increment integer).
        - metadata: A text field for storing JSON-like permission data.
        
        :return: Result of the table creation operation from DBManager.
        """
        table_schema = 'object_id INTEGER PRIMARY KEY AUTOINCREMENT, metadata TEXT NOT NULL'  
        return self.db_manager.create_table(self.table_name, table_schema) 

    def delete_permission(self, permission_id):
        """
        Delete a permission from the database by its ID.
        
        :param permission_id: ID of the permission to delete.
        :return: Result of the deletion operation from DBManager.
        """
        criteria = f'object_id = {permission_id}'
        return self.db_manager.delete(self.table_name, criteria) 
    
    def update_permission(self,permission_id: int, action: Action = None, resource: Resource = None, effect: Effect = None):
        criteria = f'object_id = {permission_id}' 
        return self.db_manager.update(self.table_name, criteria)

    def list_permissions(self):
        """
        Retrieve all permissions stored in the database.
        
        :return: A list of all permissions retrieved from the table.
        """
        return self.db_manager.select(self.table_name)  

    def get_permission(self, permission_id: int):
        """
        Retrieve a specific permission by its ID from the database.
        
        :param permission_id: ID of the permission to retrieve.
        :return: A dictionary with the permission data, or None if not found.
        """
        criteria = f'object_id = {permission_id}' 
        return self.db_manager.select(self.table_name, criteria) 
