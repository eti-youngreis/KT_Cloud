from permissionModel import Action
import sqlite3
# from DataAccess impotr DBManager
class permissionManager:

    def __init__(self, db_file: str):
        '''Initialize permissionManager with the database connection.'''
        self.db_manager = DBManager(db_file)
        self.table_name ='permission_manager'
        self.create_table()

    def create_permission(self,permission):
        self.db.insert(self.table_name, permission)

    def is_exit_permission(self, action, resource, effect):
        '''Check if an permission with these given values exists in the database.'''
        query = f'SELECT 1 FROM {self.table_name} WHERE metadata LIKE ? LIMIT 1'
        try:
            c = self.db_manager.connection.cursor()
            c.execute(query, (f'%\'action\': \'{action}\'%
                             %\'resource\': \'{resource}\'%
                            %\'effect\': \'{effect}\'%',))
            result = c.fetchone()
            return result is not None
        except sqlite3.OperationalError as e:
            raise Exception(f'Error checking for option group existence: {e}')


    def create_table(self):
        '''create objects table in the database'''
        table_schema = 'object_id INTEGER PRIMARY KEY AUTOINCREMENT ,metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)

    
    def delete_permission(self, permission_id):
        criteria = f'object_id = {permission_id}'
        self.dal.delete(self.table_name, criteria)


