from permissionModel import Action
# from DataAccess impotr DBManager
class permissionManager:

    def __init__(self, db_file: str):

        '''Initialize permissionManager with the database connection.'''
        self.db_manager = DBManager(db_file)
        self.table_name ='permission_manager'
        self.create_table()

    def create_permission(self,permission):
        self.db.insert(self.table_name, permission)


    def create_table(self):
        '''create objects table in the database'''
        table_schema = 'object_id INTEGER PRIMARY KEY AUTOINCREMENT ,metadata TEXT NOT NULL'
        self.db_manager.create_table(self.table_name, table_schema)

    
    def delete_permission(self, permission_id):
        criteria = f'object_id = {permission_id}'
        self.dal.delete(self.table_name, criteria)


