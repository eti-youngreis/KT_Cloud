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
        pass

    
    def delete_permission(self):
        pass


