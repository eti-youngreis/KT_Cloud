import sqlite3
from types import dict
from Models import TenantDatabase
from DataAccess import DataAccessLayer


class TenantDatabaseService:
    def __init__(self, dal: DataAccessLayer):
        self.dal = dal
    
    def create(self, **kwargs):
        print('In the create function')
        #handlle validations
        # tenant_db = TenantDatabase(**kwargs)
        # self.dal.insert('TenantDB', tenant_db.to_dict())
    
    def delete(self, tenant_db_name: str):
        print('In the delete function')
        #handlle validations
        # self.dal.delete('TenantDB', tenant_db_name)

    def describe(self, tenant_db_name: str):
        print('In the describe function')
        #handlle validations
        # data = self.dal.select('TenantDB', tenant_db_name)
        # return data


    def modify(self, tenant_db_name: str, updates: dict):
        print('In the modify function')
        # if not self.dal.exists('TenantDB', tenant_db_name):
        #     raise ValueError(f"Tenant data base '{tenant_db_name}' does not exist.")
        
        # current_data = self.dal.select('TenantDB', tenant_db_name)
        # if current_data is None:
        #     raise ValueError(f"Tenant data base '{tenant_db_name}' does not exist.")
        
        # updated_data = {**current_data, **updates}
        # self.dal.update('TenantDB', tenant_db_name, updated_data)



