from typing import Dict, Any, Optional
import json

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DB_UserAdministration.Models.PolicyModel import Policy


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from DB_UserAdministration.DataAccess.DBManager import DBManager

# commit
class PolicyManager(DBManager):
    def __init__(self, db_file: str):
        '''Initialize PolicyManager with the database connection.'''
        self.object_type = Policy
        self.table_name = 'policy_management'
        self.identifier_param = 'policy_id'
        self.columns = [self.identifier_param, 'permissions']
        super().__init__(db_file=db_file)

    def create_table(self):
        '''Create policies table in the database.'''
        table_schema = f'{self.identifier_param} TEXT NOT NULL PRIMARY KEY, permissions TEXT'
        super().create_table(self.table_name, table_schema)

    



