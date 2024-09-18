import json
from datetime import datetime
import os
import sys
from DataAccess.ObjectManager import ObjectManager
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../../')))
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager


class DBInstance:
    BASE_PATH = "db_instances"
    object_name = 'db_instance'
    pk_column = 'db_instance_id'
    pk_column_data_type = 'TEXT'
    table_structure = 'db_instance_id VARCHAR(255) PRIMARY KEY NOT NULL, allocated_storage INT NOT NULL, master_username VARCHAR(255) NOT NULL, master_user_password VARCHAR(255) NOT NULL, db_name VARCHAR(255) NOT NULL, port INT NOT NULL, status VARCHAR(50) NOT NULL, created_time DATETIME NOT NULL, endpoint VARCHAR(255) NOT NULL, databases TEXT NOT NULL, pk_value VARCHAR(255) NOT NULL'

    def __init__(self, **kwargs):
        """
        Initialize a new DBInstance with the given parameters.
        """
        self.db_instance_identifier = kwargs['db_instance_identifier']
        self.allocated_storage = kwargs.get('allocated_storage', 20)
        self.master_username = kwargs['master_username']
        self.master_user_password = kwargs['master_user_password']
        self.db_name = kwargs.get('db_name', None)
        self.port = kwargs.get('port', 3306)
        self.status = 'available'
        self.created_time = datetime.now()
        self.endpoint = self.db_instance_identifier
        storageManager = StorageManager(DBInstance.BASE_PATH)
        storageManager.create_directory(self.endpoint)
        self.databases = kwargs.get('databases', {})
        self.pk_value = kwargs.get('pk_value', self.db_instance_identifier)

    def to_dict(self):
        """Retrieve the metadata of the DB instance as a dictionary."""
        return ObjectManager.convert_object_attributes_to_dictionary(
            db_instance_identifier=self.db_instance_identifier,
            allocated_storage=self.allocated_storage,
            master_username=self.master_username,
            master_user_password=self.master_user_password,
            db_name=self.db_name,
            port=self.port,
            status=self.status,
            created_time=str(self.created_time),
            endpoint=self.endpoint,
            databases=self.databases,
            pk_value=self.pk_value

        )

    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                                 for v in data_dict.values()) + ')'
        # values='(\''+self.db_instance_identifier+'\',\''+json.dumps(self.to_dict())+'\')'
        return values
