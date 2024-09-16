import json
from datetime import datetime
import os
from DataAccess.ObjectManager import ObjectManager
from Storage.NEW_KT_Storage.DataAccess.StorageManager import StorageManager

class DBInstance:
    BASE_PATH = "db_instances"

    def __init__(self, **kwargs):
        """
        Initialize a new DBInstance with the given parameters.
        """
        self.db_instance_identifier = kwargs['db_instance_identifier']
        self.allocated_storage = kwargs.get('allocated_storage',20)
        self.master_username = kwargs['master_username']
        self.master_user_password = kwargs['master_user_password']
        self.db_name = kwargs.get('db_name', None)
        self.port = kwargs.get('port', 3306)
        self.status = 'available'
        self.created_time = datetime.now()
        self.endpoint = os.path.join(DBInstance.BASE_PATH, self.db_instance_identifier)
        storageManager=StorageManager(DBInstance.BASE_PATH)
        storageManager.create_directory(self.endpoint)
        self.databases = kwargs.get('databases', {})
        self.pk_column = kwargs.get('pk_column', 'db_instance_id')
        self.pk_value = kwargs.get('pk_value', self.db_instance_identifier)
    
    def to_dict(self):
        """Retrieve the metadata of the DB instance as a dictionary."""
        return ObjectManager.convert_object_attributes_to_dictionary(
            db_instance_identifier=self.db_instance_identifier,
            allocated_storage=self.allocated_storage,
            master_username=self.master_username,
            master_user_password=self.master_user_password,
            port=self.port,
            status=self.status,
            created_time=self.created_time,
            endpoint=self.endpoint,
            databases=self.databases,
            pk_column=self.pk_column,
            pk_value=self.pk_value

        )

    def to_sql(self):
        # Convert the model instance to a dictionary
        data_dict = self.to_dict()
        values = '(' + ", ".join(f'\'{json.dumps(v)}\'' if isinstance(v, dict) or isinstance(v, list) else f'\'{v}\'' if isinstance(v, str) else f'\'{str(v)}\''
                           for v in data_dict.values()) + ')'
        return values
