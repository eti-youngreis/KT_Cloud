import json
import os
from shutil import copy2
from datetime import datetime
from typing import Dict
from watchdog.observers import Observer
from KT_Cloud.DB.Models.event_file import MyHandler


class DBInstance:
    BASE_PATH = 'db_instances'
    observer = Observer()

    def __init__(self, db_instance_identifier: str, allocated_storage: int = None, port: int = 3306, region: str = 'a', is_replica: bool = False,
                 source_instance: 'DBInstance' = None, backup_auto: bool = False,  region_auto_backup: str = '',  created_time: datetime = None,
                 master_username: str = None,  master_user_password: str = None, databases: dict = None,  db_name: str = None,  path_file: str = None):
        """
        Initializes a DBInstance or Replica instance based on the is_replica flag.

        Args:
            db_instance_identifier (str): The identifier for the DB instance.
            allocated_storage (int): The allocated storage size.
            port (int): The port number (default: 3306).
            region (str): The region where the instance is created (default: 'a').
            is_replica (bool): Indicates if the instance is a replica.
            source_instance (DBInstance, optional): The source instance to replicate from.
            backup_auto (bool): Whether to automatically backup the instance.
            region_auto_backup (str): The region for automatic backup.
            created_time (datetime, optional): The creation time of the instance (default: now).
            master_username (str, optional): The master username for the DB instance.
            master_user_password (str, optional): The master user password for the DB instance.
            databases (dict, optional): A dictionary of databases associated with the instance.
            db_name (str, optional): The name of the database (only for primary instances).
            path_file (str, optional): The file path for storing instance data.
        """
        try:
            self.db_instance_identifier = db_instance_identifier
            self.allocated_storage = allocated_storage if allocated_storage is not None else source_instance.allocated_storage if source_instance else allocated_storage
            self.port = port if port is not None else source_instance.port if source_instance else port
            self.region = region if region is not None else source_instance.region if source_instance else region
            self.backup_auto = backup_auto if backup_auto is not None else source_instance.backup_auto if source_instance else False
            self.region_auto_backup = region_auto_backup if region_auto_backup is not None else source_instance.region_auto_backup if source_instance else ''
            self.status = 'available'
            self.created_time = created_time if created_time is not None else datetime.now()
            self.master_username = master_username if master_username is not None else source_instance.master_username if source_instance else master_username
            self.master_user_password = master_user_password if master_user_password is not None else source_instance.master_user_password if source_instance else master_user_password
            self.databases = databases if databases is not None else {}

            if is_replica and source_instance:
                self.endpoint = os.path.join(
                    DBInstance.BASE_PATH, self.db_instance_identifier)
                self.db_instance_identifier += f'_replica{DBInstance.static_counter}'
                self.instance_id = source_instance.db_instance_identifier
                self.path_file = getattr(
                    source_instance, 'path_file', f'./{self.db_instance_identifier}.txt')
                DBInstance.static_counter += 1
            else:
                self.db_name = db_name
                self.path_file = path_file if path_file is not None else f'./{self.db_instance_identifier}.txt'
                self.replicas = {}

            self.db_instance_arn = f'arn:vast:rds:{self.region}:2:{self.db_instance_identifier}:db:mydatabase'
            self.endpoint = os.path.join(
                DBInstance.BASE_PATH, self.db_instance_identifier)
            path_dic = './'
            event_handler = MyHandler(self)
            DBInstance.observer.schedule(
                event_handler, path_dic, recursive=False)
            if not DBInstance.observer.is_alive():
                DBInstance.observer.start()

            if not os.path.exists(DBInstance.BASE_PATH):
                os.mkdir(DBInstance.BASE_PATH)
            if not os.path.exists(self.endpoint):
                os.mkdir(self.endpoint)
        except:
            raise ValueError('The parameters are not perfect')

    def to_dict(self) -> Dict:
        '''Retrieve the metadata of the instance as a dictionary.'''
        data = {
            'db_instance_identifier': self.db_instance_identifier,
            'allocated_storage': self.allocated_storage,
            'port': self.port,
            'status': self.status,
            'created_time': self.created_time,
            'endpoint': self.endpoint,
            'databases': self.databases,
            'backup_auto': self.backup_auto,
            'region_auto_backup': self.region_auto_backup,
            'region': self.region,
            'replicas': [replica.db_instance_identifier for replica in self.replicas] if not hasattr(self, 'instance_id') else None
        }
        if hasattr(self, 'instance_id'):
            data['instance_id'] = self.instance_id
        else:
            data.update({
                'master_username': self.master_username,
                'master_user_password': self.master_user_password,
                'db_instance_arn': self.db_instance_arn,
                'path_file': self.path_file
            })
        return data

    def perform_action(self):
        if hasattr(self, 'instance_id'):
            with open(self.path_file, 'r') as file:
                obj_query = file.readline()
                for replica in self.replicas:
                    with open(replica.path_file, 'w') as file:
                        file.writelines(obj_query)
        else:
            with open(self.path_file, 'r') as file:
                obj_query = json.loads(file.readline())
