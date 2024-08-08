import json
from sql_commands import insert_into_management_table, create_db, update_management_table
from datetime import datetime
import os
from exception import AlreadyExistsError
from help_functions import get_json


class DBInstance:
    BASE_PATH = "db_instances"

    def __init__(self, **kwargs):
        """
        Initialize a new DBInstance with the given parameters.

        Args:
            db_instance_identifier (str): The identifier for the DB instance.
            allocated_storage (int): The allocated storage size.
            master_username (str): The master username.
            master_user_password (str): The master user password.
            db_name (str, optional): The name of the database. Defaults to None.
            port (int, optional): The port number. Defaults to 3306.
        """
        self.db_instance_identifier = kwargs['db_instance_identifier']
        self.allocated_storage = kwargs['allocated_storage']
        self.master_username = kwargs['master_username']
        self.master_user_password = kwargs['master_user_password']
        self.db_name = kwargs.get('db_name', None)
        self.port = kwargs.get('port', 3306)
        self.status = 'available'
        self.created_time = datetime.now()
        self.endpoint = os.path.join(DBInstance.BASE_PATH, self.db_instance_identifier)
        if not os.path.exists(self.endpoint):
            os.mkdir(self.endpoint)
        self.databases = kwargs.get('databases', {})
        if self.db_name:
            self.create_database(self.db_name)

    def __str__(self):
        return f"DBInstance: {get_json(self.get_data_dict())}"

    def save_to_db(self, exists_in_table=False):
        """Save the DB instance metadata to the management table."""
        metadata = self.get_data_dict()
        metadata_json = get_json(metadata)
        if not exists_in_table:
            insert_into_management_table(self.__class__.__name__, self.db_instance_identifier, metadata_json)
        else:
            update_management_table(self.db_instance_identifier, metadata_json)

    def get_data_dict(self):
        """Retrieve the metadata of the DB instance as a JSON string."""
        data = {
            'db_instance_identifier': self.db_instance_identifier,
            'allocated_storage': self.allocated_storage,
            'master_username': self.master_username,
            'master_user_password': self.master_user_password,
            'port': self.port,
            'status': self.status,
            'created_time': self.created_time,
            'endpoint': self.endpoint,
            'databases': self.databases
        }
        return data

    def create_database(self, db_name):
        """
        Create a new database within the DB instance.

        Args:
            db_name (str): The name of the database to create.

        Raises:
            AlreadyExistsError: If the database already exists.
        """
        if db_name in self.databases:
            raise AlreadyExistsError('db exists')

        db_path = os.path.join(self.endpoint, db_name)
        print(db_path)
        try:
            create_db(db_path)
            print(f'Database {db_name} created at {db_path}')
            self.databases[db_name] = db_path
            update_management_table(self.db_instance_identifier, get_json(self.get_data_dict()))
            print(f'DB log and triggers created at {db_path}')


        except:
            print("can't create")

    def get_path(self, db_name):
        """
        Get the path of the specified database.

        Args:
            db_name (str): The name of the database.

        Raises:
            ConnectionError: If the database does not exist.

        Returns:
            str: The path to the database.
        """
        if db_name not in self.databases:
            raise ConnectionError('db not exists')
        db_path = self.databases[db_name]
        return db_path

    def get_endpoint(self):
        """Get the endpoint of the DB instance."""
        return self.endpoint

    def stop(self):
        """Stop the DB instance."""
        self.status = 'stopped'
        self.save_to_db(True)

    def start(self):
        """Start the DB instance."""
        self.status = 'available'
        self.save_to_db(True)

