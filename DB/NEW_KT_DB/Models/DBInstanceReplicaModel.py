"""
DBInstanceModel

This class represents the model for a database instance. It encapsulates the attributes
and behavior of a database instance, including its configuration, state, and associated snapshots.

The model includes validation for various attributes and manages the versioning of the database
through a system of nodes and snapshots.

Attributes:
    db_instance_identifier: Unique identifier for the database instance.
    allocated_storage: Amount of storage allocated to the instance.
    master_username: Username for the master user of the database.
    master_user_password: Password for the master user.
    port: Port number on which the database instance accepts connections.
    status: Current status of the database instance.
    created_time: Timestamp of when the instance was created.
    endpoint: File system path where the instance data is stored.
    _node_subSnapshot_dic: Dictionary of snapshot nodes.
    _node_subSnapshot_name_to_id: Mapping of snapshot names to their IDs.
    _current_version_ids_queue: Queue of snapshot IDs representing the current version chain.

Methods:
    to_dict: Convert the instance attributes to a dictionary.

The class also includes nested Node_SubSnapshot class for managing individual snapshots.
"""

from datetime import datetime
from collections import deque
import os
import sqlite3
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
import uuid
from KT_Cloud.DB.NEW_KT_DB.Validation.DBInstanceReplicaValiditions import validate_allocated_storage, validate_master_user_name, validate_master_user_password, validate_port, validate_status
from DB.NEW_KT_DB.Validation.GeneralValidations import is_valid_db_instance_identifier

class DBInstanceModel:
    BASE_PATH = "db_instances"
    table_structure = f'''
        db_instance_identifier TEXT PRIMARY KEY,
        metadata TEXT NOT NULL
        '''
    def __init__(self, **kwargs):

        # Validate and set db_instance_identifier
        if is_valid_db_instance_identifier(kwargs.get('db_instance_identifier'), 52):
            self.db_instance_identifier = kwargs['db_instance_identifier']
        else:
            raise ValueError("Invalid DB Instance Identifier")
        
        # Validate and set allocated_storage
        allocated_storage = kwargs.get('allocated_storage', 20)  # Default value is 20
        validate_allocated_storage(allocated_storage)
        self.allocated_storage = allocated_storage
        self.is_replica=kwargs.get('is_replica',False)
        self.availability_zone=kwargs.get('availability_zone','a')
        self.db_cluster_identifier=kwargs.get('db_cluster_identifier')
 
        # Validate and set master_user_name
        master_user_name = kwargs.get('master_user_name', 'admin')  # Default value
        validate_master_user_name(master_user_name)
        self.master_username = master_user_name

        # Validate and set master_user_password
        master_user_password = kwargs.get('master_user_password', 'default_password')  # Default value
        print('master_user_password:',master_user_password)
        validate_master_user_password(master_user_password)
        self.master_user_password = master_user_password

        # Validate and set port
        port = kwargs.get('port', 3306)  # Default value is 3306
        validate_port(port)
        self.port = port

        # Validate and set status
        status = kwargs.get('status', 'available')  # Default value is 'available'
        validate_status(status)
        self.status = status
        # Set created_time (no validation required)
        self.created_time = kwargs.get('created_time', datetime.now())
        self.endpoint = os.path.join(
            DBInstanceModel.BASE_PATH, self.db_instance_identifier)

        self._node_subSnapshot_dic = kwargs.get('_node_subSnapshot_dic', {})
        self._node_subSnapshot_name_to_id = kwargs.get('_node_subSnapshot_name_to_id', {})

        if '_current_version_ids_queue' in kwargs:
            self._current_version_ids_queue = deque(kwargs['_current_version_ids_queue'])
        else:
            first_node = Node_SubSnapshot(parent_id=None, endpoint=self.endpoint)
            self._node_subSnapshot_dic[first_node.id_snapshot] = first_node
            self._current_version_ids_queue = deque([first_node.id_snapshot])

        self._last_node_of_current_version = self._node_subSnapshot_dic.get(self._current_version_ids_queue[-1])

        
    def to_dict(self):
        return ObjectManager.convert_object_attributes_to_dictionary(
            db_instance_identifier=self.db_instance_identifier,
            allocated_storage=self.allocated_storage,
            master_username=self.master_username,
            master_user_password=self.master_user_password,
            port=self.port,
            status=self.status,
            created_time=self.created_time.isoformat() if self.created_time is not None else None,
            endpoint=self.endpoint,
            availability_zone=self.availability_zone,
            db_cluster_identifier= self.db_cluster_identifier,
            is_replica=self.is_replica,
            node_subSnapshot_dic={str(k): v.to_dict() for k, v in self._node_subSnapshot_dic.items()},
            node_subSnapshot_name_to_id=self._node_subSnapshot_name_to_id,
            current_version_ids_queue=[str(id_snapshot) for id_snapshot in self._current_version_ids_queue]
        )


class Node_SubSnapshot:
    """
    A class representing a snapshot in a versioning system, used to store database schemas and deleted records.
    Each snapshot can have a parent, and a new snapshot can be created by cloning the parent's database schema.
    
    Attributes:
        id_snapshot (uuid): A unique identifier for the snapshot. Defaults to a new UUID if not provided.
        parent_id (uuid): The identifier of the parent snapshot, if applicable.
        dbs_paths_dic (dict): A dictionary mapping database names to their file paths.
        deleted_records_db_path (str): The path where deleted records are stored for the snapshot.
        snapshot_type (str): The type of snapshot (e.g., "manual").
        created_time (datetime): The time when the snapshot was created.
    """

    def __init__(self, parent=None, endpoint=None, **kwargs):
        """
        Initialize a new Node_SubSnapshot instance. If a parent snapshot is provided, 
        the databases are cloned from the parent, and paths are set for the current snapshot.
        
        Args:
            parent (Node_SubSnapshot): The parent snapshot to clone from (optional).
            endpoint (str): The base directory where the snapshot data will be stored.
            **kwargs: Optional parameters including:
                - id_snapshot (uuid): An optional unique identifier for the snapshot.
                - dbs_paths_dic (dict): An optional dictionary of database paths.
                - deleted_records_db_path (str): An optional path for storing deleted records.
        """
        self.id_snapshot = kwargs.get('id_snapshot', uuid.uuid4())  # Assign a new UUID if not provided.
        self.snapshot_type = "manual"  # Snapshot type defaults to "manual".
        self.parent_id = parent.id_snapshot if parent else None  # Assign parent ID if a parent exists.

        self.created_time = None  # Creation time is not set on initialization.

        # If there is a parent and no new dbs_paths_dic is provided, clone the parent's databases.
        if parent and not kwargs.get('dbs_paths_dic'):
            self.dbs_paths_dic = self.clone_databases_schema(parent.dbs_paths_dic, endpoint)
        else:
            self.dbs_paths_dic = kwargs.get('dbs_paths_dic', {})

        # Create or retrieve the path for deleted records in this snapshot.
        self.deleted_records_db_path = kwargs.get('deleted_records_db_path', self._create_deleted_records_db_path(endpoint))

    def to_dict(self):
        """
        Convert the Node_SubSnapshot instance to a dictionary, which can be used for storage or serialization.
        
        Returns:
            dict: A dictionary containing the snapshot's attributes.
        """
        return ObjectManager.convert_object_attributes_to_dictionary(
            id_snapshot=str(self.id_snapshot),
            parent_id=str(self.parent_id) if self.parent_id else None,
            dbs_paths_dic=self.dbs_paths_dic,
            deleted_records_db_path=self.deleted_records_db_path,
            snapshot_type=self.snapshot_type,
            created_time=self.created_time.isoformat() if self.created_time is not None else None,
        )
    
    def has_changes(self):
        for db_name, db_path in self.dbs_paths_dic.items():
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = cursor.fetchone()[0]
                if count > 0:
                    conn.close()
                    return True
            
            conn.close()
        
        return False

    def _create_deleted_records_db_path(self, endpoint):
        """
        Create a path for storing deleted records in the snapshot. A directory is created 
        with the snapshot's ID, and a SQLite database is initialized within that directory.
        
        Args:
            endpoint (str): The base directory where the snapshot data is stored.
        
        Returns:
            str: The full path to the deleted records database.
        """
        
        deleted_records_db_path = os.path.join(endpoint, str(self.id_snapshot))  # Create a folder for the snapshot.
        os.makedirs(deleted_records_db_path, exist_ok=True)  # Ensure the directory exists.
        deleted_records_db_path = os.path.join(deleted_records_db_path, "deleted_db.db")  # Define the path to the deleted records database.
        return deleted_records_db_path

    def clone_databases_schema(self, dbs_paths_dic, endpoint):
        """
        Clone the database schemas from the parent snapshot to create a new snapshot. This is done by copying 
        each database file to the new snapshot directory and cloning its schema.
        
        Args:
            dbs_paths_dic (dict): A dictionary of database names and paths from the parent snapshot.
            endpoint (str): The base directory where the cloned databases will be stored.
        
        Returns:
            dict: A dictionary mapping the new database names to their cloned paths.
        """
        from KT_Cloud.DB.NEW_KT_DB.Service.Classes.DBInstanceReplicaService import SQLCommandHelper

        dbs_paths_new_dic = {}  # Dictionary to store the new database paths.
        for db, db_path in dbs_paths_dic.items():
            db_filename = os.path.basename(db_path)  # Get the database filename.
            new_path = os.path.join(endpoint, str(self.id_snapshot), db_filename)  # Create the new path for the cloned database.
            directory = os.path.dirname(new_path)
            os.makedirs(directory, exist_ok=True)  # Ensure the directory for the new path exists.
            SQLCommandHelper.clone_database_schema(db_path, new_path)  # Clone the database schema.
            dbs_paths_new_dic[db] = new_path  # Store the new path in the dictionary.
        return dbs_paths_new_dic

    def create_child(self, endpoint):
        """
        Create a child snapshot from the current snapshot. The child will inherit the database schemas 
        from the parent and store its own changes separately.
        
        Args:
            endpoint (str): The base directory where the child snapshot data will be stored.
        
        Returns:
            Node_SubSnapshot: A new child snapshot object.
        """
        child = Node_SubSnapshot(parent=self, endpoint=endpoint)  # Create a child snapshot.
        return child
