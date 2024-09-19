from datetime import datetime
from collections import deque
import os
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
import uuid
from DB.NEW_KT_DB.Validation.DBInstanceValiditions import validate_allocated_storage, validate_master_user_name, validate_master_user_password, validate_port, validate_status
from DB.NEW_KT_DB.Validation.GeneralValidations import is_valid_db_instance_identifier

class DBInstanceModel:
    BASE_PATH = "db_instances"
    table_structure = f'''
        db_instance_identifier TEXT PRIMARY KEY,
        metadata TEXT NOT NULL
        '''
    def __init__(self, **kwargs):

        # Validate and set db_instance_identifier
        if is_valid_db_instance_identifier(kwargs.get('db_instance_identifier'), 30):
            self.db_instance_identifier = kwargs['db_instance_identifier']
        else:
            raise ValueError("Invalid DB Instance Identifier")
        
        # Validate and set allocated_storage
        allocated_storage = kwargs.get('allocated_storage', 20)  # Default value is 20
        validate_allocated_storage(allocated_storage)
        self.allocated_storage = allocated_storage
 
        # Validate and set master_user_name
        master_user_name = kwargs.get('master_user_name', 'admin')  # Default value
        validate_master_user_name(master_user_name)
        self.master_username = master_user_name

        # Validate and set master_user_password
        master_user_password = kwargs.get('master_user_password', 'default_password')  # Default value
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

            node_subSnapshot_dic={str(k): v.to_dict() for k, v in self._node_subSnapshot_dic.items()},
            node_subSnapshot_name_to_id=self._node_subSnapshot_name_to_id,
            current_version_ids_queue=[str(id_snapshot) for id_snapshot in self._current_version_ids_queue]
        )


class Node_SubSnapshot:

    def __init__(self, parent=None, endpoint=None, **kwargs):
        self.id_snapshot = kwargs.get('id_snapshot', uuid.uuid4())
        self.snapshot_type= "manual"
        self.parent_id = parent.id_snapshot if parent else None
        
        self.created_time = None
        
        if parent and not kwargs.get('dbs_paths_dic'):
            self.dbs_paths_dic = self.clone_databases_schema(parent.dbs_paths_dic,endpoint)
        else:
            self.dbs_paths_dic = kwargs.get('dbs_paths_dic', {})

        self.deleted_records_db_path = kwargs.get('deleted_records_db_path', self._create_deleted_records_db_path(endpoint))


    def to_dict(self):
        return ObjectManager.convert_object_attributes_to_dictionary(
            id_snapshot=str(self.id_snapshot),
            parent_id=str(self.parent_id) if self.parent_id else None,
            dbs_paths_dic=self.dbs_paths_dic,
            deleted_records_db_path=self.deleted_records_db_path,
            snapshot_type=self.snapshot_type,
            created_time=self.created_time.isoformat() if self.created_time is not None else None,
        )    

    def _create_deleted_records_db_path(self, endpoint):
        deleted_records_db_path = os.path.join(endpoint, str(self.id_snapshot))
        os.makedirs(deleted_records_db_path, exist_ok=True)
        deleted_records_db_path = os.path.join(
            deleted_records_db_path, "deleted_db.db")
        return deleted_records_db_path

    def clone_databases_schema(self, dbs_paths_dic,endpoint):
        from DB.NEW_KT_DB.Service.Classes.DBInstanceService import SQLCommandHelper

        dbs_paths_new_dic = {}
        for db, db_path in dbs_paths_dic.items():
            db_filename = os.path.basename(db_path)
            new_path = os.path.join(endpoint, str(self.id_snapshot), db_filename)
            directory = os.path.dirname(new_path)
            os.makedirs(directory, exist_ok=True)
            SQLCommandHelper.clone_database_schema(db_path, new_path)
            dbs_paths_new_dic[db] = new_path
        return dbs_paths_new_dic

    def create_child(self, endpoint):
        child = Node_SubSnapshot(parent=self, endpoint=endpoint)
        return child

