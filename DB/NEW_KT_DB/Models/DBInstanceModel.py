from datetime import datetime
from collections import deque
import os
from ..Service.Classes.DBInstanceService import SQLCommandHelper
from ..Service.Classes.DBInstanceService import DbSnapshotIdentifierNotFoundError
from DB.NEW_KT_DB.DataAccess.ObjectManager import ObjectManager
import uuid


class DBInstanceModel:
    BASE_PATH = "db_instances"

    def __init__(self, **kwargs):
        self.db_instance_identifier = kwargs['db_instance_identifier']
        self.allocated_storage = kwargs['allocated_storage']
        self.master_username = kwargs['master_username']
        self.master_user_password = kwargs['master_user_password']
        self.port = kwargs.get('port', 3306)
        self.status = kwargs.get('status', 'available')
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
            created_time=self.created_time.isoformat(),
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
        self.dbs_paths_dic = kwargs.get('dbs_paths_dic', {})
        self.created_time = None
        if self.parent_id and not kwargs.get('dbs_paths_dic'):
            self.dbs_paths_dic = self.clone_databases_schema(parent.dbs_paths_dic)

        self.deleted_records_db_path = kwargs.get('deleted_records_db_path', self._create_deleted_records_db_path(endpoint))


    def to_dict(self):
        return ObjectManager.convert_object_attributes_to_dictionary(
            id_snapshot=str(self.id_snapshot),
            parent_id=str(self.parent_id) if self.parent_id else None,
            dbs_paths_dic=self.dbs_paths_dic,
            deleted_records_db_path=self.deleted_records_db_path,
            snapshot_type=self.snapshot_type
            created_time=self.created_time.isoformat()
        )    

    def _create_deleted_records_db_path(self, endpoint):
        deleted_records_db_path = os.path.join(endpoint, str(self.id_snapshot))
        os.makedirs(deleted_records_db_path, exist_ok=True)
        deleted_records_db_path = os.path.join(
            deleted_records_db_path, "deleted_db.db")
        return deleted_records_db_path

    def clone_databases_schema(self, dbs_paths_dic):
        dbs_paths_new_dic = {}
        for db, db_path in dbs_paths_dic.items():
            parts = db_path.split(os.sep)
            if len(parts) < 2:
                raise ValueError(
                    f"Path '{db_path}' does not have enough parts to modify")
            parts[-2] = str(self.id_snapshot)
            new_path = os.sep.join(parts)
            directory = os.path.dirname(new_path)
            os.makedirs(directory, exist_ok=True)
            SQLCommandHelper.clone_database_schema(db_path, new_path)
            dbs_paths_new_dic[db] = new_path
        return dbs_paths_new_dic

    def create_child(self, endpoint):
        child = Node_SubSnapshot(parent_id=self.id_snapshot, endpoint=endpoint)
        return child
