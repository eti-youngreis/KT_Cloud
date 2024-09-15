from datetime import datetime
from collections import deque
import os
from ..Service.Classes.DBInstanceService import SQLCommandHelper
from ..Service.Classes.DBInstanceService import DbSnapshotIdentifierNotFoundError

class DBInstanceModel:
    BASE_PATH = "db_instances"

    def __init__(self, **kwargs):
        self._node_subSnapshot_dic = {}
        self._node_subSnapshot_name_to_id = {}
        self.db_instance_identifier = kwargs['db_instance_identifier']
        self.allocated_storage = kwargs['allocated_storage']
        self.master_username = kwargs['master_username']
        self.master_user_password = kwargs['master_user_password']
        self.port = kwargs.get('port', 3306)
        self.status = 'available'
        self.created_time = datetime.now()
        self.endpoint = os.path.join(DBInstanceModel.BASE_PATH, self.db_instance_identifier)
        self._current_version_queue = deque([Node_SubSnapshot(parent=None, endpoint=self.endpoint)])
        self._last_node_of_current_version = self._current_version_queue[-1]




    def _create_child_to_node(self, node):
        self._last_node_of_current_version = node.create_child(self.endpoint)
        self._current_version_queue.append(self._last_node_of_current_version)
        self._node_subSnapshot_dic[self._last_node_of_current_version.id_snepshot] = self._last_node_of_current_version

    def __get_node_height(self, current_node):
        height = 0
        while current_node:
            height += 1
            current_node = current_node.parent
        return height

    def _update_queue_to_current_version(self, snapshot_to_restore):
        height = self.__get_node_height(snapshot_to_restore)
        non_shared_nodes_deque = deque()
        queue_len = len(self._current_version_queue)

        while height > queue_len:
            non_shared_nodes_deque.appendleft(snapshot_to_restore)
            snapshot_to_restore = snapshot_to_restore.parent
            height -= 1

        while height < queue_len:
            self._current_version_queue.popleft()
            queue_len -= 1

        while snapshot_to_restore != self._current_version_queue[-1]:
            non_shared_nodes_deque.appendleft(snapshot_to_restore)
            snapshot_to_restore = snapshot_to_restore.parent
            self._current_version_queue.popleft()

        self._current_version_queue.extend(non_shared_nodes_deque)

    def get_endpoint(self):
        return self.endpoint

    def stop(self):
        self.status = 'stopped'

    def start(self):
        self.status = 'available'

    def to_dict(self):
        return {
            'db_instance_identifier': self.db_instance_identifier,
            'allocated_storage': self.allocated_storage,
            'master_username': self.master_username,
            'master_user_password': self.master_user_password,
            'port': self.port,
            'status': self.status,
            'created_time': self.created_time.isoformat(),
            'endpoint': self.endpoint
        }

class Node_SubSnapshot:
    current_node_id = 1

    def __init__(self, parent, endpoint):
        self.id_snepshot = Node_SubSnapshot.current_node_id
        Node_SubSnapshot.current_node_id += 1
        self.parent = parent
        self.dbs_paths_dic = {}
        
        if self.parent:
            self.dbs_paths_dic = self.clone_databases_schema(parent.dbs_paths_dic)
        
        self.deleted_records_db_path = self._create_deleted_records_db_path(endpoint)
        self.snapshot_name = None
        self.children = []

    def _create_deleted_records_db_path(self, endpoint):
        deleted_records_db_path = os.path.join(endpoint, str(self.id_snepshot))
        os.makedirs(deleted_records_db_path, exist_ok=True)
        deleted_records_db_path = os.path.join(deleted_records_db_path, "deleted_db.db")
        return deleted_records_db_path

    def clone_databases_schema(self, dbs_paths_dic):
        dbs_paths_new_dic = {}
        for db, db_path in dbs_paths_dic.items():
            parts = db_path.split(os.sep)
            if len(parts) < 2:
                raise ValueError(f"Path '{db_path}' does not have enough parts to modify")
            parts[-2] = str(self.id_snepshot)
            new_path = os.sep.join(parts)
            directory = os.path.dirname(new_path)
            os.makedirs(directory, exist_ok=True)
            SQLCommandHelper.clone_database_schema(db_path, new_path)
            dbs_paths_new_dic[db] = new_path
        return dbs_paths_new_dic

    def _add_child(self, child):
        self.children.append(child)

    def create_child(self, endpoint):
        child = Node_SubSnapshot(parent=self, endpoint=endpoint)
        self._add_child(child)
        return child
