from collections import deque
from datetime import datetime
from sql_command import create_database
import os
from exception import DbSnapshotIdentifierNotFoundError
from node_subSnapshot import Node_SubSnapshot

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
        # Initialize instance variables
        self._node_subSnapshot_dic = {}
        self._node_subSnapshot_name_to_id = {}
        self.db_instance_identifier = kwargs['db_instance_identifier']
        self.allocated_storage = kwargs['allocated_storage']
        self.master_username = kwargs['master_username']
        self.master_user_password = kwargs['master_user_password']
        self.port = kwargs.get('port', 3306)
        self.status = 'available'
        self.created_time = datetime.now()
        self.endpoint = os.path.join(
            DBInstance.BASE_PATH, self.db_instance_identifier)
        self._current_version_queue = deque(
            [Node_SubSnapshot(parent=None, endpoint=self.endpoint)])
        self._last_node_of_current_version = self._current_version_queue[-1]

        # Create the directory for the database instance if it doesn't exist
        if not os.path.exists(self.endpoint):
            os.makedirs(self.endpoint)

        # Create the database if a name is provided
        db_name = kwargs.get('db_name', None)
        if db_name:
            create_database(
                db_name, self._last_node_of_current_version, self.endpoint)

    def create_snapshot(self, db_snapshot_identifier):
        """Create a snapshot of the current database state."""
        self._node_subSnapshot_name_to_id[db_snapshot_identifier] = self._last_node_of_current_version.id_snepshot
        self._create_child_to_node(self._last_node_of_current_version)

    def restore_version(self, db_snapshot_identifier):
        """Restore the database to a specific snapshot version."""
        if db_snapshot_identifier not in self._node_subSnapshot_name_to_id:
            raise DbSnapshotIdentifierNotFoundError(
                f"Snapshot identifier '{db_snapshot_identifier}' not found.")

        node_id = self._node_subSnapshot_name_to_id[db_snapshot_identifier]
        snapshot = self._node_subSnapshot_dic.get(node_id)

        if snapshot:
            self._update_queue_to_current_version(snapshot)
            self._create_child_to_node(snapshot)

    def _create_child_to_node(self, node):
        """Create a child node from the given node and update the version queue."""
        self._last_node_of_current_version = node.create_child(self.endpoint)
        self._current_version_queue.append(self._last_node_of_current_version)
        self._node_subSnapshot_dic[self._last_node_of_current_version.id_snepshot] = self._last_node_of_current_version

    def __get_node_height(self, current_node):
        """
        Calculate the height of the given node in the tree.

        Args:
            current_node: The node for which the height is calculated.

        Returns:
            int: The height of the node.
        """
        height = 0
        while current_node:
            height += 1
            current_node = current_node.parent
        return height

    def _update_queue_to_current_version(self, snapshot_to_restore):
        """
        Update the current version queue to match the specified snapshot.

        Args:
            snapshot_to_restore: The snapshot to restore to.
        """
        height = self.__get_node_height(snapshot_to_restore)
        # Store nodes that are not shared until the common ancestor
        non_shared_nodes_deque = deque()
        queue_len = len(self._current_version_queue)

        # Adjust the queue until the height matches the queue length
        while height > queue_len:
            # Add current snapshot to deque
            non_shared_nodes_deque.appendleft(snapshot_to_restore)
            snapshot_to_restore = snapshot_to_restore.parent  # Move to parent node
            height -= 1  # Decrement the height

        while height < queue_len:
            # Remove front of the current version queue
            self._current_version_queue.popleft()
            queue_len -= 1  # Decrement the queue length

        while snapshot_to_restore != self._current_version_queue[-1]:
            # Add current snapshot to deque
            non_shared_nodes_deque.appendleft(snapshot_to_restore)
            snapshot_to_restore = snapshot_to_restore.parent  # Move to parent node
            self._current_version_queue.popleft()  # Remove last node from the queue

        # Extend queue with non-shared nodes
        self._current_version_queue.extend(non_shared_nodes_deque)

    def get_endpoint(self):
        """Get the endpoint of the DB instance."""
        return self.endpoint


# def __str__(self):
#     return f"DBInstance: {get_json(self.get_data_dict())}"
# def save_to_db(self, exists_in_table=False):5
#     """Save the DB instance metadata to the management table."""
#     metadata = self.get_data_dict()
#     metadata_json = get_json(metadata)
#     if not exists_in_table:
#         insert_into_management_table(
#             self.__class__.__name__, self.db_instance_identifier, metadata_json)
#     else:
#         update_management_table(self.db_instance_identifier, metadata_json)
# def get_data_dict(self):
#     """Retrieve the metadata of the DB instance as a JSON string."""
#     data = {
#         'db_instance_identifier': self.db_instance_identifier,
#         'allocated_storage': self.allocated_storage,
#         'master_username': self.master_username,
#         'master_user_password': self.master_user_password,
#         'port': self.port,
#         'status': self.status,
#         'created_time': self.created_time,
#         'endpoint': self.endpoint,
#         'databases': self.databases
#     }
#     return data
# def stop(self):
#     """Stop the DB instance."""
#     self.status = 'stopped'
#     self.save_to_db(True)
# def start(self):
#     """Start the DB instance."""
#     self.status = 'available'
#     self.save_to_db(True)
