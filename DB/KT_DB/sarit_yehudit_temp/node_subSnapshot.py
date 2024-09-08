from sql_command import clone_database_schema
from exception import DatabaseCloneError
import os

class Node_SubSnapshot:
    current_node_id = 1  # Class variable to track the current node ID

    def __init__(self, parent, endpoint):
        """
        Initialize a new Node_SubSnapshot instance.

        Args:
            parent (Node_SubSnapshot): The parent snapshot node.
            endpoint (str): The endpoint for storing database paths.
        """
        self.id_snepshot = Node_SubSnapshot.current_node_id
        Node_SubSnapshot.current_node_id += 1  # Increment the node ID for the next instance
        self.parent = parent
        self.dbs_paths_dic = {}  # Dictionary to store database paths
        
        # Clone database schema from the parent if it exists
        if self.parent:
            self.dbs_paths_dic = self.clone_databases_schema(parent.dbs_paths_dic)
        
        # Create path for deleted records database
        self.deleted_records_db_path = self._create_deleted_records_db_path(endpoint)
        self.snapshot_name = None  # Name of the snapshot
        self.children = []  # List to store child snapshots

    def _create_deleted_records_db_path(self, endpoint):
        """
        Create a path for the deleted records database.

        Args:
            endpoint (str): The endpoint for storing database paths.

        Returns:
            str: The path to the deleted records database.
        """
        deleted_records_db_path = os.path.join(endpoint, str(self.id_snepshot))

        # Create the directory if it does not exist
        os.makedirs(deleted_records_db_path, exist_ok=True)
        deleted_records_db_path = os.path.join(deleted_records_db_path, "deleted_db.db")
        return deleted_records_db_path

    def clone_databases_schema(self, dbs_paths_dic):
        """
        Clone the database schemas from the given paths.

        Args:
            dbs_paths_dic (dict): Dictionary of database names and their paths.

        Returns:
            dict: A new dictionary with cloned database paths.
        """
        print(dbs_paths_dic)
        dbs_paths_new_dic = {}
        
        for db, db_path in dbs_paths_dic.items():
            try:
                parts = db_path.split(os.sep)

                # Ensure the path has enough parts to modify
                if len(parts) < 2:
                    raise ValueError(f"Path '{db_path}' does not have enough parts to modify")

                # Update the second last part of the path with the current node ID
                parts[-2] = str(self.id_snepshot)
                new_path = os.sep.join(parts)

                directory = os.path.dirname(new_path)
                os.makedirs(directory, exist_ok=True)  # Create the directory for the new path
                print(new_path)

                # Clone the database schema
                clone_database_schema(db_path, new_path)
                dbs_paths_new_dic[db] = new_path  # Store the new path in the dictionary
            except DatabaseCloneError as e:
                print(f"Failed to clone database schema: {e}")
            except ValueError as e:
                print(f"ValueError: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")

        return dbs_paths_new_dic

    def _add_child(self, child):
        """Add a child node to this snapshot."""
        self.children.append(child)

    def create_child(self, endpoint):
        """
        Create a child Node_SubSnapshot instance.

        Args:
            endpoint (str): The endpoint for the child snapshot.

        Returns:
            Node_SubSnapshot: The created child snapshot instance.
        """
        child = Node_SubSnapshot(parent=self, endpoint=endpoint)
        self._add_child(child)  # Add the child to the current node
        return child
