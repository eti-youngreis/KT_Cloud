"""
DBSnapshotController Module
---------------------------

This module defines the `DBSnapshotController` class, which provides a high-level interface for managing 
database snapshots associated with a specific database instance. 

The controller works by interacting with a `DBInstanceService` object, allowing users to perform 
operations such as:

- Creating snapshots
- Deleting snapshots
- Restoring a snapshot to a DB instance
- Listing all available snapshots for a DB instance
- Describing details of a specific snapshot
- Modifying snapshot attributes

### Classes:
    - DBSnapshotController: A controller class for managing DB snapshots.

### Example Usage:
    # Initialize the service and controller
    db_instance_service = DBInstanceService()
    snapshot_controller = DBSnapshotController(db_instance_service)

    # Create a new snapshot
    snapshot_controller.create_snapshot('my-db-instance', 'my-snapshot')

    # Delete a snapshot
    snapshot_controller.delete_snapshot('my-db-instance', 'my-snapshot')

    # Restore a snapshot
    snapshot_controller.restore_snapshot('my-db-instance', 'my-snapshot')

    # List all snapshots
    snapshots = snapshot_controller.list_snapshots('my-db-instance')

    # Describe a specific snapshot
    snapshot_details = snapshot_controller.describe_snapshot('my-db-instance', 'my-snapshot')

    # Modify a snapshot
    snapshot_controller.modify_snapshot('my-db-instance', 'my-snapshot', new_name='updated-snapshot')

### Dependencies:
    - DBInstanceService: A service class that provides lower-level operations for database instances and snapshots.

"""

from DB.NEW_KT_DB.Service.Classes.DBInstanceServiceReplica import DBInstanceService

class DBSnapshotController:
    """
    This class provides control over database snapshots for a specific DB instance.
    It uses the DBInstanceService to perform operations like create, delete, restore, and manage snapshots.
    """

    def __init__(self, db_instance_service: DBInstanceService):
        """
        Initialize the DBSnapshotController with a DBInstanceService object.
        
        Args:
            db_instance_service (DBInstanceService): The service responsible for managing DB instances and snapshots.
        """
        self.db_instance_service = db_instance_service

    def create_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str):
        """
        Create a snapshot for a given DB instance.
        
        Args:
            db_instance_identifier (str): The identifier of the DB instance for which the snapshot will be created.
            db_snapshot_identifier (str): The unique identifier for the snapshot to be created.
        
        Returns:
            The result of the snapshot creation from the DBInstanceService.
        """
        return self.db_instance_service.create_snapshot(db_instance_identifier, db_snapshot_identifier)

    def delete_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str):
        """
        Delete a specific snapshot for a given DB instance.
        
        Args:
            db_instance_identifier (str): The identifier of the DB instance that has the snapshot.
            db_snapshot_identifier (str): The unique identifier of the snapshot to be deleted.
        
        Returns:
            The result of the snapshot deletion from the DBInstanceService.
        """
        return self.db_instance_service.delete_snapshot(db_instance_identifier, db_snapshot_identifier)

    def restore_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str):
        """
        Restore a specific snapshot for a given DB instance.
        
        Args:
            db_instance_identifier (str): The identifier of the DB instance to which the snapshot will be restored.
            db_snapshot_identifier (str): The unique identifier of the snapshot to restore.
        
        Returns:
            The result of the snapshot restoration from the DBInstanceService.
        """
        return self.db_instance_service.restore_version_snapshot(db_instance_identifier, db_snapshot_identifier)

    def list_snapshots(self, db_instance_identifier: str):
        """
        List all snapshots associated with a specific DB instance.
        
        Args:
            db_instance_identifier (str): The identifier of the DB instance whose snapshots will be listed.
        
        Returns:
            List of snapshot identifiers for the given DB instance.
        """
        db_instance = self.db_instance_service.get(db_instance_identifier)
        return list(db_instance._node_subSnapshot_name_to_id.keys())

    def describe_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str):
        """
        Retrieve details for a specific snapshot of a DB instance.
        
        Args:
            db_instance_identifier (str): The identifier of the DB instance containing the snapshot.
            db_snapshot_identifier (str): The unique identifier of the snapshot to describe.
        
        Returns:
            The details of the specified snapshot from the DBInstanceService.
        """
        return self.db_instance_service.describe_snapshot(db_instance_identifier, db_snapshot_identifier)

    def modify_snapshot(self, db_instance_identifier: str, db_snapshot_identifier: str, **kwargs):
        """
        Modify a specific snapshot for a given DB instance with provided attributes.
        
        Args:
            db_instance_identifier (str): The identifier of the DB instance containing the snapshot.
            db_snapshot_identifier (str): The unique identifier of the snapshot to be modified.
            **kwargs: Additional attributes to modify for the snapshot.
        
        Returns:
            The result of the snapshot modification from the DBInstanceService.
        """
        return self.db_instance_service.modify_snapshot(db_instance_identifier, db_snapshot_identifier, **kwargs)
