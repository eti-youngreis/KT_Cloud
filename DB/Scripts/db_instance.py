from DB.Scripts.Management import *


class DBInstance:
    def __init__(self, db_instance_identifier, cluster_id=None):
        """
        Initializes a DBInstance instance.

        Args:
        db_instance_identifier (str): The identifier of the DB instance
        cluster_id (str, optional): The identifier of the cluster to which the DB instance belongs (default: None)
        """
        self.db_instance_identifier = db_instance_identifier
        self.cluster_id = cluster_id

    def independent(self, conn=None):
        """
        Makes the DB instance independent by removing its association with any cluster.

        Args:
        conn (optional): Database connection object (default: None)
        """
        self.cluster_id = None
        update_metadata(self.__class__.__name__,
                        self.db_instance_identifier, 'cluster_id', None, conn=conn)

    def delete(self):
        """
        Deletes the DB instance from the management table and removes all associated DBs.

        This method is a placeholder and should be implemented with the actual logic for deleting the DB instance.
        """
        pass

# class DBSnapshot:
#     def __init__(self, id, cluster_id):
#         self.snapshot_id = id
#         self.cluster_id = cluster_id
