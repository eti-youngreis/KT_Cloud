from DB.Scripts.db_instance import DBInstance
from DB.Scripts.endpoint import Endpoint
from typing import List
from DB.Scripts.parameter_group import ParameterGroup


class DBCluster:
    def __init__(self, cluster_identifier: str, parameter_group=ParameterGroup.build_default_parameter_group()):
        """
        Initializes a DBCluster instance.

        Args:
        cluster_identifier (str): The unique identifier for the DB cluster.
        parameter_group (ParameterGroup, optional): The parameter group associated with the DB cluster (default: default parameter group).
        """
        self.cluster_identifier = cluster_identifier
        self.instances: List[DBInstance] = []
        self.endpoints: List[Endpoint] = []
        self.parameter_group = parameter_group
        # self.snapshots: List[DBSnapshot] = []

    def delete_all_instances(self):
        """
        Deletes all DB instances associated with this cluster.

        Iterates over the list of instances and calls the delete method on each.
        """
        for instance in self.instances:
            instance.delete()
            # delete_db_instance(instance.db_instance_identifier)

    def make_instances_independent(self, conn=None):
        """
        Makes all DB instances independent by clearing their cluster associations.

        Args:
        conn (optional): A database connection object; used for updating the instance's metadata (default: None).
        """
        for instance in self.instances:
            instance.independent(conn)

    def delete_all_endpoints(self):
        """
        Deletes all endpoints associated with this cluster.

        Iterates over the list of endpoints and calls the delete method on each.
        """
        for endpoint in self.endpoints:
            endpoint.delete()
            # delete_db_endpoint(endpoint.endpoint_id)

    def add_endpoint(self, cluster_identifier, endpoint_identifier, endpoint_type, static_members, excluded_members):
        """
        Adds a new endpoint to the DB cluster.

        Args:
        cluster_identifier (str): The unique identifier for the DB cluster.
        endpoint_identifier (str): The unique identifier for the new endpoint.
        endpoint_type (str): The type of the new endpoint (e.g., 'READ', 'WRITE').
        static_members (list, optional): List of DB instance identifiers to be included in the endpoint (default: None).
        excluded_members (list, optional): List of DB instance identifiers to be excluded from the endpoint (default: None).

        Returns:
        dict: A description of the newly created endpoint.
        """
        if excluded_members is None:
            excluded_members = []
        if static_members is None:
            static_members = [inst.db_instance_identifier for inst in self.instances if inst.db_instance_identifier
                              not in excluded_members]

        endpoint = Endpoint(cluster_identifier, endpoint_identifier,
                            endpoint_type, static_members, excluded_members)
        endpoint.save_to_db()
        self.endpoints.append(endpoint)
        print(f"Custom endpoint {endpoint_identifier} created successfully.")
        return endpoint.describe('creating')

    # def delete_all_snapshots(self):
    #     """
    #     Deletes all snapshots associated with this cluster.

    #     Iterates over the list of snapshots and calls the delete_db_snapshot function on each.
    #     """
    #     for snapshot in self.snapshots:
    #         delete_db_snapshot(snapshot.snapshot_id)

    def delete_endpoint(self, endpoint, conn=None):
        """
        Deletes a specified endpoint from the DB cluster.

        Args:
        endpoint (Endpoint): The endpoint to be deleted.

        Returns:
        dict: A description of the deleted endpoint.

        Raises:
        RuntimeError: If the endpoint is currently in use (counter > 0).
        """
        # If the endpoint is in use
        if endpoint.counter > 0:
            raise RuntimeError(f"the endpoint is in use")
        endpoint.delete(conn)
        response = endpoint.describe('deleting')
        self.endpoints.remove(endpoint)
        return response
