import uuid
from Abc import DBO
from typing import Optional, List, Dict
from KT_DB.Validation.ValiditionDBClusterEndpoint import is_valid_identifier
from Models import DBClusterEndpointModel
from DataAccess import DBClusterEndpointManager

class DBClusterEndpointService(DBO):
    """
    Service class for managing DB Cluster Endpoints.
    """

    def __init__(self, dal: DBClusterEndpointManager):
        """
        Initialize the service with a DBClusterEndpointManager instance.
        
        :param dal: DBClusterEndpointManager instance for interacting with the database.
        """
        self.dal = dal

    def create(self, cluster_identifier: str, endpoint_identifier: str, endpoint_type: str, 
               static_members: Optional[List[str]] = None, excluded_members: Optional[List[str]] = None) -> Dict:
        """
        Create a new DB Cluster Endpoint.
        
        :param cluster_identifier: The identifier of the DB cluster.
        :param endpoint_identifier: The identifier of the endpoint.
        :param endpoint_type: The type of the endpoint (e.g., 'WRITER', 'READER').
        :param static_members: A list of static members for the endpoint. Defaults to all instances excluding excluded_members.
        :param excluded_members: A list of members to exclude from the endpoint. Optional.
        :return: A dictionary containing the details of the newly created endpoint.
        :raises ValueError: If the identifiers are not valid.
        """
        if not is_valid_identifier(cluster_identifier):
            raise ValueError(f"Invalid cluster_identifier: {cluster_identifier}")
        if not is_valid_identifier(endpoint_identifier):
            raise ValueError(f"Invalid endpoint_identifier: {endpoint_identifier}")

        if excluded_members is None:
            excluded_members = []
        if static_members is None:
            static_members = [inst.db_instance_identifier for inst in self.instances 
                              if inst.db_instance_identifier not in excluded_members]

        endpoint = DBClusterEndpointModel(cluster_identifier, endpoint_identifier, endpoint_type, static_members, excluded_members)
        self.dal.insert(endpoint.to_dict(), endpoint_identifier)
        return self.describe(endpoint_identifier, 'creating')

    def delete(self, endpoint_identifier: str) -> Dict:
        """
        Delete an existing DB Cluster Endpoint.
        
        :param endpoint_identifier: The identifier of the endpoint to delete.
        :return: A dictionary containing the details of the deleted endpoint.
        :raises ValueError: If the endpoint does not exist.
        """
        if not self.dal.is_identifier_exist(endpoint_identifier):
            raise ValueError(f"DB Cluster Endpoint '{endpoint_identifier}' does not exist.")
        
        self.dal.delete(endpoint_identifier)
        return self.describe(endpoint_identifier, 'deleting')

    def describe(self, endpoint_identifier: str, status: str = 'available') -> Dict:
        """
        Retrieve the details of a DB Cluster Endpoint.
        
        :param endpoint_identifier: The identifier of the endpoint to describe.
        :param status: The current status of the endpoint. Defaults to 'available'.
        :return: A dictionary containing details about the specified endpoint.
        """
        data = self.get(endpoint_identifier)
        describe = {
            'DBClusterEndpointIdentifier': endpoint_identifier,
            'DBClusterIdentifier': data['cluster_identifier'],
            'DBClusterEndpointResourceIdentifier': str(uuid.uuid4()),
            'Endpoint': f'{endpoint_identifier}.cluster-{data["cluster_identifier"]}.example.com',
            'Status': status,
            'EndpointType': 'CUSTOM',
            'CustomEndpointType': data['endpoint_type'],
            'StaticMembers': data['static_members'],
            'ExcludedMembers': data['excluded_members'],
            'DBClusterEndpointArn': f'arn:aws:rds:region:account:dbcluster-endpoint/{endpoint_identifier}'
        }
        return describe

    def modify(self, endpoint_identifier: str, updates: Dict):
        """
        Modify an existing DB Cluster Endpoint.
        
        :param endpoint_identifier: The identifier of the endpoint to modify.
        :param updates: A dictionary containing the updates to apply.
        :return: A dictionary containing details about the modified endpoint.
        :raises ValueError: If the endpoint does not exist.
        """
        current_data = self.dal.get(endpoint_identifier)
        if current_data is None:
            raise ValueError(f"DB Cluster Endpoint '{endpoint_identifier}' does not exist.")
        
        updated_data = {**current_data, **updates}
        self.dal.update(endpoint_identifier, updated_data)
        return self.describe(endpoint_identifier, 'modifying')

    def get(self, endpoint_identifier: str) -> Dict:
        """
        Retrieve the details of a DB Cluster Endpoint.
        
        :param endpoint_identifier: The identifier of the endpoint to retrieve.
        :return: A dictionary containing the endpoint details.
        :raises ValueError: If the endpoint does not exist.
        """
        data = self.dal.get(endpoint_identifier)
        if data is None:
            raise ValueError(f"DB Cluster Endpoint '{endpoint_identifier}' does not exist.")
        return data
